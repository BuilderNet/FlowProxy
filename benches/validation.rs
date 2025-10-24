use std::hint::black_box;

use alloy_primitives::{keccak256, Address};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use flowproxy::{
    consts::FLASHBOTS_SIGNATURE_HEADER,
    ingress::maybe_verify_signature,
    primitives::{SystemBundleDecoder, SystemBundleMetadata, UtcInstant},
    priority::Priority,
    utils::testutils::Random,
};
use hyper::HeaderMap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rbuilder_primitives::serialize::RawBundle;
use serde_json::json;

struct RawBundleWithSigner {
    raw_bundle: RawBundle,
    signer: Address,
    received_at: UtcInstant,
}

struct SignedRequest {
    json: Vec<u8>,
    headers: HeaderMap,
}

impl Random for RawBundleWithSigner {
    fn random<R: Rng>(rng: &mut R) -> Self {
        Self {
            raw_bundle: RawBundle::random(rng),
            signer: Address::random_with(rng),
            received_at: UtcInstant::now(),
        }
    }
}

fn generate_bundles_with_signer(size: u64, rng: &mut StdRng) -> Vec<RawBundleWithSigner> {
    (0..size).map(|_| RawBundleWithSigner::random(rng)).collect()
}

fn generate_signed_bundles(size: u64, rng: &mut StdRng) -> Vec<SignedRequest> {
    (0..size)
        .map(|_| {
            let raw = RawBundle::random(rng);

            let json = json!({
                "id": 0,
                "jsonrpc": "2.0",
                "method": "eth_sendBundle",
                "params": [raw]
            });

            let json_vec = serde_json::to_vec(&json).unwrap();
            let json_str = serde_json::to_string(&json).unwrap();

            let hash = keccak256(&json_str);
            let signer = PrivateKeySigner::random();

            let signature = signer.sign_message_sync(format!("{hash:?}").as_bytes()).unwrap();

            let mut headers = HeaderMap::new();
            headers.insert(
                FLASHBOTS_SIGNATURE_HEADER,
                format!("{:?}:{}", signer.address(), signature).parse().unwrap(),
            );

            SignedRequest { json: json_vec, headers }
        })
        .collect()
}

pub fn bench_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bundle_validation");
    group.sample_size(64);
    let size = 128;

    group.throughput(Throughput::Elements(size));
    group.bench_function(BenchmarkId::from_parameter(size), |b| {
        let mut rng = StdRng::seed_from_u64(12);

        let decoder = SystemBundleDecoder::default();

        // We use iter_batched here so we have an owned value for the benchmarked function (second
        // closure)
        b.iter_batched(
            || generate_bundles_with_signer(size, &mut rng),
            |inputs| {
                for input in inputs {
                    let metadata = SystemBundleMetadata {
                        received_at: input.received_at,
                        signer: input.signer,
                        priority: Priority::Medium,
                    };
                    let result = decoder.try_decode(input.raw_bundle, metadata).unwrap();
                    black_box(result);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_sigverify(c: &mut Criterion) {
    let mut group = c.benchmark_group("sig_verification");
    group.sample_size(64);
    let size = 128;

    group.throughput(Throughput::Elements(size));
    group.bench_function(BenchmarkId::from_parameter(size), |b| {
        let mut rng = StdRng::seed_from_u64(12);

        // We use iter_batched here so we have an owned value for the benchmarked function (second
        // closure)
        b.iter_batched(
            || {
                // Generate inputs
                generate_signed_bundles(size, &mut rng)
            },
            |inputs| {
                for input in inputs {
                    let result = maybe_verify_signature(&input.headers, &input.json, true)
                        .expect("failed to verify signature");
                    black_box(result);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_validation, bench_sigverify);
criterion_main!(benches);

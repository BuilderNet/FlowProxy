//! Benchmarks for everything related to bundle validation.
//! - `bench_validation` benchmarks the validation of a bundle.

use std::hint::black_box;

use alloy_primitives::Address;
use buildernet_orderflow_proxy::{types::SystemBundle, utils::testutils::Random};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rbuilder_primitives::serialize::RawBundle;
use time::UtcDateTime;

struct RawBundleWithSigner {
    raw_bundle: RawBundle,
    signer: Address,
    received_at: UtcDateTime,
}

impl Random for RawBundleWithSigner {
    fn random<R: Rng>(rng: &mut R) -> Self {
        Self {
            raw_bundle: RawBundle::random(rng),
            signer: Address::random_with(rng),
            received_at: UtcDateTime::now(),
        }
    }
}

fn generate_inputs(size: u64, rng: &mut StdRng) -> Vec<RawBundleWithSigner> {
    (0..size).map(|_| RawBundleWithSigner::random(rng)).collect()
}

pub fn bench_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bundle_validation");
    group.sample_size(64);
    let size = 128;

    group.throughput(Throughput::Elements(size));
    group.bench_function(BenchmarkId::from_parameter(size), |b| {
        let mut rng = StdRng::seed_from_u64(12);

        // We use iter_batched here so we have an owned value for the benchmarked function (second
        // closure)
        b.iter_batched(
            || generate_inputs(size, &mut rng),
            |inputs| {
                for input in inputs {
                    let result = SystemBundle::try_from_bundle_and_signer(
                        input.raw_bundle,
                        input.signer,
                        input.received_at,
                    )
                    .unwrap();
                    black_box(result);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_validation);
criterion_main!(benches);

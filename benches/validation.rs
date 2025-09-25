use std::hint::black_box;

use alloy_primitives::Address;
use buildernet_orderflow_proxy::{types::SystemBundle, utils::testutils::Random};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rbuilder_primitives::serialize::RawBundle;

struct RawBundleWithSigner {
    raw_bundle: RawBundle,
    signer: Address,
}

impl Random for RawBundleWithSigner {
    fn random<R: Rng>(rng: &mut R) -> Self {
        Self { raw_bundle: RawBundle::random(rng), signer: Address::random_with(rng) }
    }
}

pub fn bench_validation(c: &mut Criterion) {
    let size = 100;

    c.bench_function(&format!("bundle_validation_{size}"), |b| {
        let mut rng = StdRng::seed_from_u64(12);

        b.iter_batched(
            || {
                // Generate inputs
                (0..size).map(|_| RawBundleWithSigner::random(&mut rng)).collect::<Vec<_>>()
            },
            |inputs| {
                for input in inputs {
                    let result =
                        SystemBundle::try_from_bundle_and_signer(input.raw_bundle, input.signer)
                            .unwrap();
                    black_box(result);
                }
            },
            criterion::BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench_validation);
criterion_main!(benches);

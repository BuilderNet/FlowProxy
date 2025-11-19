mod common;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use alloy_signer_local::PrivateKeySigner;
use flowproxy::utils::testutils::Random as _;
use futures::stream::StreamExt;
use rand::SeedableRng;
use rbuilder_primitives::serialize::RawBundle;
use revm_primitives::B256;
use tokio_stream::wrappers::ReceiverStream;

use crate::common::IngressClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn spam() {
    let mut rng = rand::rng();

    let _east = "http://172.190.190.141".to_string();
    let west = "http://20.234.206.56".to_string();

    let signer_seed = B256::random_with(&mut rng);

    let client = IngressClient {
        url: west,
        signer: PrivateKeySigner::from_bytes(&signer_seed).unwrap(),
        client: reqwest::Client::default(),
    };

    // Create a channel for bundle generation
    let (bundle_tx, bundle_rx) = tokio::sync::mpsc::channel::<RawBundle>(2000);

    // Spawn multiple blocking tasks to generate bundles in parallel
    let num_generators = 4;
    let bundles_per_generator = 100000 / num_generators;

    for i in 0..num_generators {
        let tx = bundle_tx.clone();
        let seed = rand::random::<u64>();

        std::thread::spawn(move || {
            let mut thread_rng = rand::rngs::StdRng::seed_from_u64(seed);
            let count = if i == num_generators - 1 {
                // Last generator handles any remainder
                bundles_per_generator + (100000 % num_generators)
            } else {
                bundles_per_generator
            };

            for _ in 0..count {
                let bundle = RawBundle::random(&mut thread_rng);
                // If send fails, receiver is dropped (test ended)
                if tx.blocking_send(bundle).is_err() {
                    break;
                }
            }
        });
    }

    // Drop the original sender so the channel closes when all generators finish
    drop(bundle_tx);

    // Track requests and RPS
    let total_requests = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    // Spawn a task to print RPS every second
    let rps_counter = total_requests.clone();
    let rps_task = tokio::spawn(async move {
        let mut last_count = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let current_count = rps_counter.load(Ordering::Relaxed);
            let rps = current_count - last_count;
            let elapsed = start_time.elapsed().as_secs_f64();
            let avg_rps = current_count as f64 / elapsed;
            println!("RPS: {} | Total: {} | Avg RPS: {:.2}", rps, current_count, avg_rps);
            last_count = current_count;
        }
    });

    // Convert receiver into a stream and send bundles concurrently
    ReceiverStream::new(bundle_rx)
        .map(|bundle| {
            let client = &client;
            let counter = total_requests.clone();
            async move {
                client.send_bundle(&bundle).await;

                // Increment counter after successful request
                counter.fetch_add(1, Ordering::Relaxed);
            }
        })
        .buffered(200)
        .collect::<Vec<_>>()
        .await;

    // Cancel the RPS monitoring task
    rps_task.abort();

    // Print final stats
    let total = total_requests.load(Ordering::Relaxed);
    let elapsed = start_time.elapsed();
    println!("\n=== Final Stats ===");
    println!("Total requests: {}", total);
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!("Average RPS: {:.2}", total as f64 / elapsed.as_secs_f64());
}

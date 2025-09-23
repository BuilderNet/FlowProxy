use std::time::Duration;

use alloy_primitives::Bytes;

mod common;
use common::{create_test_bundle, spawn_ingress, test_transaction_raw, BuilderReceiver};
use rbuilder_primitives::serialize::RawBundle;

/// This tests proper order propagation between 2 proxies.
#[tokio::test]
async fn network_e2e() {
    let mut builder1 = BuilderReceiver::spawn().await;
    let mut builder2 = BuilderReceiver::spawn().await;
    let client1 = spawn_ingress(Some(builder1.url())).await;
    let client2 = spawn_ingress(Some(builder2.url())).await;

    // Wait for the proxies to be ready and connected to each other.
    tokio::time::sleep(Duration::from_secs(35)).await;

    let raw_tx = test_transaction_raw();
    let response = client1.send_raw_tx(&raw_tx).await;
    assert!(response.status().is_success());

    let received = builder1.recv::<Bytes>().await.unwrap();

    println!("received 1: {:?}", received);
    assert_eq!(received, raw_tx);

    let received = builder2.recv::<Bytes>().await.unwrap();
    println!("received 2: {:?}", received);
    assert_eq!(received, raw_tx);

    let bundle = create_test_bundle();
    let response = client2.send_bundle(&bundle).await;
    assert!(response.status().is_success());

    let received = builder1.recv::<RawBundle>().await.unwrap();
    assert_eq!(received, bundle);

    let received = builder2.recv::<RawBundle>().await.unwrap();
    assert_eq!(received, bundle);

    tokio::time::sleep(Duration::from_secs(2)).await;
}

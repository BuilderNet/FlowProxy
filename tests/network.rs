use std::time::Duration;

mod common;
use common::{create_test_bundle, spawn_ingress, test_transaction_raw};

/// This tests proper order propagation between 2 proxies.
#[tokio::test]
async fn network_e2e() {
    let _client1 = spawn_ingress().await;
    let client2 = spawn_ingress().await;

    // Wait for the proxies to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    let raw_tx = test_transaction_raw();
    let response = client2.send_raw_tx(&raw_tx).await;
    assert!(response.status().is_success());

    let bundle = create_test_bundle();
    let response = client2.send_bundle(&bundle).await;
    assert!(response.status().is_success());

    tokio::time::sleep(Duration::from_secs(2)).await;
}

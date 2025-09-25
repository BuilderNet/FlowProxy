use std::time::Duration;

mod common;
use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use buildernet_orderflow_proxy::utils::testutils::Random as _;
use common::spawn_ingress;
use rbuilder_primitives::serialize::RawBundle;

/// This tests proper order propagation between 2 proxies.
#[tokio::test]
async fn network_e2e() {
    let mut rng = rand::rng();
    let _client1 = spawn_ingress().await;
    let client2 = spawn_ingress().await;

    // Wait for the proxies to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    let raw_tx = TxEnvelope::random(&mut rng).encoded_2718().into();
    let response = client2.send_raw_tx(&raw_tx).await;
    assert!(response.status().is_success());

    let bundle = RawBundle::random(&mut rng);
    let response = client2.send_bundle(&bundle).await;
    assert!(response.status().is_success());

    tokio::time::sleep(Duration::from_secs(2)).await;
}

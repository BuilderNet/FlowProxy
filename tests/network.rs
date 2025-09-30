use std::time::Duration;

use alloy_primitives::Bytes;

mod common;
use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use buildernet_orderflow_proxy::utils::testutils::Random as _;
use common::{spawn_ingress, BuilderReceiver};
use rbuilder_primitives::serialize::RawBundle;

/// This tests proper order propagation between 2 proxies.
#[tokio::test]
async fn network_e2e() {
    let mut rng = rand::rng();
    let mut builder1 = BuilderReceiver::spawn().await;
    let mut builder2 = BuilderReceiver::spawn().await;
    let client1 = spawn_ingress(Some(builder1.url())).await;
    let client2 = spawn_ingress(Some(builder2.url())).await;

    // Wait for the proxies to be ready and connected to each other.
    tokio::time::sleep(Duration::from_secs(35)).await;

    let raw_tx = TxEnvelope::random(&mut rng).encoded_2718().into();
    let response = client1.send_raw_tx(&raw_tx).await;
    assert!(response.status().is_success());

    let received = builder1.recv::<Bytes>().await.unwrap();
    assert_eq!(received, raw_tx);

    let received = builder2.recv::<Bytes>().await.unwrap();
    assert_eq!(received, raw_tx);

    let bundle = RawBundle::random(&mut rng);
    let response = client2.send_bundle(&bundle).await;
    assert!(response.status().is_success());

    let mut received = builder2.recv::<RawBundle>().await.unwrap();

    assert!(received.signing_address.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.signing_address = None;
    assert_eq!(received, bundle);

    let mut received = builder1.recv::<RawBundle>().await.unwrap();
    assert!(received.signing_address.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.signing_address = None;
    assert_eq!(received, bundle);

    tokio::time::sleep(Duration::from_secs(2)).await;
}

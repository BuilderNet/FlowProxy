use std::{path::PathBuf, str::FromStr as _, time::Duration};

use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use alloy_primitives::Bytes;
use common::BuilderReceiver;

mod common;
use flowproxy::{cli::OrderflowIngressArgs, utils::testutils::Random as _};
use rbuilder_primitives::serialize::RawBundle;
use tracing::{debug, info};

use crate::common::spawn_ingress_with_args;

/// This tests proper order propagation between 2 proxies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_e2e_bundle_tx_works() {
    let _ = tracing_subscriber::fmt::try_init();
    info!("starting network e2e tcp test");

    let mut rng = rand::rng();
    let builder1 = BuilderReceiver::spawn().await;
    let builder2 = BuilderReceiver::spawn().await;
    let builder3 = BuilderReceiver::spawn().await;
    // let builder4 = BuilderReceiver::spawn().await;
    let mut args = OrderflowIngressArgs::default().gzip_enabled().disable_builder_hub();
    args.peer_update_interval_s = 5;

    args.builder_url = Some(builder1.url());
    args.server_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_1.pem").unwrap();
    args.client_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_1.pem").unwrap();
    args.system_listen_addr = "127.0.0.1:0".parse().unwrap();
    let client1 = spawn_ingress_with_args(args.clone()).await;

    args.builder_url = Some(builder2.url());
    args.server_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_2.pem").unwrap();
    args.client_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_2.pem").unwrap();
    args.system_listen_addr = "127.0.0.1:0".parse().unwrap();
    let client2 = spawn_ingress_with_args(args.clone()).await;

    args.builder_url = Some(builder3.url());
    args.server_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_3.pem").unwrap();
    args.client_certificate_pem_file =
        PathBuf::from_str("./tests/testdata/certificates/cert_3.pem").unwrap();
    args.system_listen_addr = "127.0.0.1:0".parse().unwrap();
    let _client3 = spawn_ingress_with_args(args.clone()).await;

    // args.builder_url = Some(builder4.url());
    // args.server_certificate_pem_file =
    //     PathBuf::from_str("./tests/testdata/certificates/cert_4.pem").unwrap();
    // args.client_certificate_pem_file =
    //     PathBuf::from_str("./tests/testdata/certificates/cert_4.pem").unwrap();
    // args.system_listen_addr = "[::1]:0".parse().unwrap();
    // let _client4 = spawn_ingress_with_args(args).await;

    let mut builders = [builder1, builder2, builder3 /* builder4 */];

    // Wait for the proxies to be ready and connected to each other.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let raw_tx = TxEnvelope::random(&mut rng).encoded_2718().into();
    let response = client1.send_raw_tx(&raw_tx).await;
    info!("sent raw tx from client1");
    assert!(response.status().is_success());

    for (i, b) in builders.iter_mut().enumerate() {
        let received = b.recv::<Bytes>().await.unwrap();
        assert_eq!(received, raw_tx);
        debug!(?i, "builder received tx from client1");
    }

    let bundle = RawBundle::random(&mut rng);
    let response = client2.send_bundle(&bundle).await;
    info!("sent raw bundle from client2");
    assert!(response.status().is_success());

    for (i, b) in builders.iter_mut().enumerate() {
        let mut received = b.recv::<RawBundle>().await.unwrap();
        debug!(?i, "builder received raw bundle from client2");

        assert!(received.metadata.signing_address.is_some());
        assert!(received.metadata.bundle_hash.is_some());
        // NOTE: This will have a signing address populated which we reset
        received.metadata.signing_address = None;
        received.metadata.bundle_hash = None;
        assert_eq!(received, bundle);
    }
}

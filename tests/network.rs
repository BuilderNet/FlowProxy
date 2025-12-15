use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use alloy_primitives::Bytes;
use common::{spawn_ingress, BuilderReceiver};
use flowproxy::utils::testutils::Random as _;
use rbuilder_primitives::serialize::{RawBundle, RawShareBundle};
use std::time::Duration;

mod common;
use tracing::{debug, info};

/// This tests proper order propagation between 2 proxies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_e2e_bundle_tx_works() {
    let _ = tracing_subscriber::fmt::try_init();
    info!("starting network e2e tcp test");

    let mut rng = rand::rng();
    let mut builder1 = BuilderReceiver::spawn().await;
    let mut builder2 = BuilderReceiver::spawn().await;
    let client1 = spawn_ingress(Some(builder1.url())).await;
    let client2 = spawn_ingress(Some(builder2.url())).await;

    // Wait for the proxies to be ready and connected to each other.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let raw_tx = TxEnvelope::random(&mut rng).encoded_2718().into();
    let response = client1.send_raw_tx(&raw_tx).await;
    info!("sent raw tx from client1");
    assert!(response.status().is_success());

    let received = builder1.recv::<Bytes>().await.unwrap();
    assert_eq!(received, raw_tx);
    debug!("builder1 received tx from client1");

    let received = builder2.recv::<Bytes>().await.unwrap();
    assert_eq!(received, raw_tx);
    debug!("builder2 received tx from client1");

    let bundle = RawBundle::random(&mut rng);
    let response = client2.send_bundle(&bundle).await;
    info!("sent raw bundle from client2");
    assert!(response.status().is_success());

    let mut received = builder2.recv::<RawBundle>().await.unwrap();
    debug!("builder2 received raw bundle from client2");

    assert!(received.metadata.signing_address.is_some());
    assert!(received.metadata.bundle_hash.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.metadata.signing_address = None;
    received.metadata.bundle_hash = None;
    assert_eq!(received, bundle);

    let mut received = builder1.recv::<RawBundle>().await.unwrap();
    assert!(received.metadata.signing_address.is_some());
    assert!(received.metadata.bundle_hash.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.metadata.signing_address = None;
    received.metadata.bundle_hash = None;
    assert_eq!(received, bundle);
    debug!("builder1 received tx from client2");

    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn network_e2e_mev_share_bundle_works() {
    let mut rng = rand::rng();
    let mut builder1 = BuilderReceiver::spawn().await;
    let mut builder2 = BuilderReceiver::spawn().await;
    let _client1 = spawn_ingress(Some(builder1.url())).await;
    let client2 = spawn_ingress(Some(builder2.url())).await;

    // Wait for the proxies to be ready and connected to each other.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let bundle = RawShareBundle::random(&mut rng);
    let response = client2.send_mev_share_bundle(&bundle).await;
    assert!(response.status().is_success());

    let received = builder2.recv::<RawShareBundle>().await.unwrap();
    debug!("builder2 received bundle");
    assert_eq!(received, bundle);

    let received = builder1.recv::<RawShareBundle>().await.unwrap();
    debug!("builder1 received bundle");
    assert_eq!(received, bundle);
}

/// NOTE: This only works on Linux, because OpenSSL behaves differently on macOS (in a way that
/// makes this test fail).
#[cfg(target_os = "linux")]
mod linux {
    use std::{net::SocketAddr, path::PathBuf, str::FromStr as _, time::Duration};

    use alloy_signer_local::PrivateKeySigner;
    use flowproxy::{
        cli::OrderflowIngressArgs, statics::LOCAL_PEER_STORE, utils::testutils::Random as _,
    };
    use rbuilder_primitives::serialize::RawBundle;
    use tokio::io::AsyncReadExt as _;

    use crate::common::{spawn_haproxy, spawn_ingress_with_args, BuilderReceiver};

    /// Generate key material for TLS like this (from the ./testdata/certificates/ directory):
    /// ```bash
    /// openssl req -newkey rsa:2048 -nodes \
    ///     -keyout client.key \
    ///     -out /tmp/client.csr \
    ///     -subj "/CN=client"
    ///   openssl x509 -req -days 365 -sha256 \
    ///     -in /tmp/client.csr \
    ///     -signkey client.key \
    ///     -out client.crt
    ///   rm /tmp/client.csr
    ///
    ///   openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    ///     -keyout default.key \
    ///     -out default.crt \
    ///     -config ../openssl.cnf \
    ///     -extensions v3_req
    ///
    ///
    /// cat default.key default.crt > default.pem
    /// ```
    #[tokio::test]
    async fn network_e2e_tls() {
        let testdata_dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/testdata"));
        let cert_dir = testdata_dir.join("certificates");

        let signer1 = PrivateKeySigner::random();
        let signer2 = PrivateKeySigner::random();

        let mut args = OrderflowIngressArgs { peer_update_interval_s: 5, ..Default::default() }
            .disable_builder_hub();
        // args.private_key_pem_file = Some(cert_dir.join("default.key"));
        // args.certificate_pem_file = Some(cert_dir.join("default.crt"));
        args.orderflow_signer = Some(signer1);

        let mut args2 = args.clone();
        // args2.private_key_pem_file = Some(cert_dir.join("default.key"));
        // args2.certificate_pem_file = Some(cert_dir.join("default.crt"));
        args2.orderflow_signer = Some(signer2.clone());
        args2.system_listen_addr = SocketAddr::from_str("127.0.0.1:5542").unwrap();

        let mut builder1 = BuilderReceiver::spawn().await;
        let mut builder2 = BuilderReceiver::spawn().await;

        args.builder_url = Some(builder1.url());
        args2.builder_url = Some(builder2.url());

        let client1 = spawn_ingress_with_args(args).await;
        let _client2 = spawn_ingress_with_args(args2).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let haproxy = spawn_haproxy(&testdata_dir.join("haproxy.cfg"), &cert_dir).await.unwrap();
        tracing::info!(ports = ?haproxy.ports().await, "spawned haproxy");

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Override server instance data
        let ps = LOCAL_PEER_STORE.clone();
        tracing::debug!(?ps, "peer store");

        ps.builders.entry(signer2.address().to_string()).and_modify(|entry| {
            tracing::info!("overwriting signer2 peer with haproxy");

            // Overwrite the IP address to HAProxy system API
            entry.ip = "[::1]:5544".to_string();
            entry.dns_name = "localhost".to_string();
            entry.instance.tls_cert = std::fs::read_to_string(cert_dir.join("cert.pem")).unwrap();
        });

        tracing::debug!(?ps, "peer store after haproxy change");
        // NOTE: wait for peer config to update, so peer 1 connects to peer 2 HAProxy.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let mut stdout = String::new();
        haproxy.stdout(false).read_to_string(&mut stdout).await.unwrap();
        let mut stderr = String::new();
        haproxy.stderr(false).read_to_string(&mut stderr).await.unwrap();

        tracing::info!("\n\nhaproxy stdout: {}\n\n", stdout);
        tracing::info!("\n\nhaproxy stderr: {}\n\n", stderr);

        let mut rng = rand::rng();

        let bundle = RawBundle::random(&mut rng);

        tracing::info!("sending bundle from client1");
        let response = client1.send_bundle(&bundle).await;
        assert!(response.status().is_success());

        let mut received = builder1.recv::<RawBundle>().await.unwrap();
        tracing::info!("builder1 (local) received bundle from client1");
        assert!(received.metadata.signing_address.is_some());
        assert!(received.metadata.bundle_hash.is_some());
        // NOTE: This will have a signing address populated which we reset
        received.metadata.signing_address = None;
        received.metadata.bundle_hash = None;
        assert_eq!(received, bundle);

        let mut received = builder2.recv::<RawBundle>().await.unwrap();
        tracing::info!("builder2 (remote) received bundle from client1");

        assert!(received.metadata.signing_address.is_some());
        assert!(received.metadata.bundle_hash.is_some());
        // NOTE: This will have a signing address populated which we reset
        received.metadata.signing_address = None;
        received.metadata.bundle_hash = None;
        assert_eq!(received, bundle);

        let mut stdout = String::new();
        haproxy.stdout(false).read_to_string(&mut stdout).await.unwrap();
        let mut stderr = String::new();
        haproxy.stderr(false).read_to_string(&mut stderr).await.unwrap();

        println!("haproxy stdout: {}", stdout);
        println!("haproxy stderr: {}", stderr);
    }
}

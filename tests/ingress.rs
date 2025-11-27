use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use alloy_primitives::Bytes;
use alloy_signer::Signer as _;
use alloy_signer_local::LocalSigner;
use flate2::{write::GzEncoder, Compression};
use flowproxy::{
    consts::FLASHBOTS_SIGNATURE_HEADER,
    jsonrpc::{JsonRpcError, JSONRPC_VERSION_2},
    utils::testutils::Random,
};
use rbuilder_primitives::serialize::{RawBundle, RawShareBundle};
use reqwest::{header, StatusCode};
use revm_primitives::keccak256;
use serde_json::json;
use std::io::Write;

mod common;
use common::spawn_ingress;

use crate::common::BuilderReceiver;

mod assert {
    use flowproxy::jsonrpc::{JsonRpcError, JsonRpcResponse, JsonRpcResponseTy};

    pub(crate) async fn jsonrpc_error(response: reqwest::Response, expected: JsonRpcError) {
        let body = response.bytes().await.unwrap();
        let error: JsonRpcResponse<()> = serde_json::from_slice(body.as_ref()).unwrap();
        assert_eq!(
            error.result_or_error,
            JsonRpcResponseTy::Error { code: expected.code(), message: expected }
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingress_spam() {
    let _ = tracing_subscriber::fmt::try_init();

    use std::sync::Arc;

    // Build client once
    let client = Arc::new(
        reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .unwrap(),
    );

    // Pre-create signer once
    let signer = Arc::new(LocalSigner::random());

    // Spawn a fixed number of spam workers (per-thread parallelism)
    let workers = 4;
    for _ in 0..workers {
        let client = client.clone();
        let signer = signer.clone();

        tokio::spawn(async move {
            loop {
                // IMPORTANT: rng is created AND dropped before any await
                let (body, sighash) = {
                    let mut rng = rand::rng(); // ThreadRng (not Send)

                    let bundle = RawBundle::random(&mut rng);

                    let request = serde_json::json!({
                        "id": 0,
                        "jsonrpc": "2.0",
                        "method": "eth_sendBundle",
                        "params": [bundle]
                    });

                    let body = serde_json::to_vec(&request).unwrap();
                    let sighash = crate::keccak256(&body);
                    (body, sighash)
                };
                let signature = signer.sign_message(sighash.as_slice()).await.unwrap();

                let signature_header = format!("{:?}:{}", signer.address(), signature);

                // Send request inline (not spawning new tasks)
                // This keeps number of concurrent TCP requests bounded
                match client
                    .post("https://100.86.155.122:5544")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(FLASHBOTS_SIGNATURE_HEADER, &signature_header)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(res) => {
                        let status = res.status();
                        let text = res.text().await.unwrap();
                        tracing::info!("Spam response: {:?}, text: {text}", status);
                    }
                    Err(e) => {
                        tracing::warn!("Request error: {:?}", e);
                    }
                }
            }
        });
    }

    // Let test run for some time
    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
}

#[tokio::test]
async fn ingress_http_e2e() {
    let mut rng = rand::rng();
    let mut builder = BuilderReceiver::spawn().await;
    let client = spawn_ingress(Some(builder.url())).await;

    let empty = json!({});
    let response =
        client.build_request(serde_json::to_vec(&empty).unwrap(), None).send().await.unwrap();
    assert!(response.status().is_client_error());
    assert::jsonrpc_error(response, JsonRpcError::InvalidSignature).await;

    let empty = json!({});

    let response = client.send_json(&empty).await;
    assert!(response.status().is_client_error());
    assert::jsonrpc_error(response, JsonRpcError::ParseError).await;

    let no_id = json!({
        "jsonrpc": JSONRPC_VERSION_2,
        "method": "someMethod",
    });
    let response = client.send_json(&no_id).await;
    assert!(response.status().is_client_error());
    assert::jsonrpc_error(response, JsonRpcError::ParseError).await;

    let invalid_jsonrpc_version = json!({
        "id": 0,
        "jsonrpc": "invalid",
        "method": "someMethod",
    });
    let response = client.send_json(&invalid_jsonrpc_version).await;
    assert!(response.status().is_client_error());
    assert::jsonrpc_error(response, JsonRpcError::InvalidRequest).await;

    let unknown_method = json!({
        "id": 0,
        "jsonrpc": JSONRPC_VERSION_2,
        "method": "someMethod",
    });
    let response = client.send_json(&unknown_method).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert::jsonrpc_error(response, JsonRpcError::MethodNotFound("someMethod".to_string())).await;

    let empty_params = json!({
        "id": 0,
        "jsonrpc": JSONRPC_VERSION_2,
        "method": "eth_sendBundle",
        "params": [],
    });
    let response = client.send_json(&empty_params).await;
    assert!(response.status().is_client_error());
    assert::jsonrpc_error(response, JsonRpcError::InvalidParams).await;

    let test_tx = TxEnvelope::random(&mut rng);
    let raw_tx = test_tx.encoded_2718().into();
    let response = client.send_raw_tx(&raw_tx).await;
    assert!(response.status().is_success());

    let received = builder.recv::<Bytes>().await.unwrap();
    assert_eq!(received, raw_tx);

    let bundle = RawBundle::random(&mut rng);
    let response = client.send_bundle(&bundle).await;
    assert!(response.status().is_success());

    let mut received = builder.recv::<RawBundle>().await.unwrap();
    assert!(received.metadata.signing_address.is_some());
    assert!(received.metadata.bundle_hash.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.metadata.signing_address = None;
    received.metadata.bundle_hash = None;

    assert_eq!(received, bundle);

    let bundle = RawBundle::random(&mut rng);

    let request = json!({
        "id": 0,
        "jsonrpc": JSONRPC_VERSION_2,
        "method": "eth_sendBundle",
        "params": [bundle]
    });
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    let payload = serde_json::to_vec(&request).unwrap();
    let signature = client.sign_payload(&payload).await;
    encoder.write_all(&payload).unwrap();
    let compressed = encoder.finish().unwrap();
    let response = client
        .build_request(compressed, None)
        .header(header::CONTENT_ENCODING, "gzip")
        .header(FLASHBOTS_SIGNATURE_HEADER, signature)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    let mut received = builder.recv::<RawBundle>().await.unwrap();
    assert!(received.metadata.signing_address.is_some());
    assert!(received.metadata.bundle_hash.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.metadata.signing_address = None;
    received.metadata.bundle_hash = None;
    assert_eq!(received, bundle);
}

#[tokio::test]
async fn ingress_e2e_mev_share_bundle() {
    let mut rng = rand::rng();
    let mut builder = BuilderReceiver::spawn().await;
    let client = spawn_ingress(Some(builder.url())).await;

    let bundle = RawShareBundle::random(&mut rng);
    let response = client.send_mev_share_bundle(&bundle).await;
    assert!(response.status().is_success());

    let received = builder.recv::<RawShareBundle>().await.unwrap();
    assert_eq!(received, bundle);
}

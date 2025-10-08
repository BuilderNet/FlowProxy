use alloy_consensus::TxEnvelope;
use alloy_eips::Encodable2718 as _;
use alloy_primitives::Bytes;
use buildernet_orderflow_proxy::{
    consts::FLASHBOTS_SIGNATURE_HEADER,
    jsonrpc::{JsonRpcError, JSONRPC_VERSION_2},
    utils::testutils::Random,
};
use flate2::{write::GzEncoder, Compression};
use rbuilder_primitives::serialize::RawBundle;
use reqwest::{header, StatusCode};
use serde_json::json;
use std::io::Write;

mod common;
use common::spawn_ingress;

use crate::common::BuilderReceiver;

mod assert {
    use buildernet_orderflow_proxy::jsonrpc::{JsonRpcError, JsonRpcResponse, JsonRpcResponseTy};

    pub(crate) async fn jsonrpc_error(response: reqwest::Response, expected: JsonRpcError) {
        let body = response.bytes().await.unwrap();
        let error: JsonRpcResponse<()> = serde_json::from_slice(body.as_ref()).unwrap();
        assert_eq!(
            error.result_or_error,
            JsonRpcResponseTy::Error { code: expected.code(), message: expected }
        );
    }
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
    assert::jsonrpc_error(response, JsonRpcError::MethodNotFound).await;

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
    assert!(received.signing_address.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.signing_address = None;
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
    assert!(received.signing_address.is_some());
    // NOTE: This will have a signing address populated which we reset
    received.signing_address = None;
    assert_eq!(received, bundle);
}

// Common test utilities and types
// This module is shared across all integration tests

use std::time::Duration;

use alloy_primitives::Bytes;
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use buildernet_orderflow_proxy::{
    cli::OrderflowIngressArgs, ingress::FLASHBOTS_SIGNATURE_HEADER, jsonrpc::JSONRPC_VERSION_2,
};
use hyper::header;
use rbuilder_primitives::serialize::RawBundle;
use revm_primitives::keccak256;
use serde_json::json;
use tokio::net::TcpListener;

pub(crate) struct IngressClient<S: Signer> {
    pub(crate) url: String,
    pub(crate) client: reqwest::Client,
    pub(crate) signer: S,
}

pub(crate) async fn spawn_ingress() -> IngressClient<PrivateKeySigner> {
    let args = OrderflowIngressArgs::default().gzip_enabled().disable_builder_hub();
    let user_listener = TcpListener::bind(&args.user_listen_url).await.unwrap();
    let system_listener = TcpListener::bind(&args.system_listen_url).await.unwrap();
    let builder_listener = TcpListener::bind(&args.builder_listen_url).await.unwrap();
    let address = user_listener.local_addr().unwrap();

    tokio::spawn(async move {
        buildernet_orderflow_proxy::run_with_listeners(
            args,
            user_listener,
            system_listener,
            builder_listener,
        )
        .await
        .unwrap();
    });

    let url = format!("http://{address}");
    let health_url = format!("{url}/health");
    let client = reqwest::Client::default();

    let mut user_live = false;
    while !user_live {
        user_live = client.get(&health_url).send().await.unwrap().status().is_success();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let signer = PrivateKeySigner::random();

    IngressClient { url, client, signer }
}

impl<S: Signer> IngressClient<S> {
    pub(crate) fn build_request(
        &self,
        body: impl Into<reqwest::Body>,
        signature_header: Option<String>,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .post(&self.url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into());

        if let Some(signature_header) = signature_header {
            request = request.header(FLASHBOTS_SIGNATURE_HEADER, signature_header);
        }

        request
    }

    pub(crate) async fn send_json(&self, body: &serde_json::Value) -> reqwest::Response {
        let body = serde_json::to_vec(body).unwrap();
        let signature = self.sign_payload(&body).await;
        self.build_request(body, Some(signature)).send().await.unwrap()
    }

    pub(crate) async fn send_bundle(&self, bundle: &RawBundle) -> reqwest::Response {
        let request = json!({
            "id": 0,
            "jsonrpc": JSONRPC_VERSION_2,
            "method": "eth_sendBundle",
            "params": [bundle]
        });
        self.send_json(&request).await
    }

    pub(crate) async fn send_raw_tx(&self, tx: &Bytes) -> reqwest::Response {
        let request = json!({
            "id": 0,
            "jsonrpc": JSONRPC_VERSION_2,
            "method": "eth_sendRawTransaction",
            "params": [tx]
        });
        self.send_json(&request).await
    }

    pub(crate) async fn sign_payload(&self, payload: &[u8]) -> String {
        let sighash = keccak256(payload);
        let signature = self.signer.sign_hash(&sighash).await.unwrap();
        format!("{:?}:{}", self.signer.address(), signature)
    }
}

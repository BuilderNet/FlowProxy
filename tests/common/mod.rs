// Common test utilities and types
// This module is shared across all integration tests

use std::net::SocketAddr;

use alloy_primitives::Bytes;
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use axum::{extract::State, routing::post, Router};
use flowproxy::{
    cli::OrderflowIngressArgs,
    consts::FLASHBOTS_SIGNATURE_HEADER,
    ingress::maybe_decompress,
    jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, JSONRPC_VERSION_2},
    runner::CliContext,
};
use hyper::{header, HeaderMap};
use rbuilder_primitives::serialize::RawBundle;
use revm_primitives::keccak256;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::mpsc};

pub(crate) struct IngressClient<S: Signer> {
    pub(crate) url: String,
    pub(crate) client: reqwest::Client,
    pub(crate) signer: S,
}

pub(crate) async fn spawn_ingress_with_args(
    args: OrderflowIngressArgs,
) -> IngressClient<PrivateKeySigner> {
    let user_listener = TcpListener::bind(&args.user_listen_addr).await.unwrap();
    let builder_listener = None;
    let address = user_listener.local_addr().unwrap();

    let task_manager = rbuilder_utils::tasks::TaskManager::current();

    tokio::spawn(async move {
        flowproxy::run_with_listeners(
            args,
            user_listener,
            builder_listener,
            CliContext { task_executor: task_manager.executor() },
        )
        .await
        .unwrap();
    });

    IngressClient {
        url: format!("http://{address}"),
        client: reqwest::Client::default(),
        signer: PrivateKeySigner::random(),
    }
}

pub(crate) async fn spawn_ingress(builder_url: Option<String>) -> IngressClient<PrivateKeySigner> {
    let mut args = OrderflowIngressArgs::default().gzip_enabled().disable_builder_hub();
    args.peer_update_interval_s = 5;
    args.builder_url = builder_url;
    spawn_ingress_with_args(args).await
}

impl<S: Signer + Sync> IngressClient<S> {
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
        let sighash = format!("{:?}", keccak256(payload));
        let signature = self.signer.sign_message(sighash.as_bytes()).await.unwrap();
        format!("{:?}:{}", self.signer.address(), signature)
    }
}

pub(crate) struct BuilderReceiver {
    pub(crate) local_addr: SocketAddr,
    pub(crate) receiver: mpsc::Receiver<Value>,
}

impl BuilderReceiver {
    pub(crate) async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let (sender, receiver) = mpsc::channel(128);

        let router = Router::new().route("/", post(BuilderReceiver::receive)).with_state(sender);

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        BuilderReceiver { local_addr: address, receiver }
    }

    pub(crate) async fn recv<T: DeserializeOwned>(&mut self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.receiver.recv().await.unwrap())
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.local_addr)
    }

    #[tracing::instrument(skip_all, name = "builder_receiver")]
    async fn receive(
        State(sender): State<mpsc::Sender<Value>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<()> {
        let body = match maybe_decompress(true, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => {
                tracing::error!(?error, "failed to decompressing body");
                return JsonRpcResponse::error(Value::Null, error);
            }
        };

        let mut request: JsonRpcRequest<serde_json::Value> = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(e) => {
                tracing::error!(?e, ?body, "failed to decode body");
                return JsonRpcResponse::error(Value::Null, JsonRpcError::ParseError);
            }
        };

        let request_id = request.id.clone();
        tracing::info!(id = ?request_id, method = request.method, "received request");

        tracing::info!("sending request to builder");
        tracing::debug!(?request, "request");
        if let Err(e) = sender.send(request.take_single_param().unwrap()).await {
            panic!("failed to send received request to channel: {e}");
        }

        tracing::info!(id = ?request_id, "handled request");

        JsonRpcResponse::result(request_id, ())
    }
}

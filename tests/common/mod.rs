// Common test utilities and types
// This module is shared across all integration tests

use std::{net::SocketAddr, time::Duration};

use alloy_consensus::{
    EthereumTxEnvelope, EthereumTypedTransaction, SignableTransaction as _, TxEip1559, TxEip4844,
};
use alloy_eips::Encodable2718;
use alloy_primitives::{bytes::BytesMut, Address, Bytes, TxKind, U256};
use alloy_signer::{Signer, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use axum::{extract::State, routing::post, Router};
use buildernet_orderflow_proxy::{
    cli::OrderflowIngressArgs,
    ingress::{maybe_decompress, FLASHBOTS_SIGNATURE_HEADER},
    jsonrpc::{JsonRpcRequest, JsonRpcResponse, JSONRPC_VERSION_2},
};
use hyper::{header, HeaderMap};
use rbuilder_primitives::serialize::RawBundle;
use revm_primitives::keccak256;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::broadcast};
use tracing::Instrument as _;

pub(crate) struct IngressClient<S: Signer> {
    pub(crate) url: String,
    pub(crate) client: reqwest::Client,
    pub(crate) signer: S,
}

pub(crate) async fn spawn_ingress(builder_url: Option<String>) -> IngressClient<PrivateKeySigner> {
    let mut args = OrderflowIngressArgs::default().gzip_enabled().disable_builder_hub();
    if let Some(builder_url) = builder_url {
        args.builder_url = builder_url;
    }
    let user_listener = TcpListener::bind(&args.user_listen_url).await.unwrap();
    let system_listener = TcpListener::bind(&args.system_listen_url).await.unwrap();
    let builder_listener = TcpListener::bind(&args.builder_listen_url).await.unwrap();
    let address = user_listener.local_addr().unwrap();

    tokio::spawn(
        async move {
            buildernet_orderflow_proxy::run_with_listeners(
                args,
                user_listener,
                system_listener,
                builder_listener,
            )
            .await
            .unwrap();
        }
        .instrument(tracing::info_span!("proxy", ?address)),
    );

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

pub(crate) struct BuilderReceiver {
    pub(crate) local_addr: SocketAddr,
    pub(crate) receiver: broadcast::Receiver<Value>,
}

impl BuilderReceiver {
    pub(crate) async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let (sender, receiver) = broadcast::channel(128);

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

    async fn receive(
        State(sender): State<broadcast::Sender<Value>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<()> {
        let body = match maybe_decompress(true, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => {
                tracing::error!("Error decompressing body: {:?}", error);
                return JsonRpcResponse::error(None, error)
            }
        };

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(error) => return JsonRpcResponse::error(None, error),
        };

        let request_id = request.id;
        tracing::info!(id = request_id, method = request.method, "Received request");

        tracing::info!("Sending request to builder");
        tracing::debug!("Request: {:?}", request);
        let _ = sender.send(request.take_single_param().unwrap());

        JsonRpcResponse::result(request_id, ())
    }
}

/// Creates a test EIP-1559 transaction
pub(crate) fn test_eip1559_transaction() -> EthereumTypedTransaction<TxEip4844> {
    EthereumTypedTransaction::Eip1559(TxEip1559 {
        chain_id: 1,
        nonce: 0,
        gas_limit: 21_000,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(Address::random()),
        value: U256::ZERO,
        access_list: Default::default(),
        input: Bytes::default(),
    })
}

/// Creates a signed transaction envelope
pub(crate) fn test_transaction_signed() -> EthereumTxEnvelope<TxEip4844> {
    let signer = PrivateKeySigner::random();
    let transaction = test_eip1559_transaction();
    let sighash = transaction.signature_hash();
    let signature = signer.sign_hash_sync(&sighash).unwrap();
    EthereumTxEnvelope::new_unhashed(transaction, signature)
}

/// Creates raw transaction bytes
pub(crate) fn test_transaction_raw() -> Bytes {
    let transaction = test_transaction_signed();
    let mut buf = BytesMut::new();
    transaction.encode_2718(&mut buf);
    buf.freeze().into()
}

/// Creates a test bundle with default values
pub(crate) fn create_test_bundle() -> RawBundle {
    let bundle_json = r#"
        {
            "version": "v1",
            "blockNumber": "0x1136F1F",
            "txs": ["0x02f9037b018203cd8405f5e1008503692da370830388ba943fc91a3afd70395cd496c647d5a6cc9d4b2b7fad8780e531581b77c4b903043593564c000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000064f390d300000000000000000000000000000000000000000000000000000000000000030b090c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000080e531581b77c400000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000009184e72a0000000000000000000000000000000000000000000000000000080e531581b77c400000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000b5ea574dd8f2b735424dfc8c4e16760fc44a931b000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000c001a0a9ea84ad107d335afd5e5d2ddcc576f183be37386a9ac6c9d4469d0329c22e87a06a51ea5a0809f43bf72d0156f1db956da3a9f3da24b590b7eed01128ff84a2c1"],
            "revertingTxHashes": ["0xda7007bee134daa707d0e7399ce35bb451674f042fbbbcac3f6a3cb77846949c"],
            "minTimestamp": 0,
            "maxTimestamp": 1707136884,
            "signingAddress": "0x4696595f68034b47BbEc82dB62852B49a8EE7105"
        }"#;

    serde_json::from_str(bundle_json).expect("failed to decode bundle")
}

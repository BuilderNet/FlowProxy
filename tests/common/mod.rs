// Common test utilities and types
// This module is shared across all integration tests

use std::time::Duration;

use alloy_consensus::{
    EthereumTxEnvelope, EthereumTypedTransaction, SignableTransaction as _, TxEip1559, TxEip4844,
};
use alloy_eips::Encodable2718;
use alloy_primitives::{bytes::BytesMut, Address, Bytes, TxKind, U256};
use alloy_signer::{Signer, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use buildernet_orderflow_proxy::{
    cli::OrderflowIngressArgs, ingress::FLASHBOTS_SIGNATURE_HEADER, jsonrpc::JSONRPC_VERSION_2,
    types::RpcBundle,
};
use hyper::header;
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

    pub(crate) async fn send_bundle(&self, bundle: &RpcBundle) -> reqwest::Response {
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
pub(crate) fn create_test_bundle() -> RpcBundle {
    RpcBundle {
        block_number: None,
        txs: vec![test_transaction_raw(), test_transaction_raw()],
        min_timestamp: None,
        max_timestamp: None,
        reverting_tx_hashes: vec![],
        dropping_tx_hashes: vec![],
        uuid: None,
        refund_percent: None,
        refund_recipient: None,
        refund_tx_hashes: vec![],
        refund_identity: None,
        version: Default::default(),
    }
}

use crate::{
    builderhub,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        FLASHBOTS_SIGNATURE_HEADER,
    },
    metrics::SYSTEM_METRICS,
    primitives::{
        EncodedOrder, Protocol, RawOrderMetadata, SystemOrder, UtcInstant, WithEncoding,
        WithHeaders,
    },
    priority::{self, workers::PriorityWorkers, Priority},
    utils::UtcDateTimeHeader as _,
};
use alloy_primitives::{keccak256, Address, B256};
use alloy_signer::SignerSync as _;
use alloy_signer_local::PrivateKeySigner;
use axum::http::HeaderValue;
use dashmap::DashMap;
use hyper::{header::CONTENT_TYPE, HeaderMap};
use serde_json::json;
use std::{collections::HashMap, fmt::Display, sync::Arc};
use time::UtcDateTime;
use tracing::*;

pub mod client;
pub mod http;
pub mod tcp;

/// Sign and build the signature header in the form of `signer_address:signature`.
fn build_signature_header(signer: &PrivateKeySigner, body: &[u8]) -> String {
    let body_hash = keccak256(body);
    let signature = signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
    format!("{:?}:{}", signer.address(), signature)
}

#[derive(Debug)]
pub struct IngressForwarders {
    /// The orderflow signer.
    signer: PrivateKeySigner,
    /// The sender to the local builder forwarder.
    local: priority::channel::UnboundedSender<Arc<ForwardingRequest>>,
    /// The senders to peer ingresses. Continuously updated from builderhub configuration.
    peers: Arc<DashMap<String, PeerHandle>>,
    /// The priority workers for signing requests.
    workers: PriorityWorkers,
}

impl IngressForwarders {
    /// Create new ingress forwards.
    pub fn new(
        local: priority::channel::UnboundedSender<Arc<ForwardingRequest>>,
        peers: Arc<DashMap<String, PeerHandle>>,
        signer: PrivateKeySigner,
        workers: PriorityWorkers,
    ) -> Self {
        Self { local, peers, signer, workers }
    }

    /// Find peer name by address.
    pub fn find_peer(&self, address: Address) -> Option<String> {
        self.peers
            .iter()
            .find(|peer| peer.info.orderflow_proxy.ecdsa_pubkey_address == address)
            .map(|peer| peer.info.name.clone())
    }

    /// Broadcast a certain order to all forwarders.
    pub(crate) async fn broadcast_order(&self, order: SystemOrder) {
        let priority = order.priority();
        let mut encoded_order = order.encode();

        // Create local request first
        let local = Arc::new(ForwardingRequest::user_to_local(encoded_order.clone().into()));
        let _ = self.local.send(local.priority(), local);

        let signer = self.signer.clone();
        let encoding = encoded_order.encoding.clone();
        let signature_header = self
            .workers
            .spawn_with_priority(priority, move || {
                build_signature_header(&signer, encoding.as_ref())
            })
            .await;

        let headers = ForwardingRequest::create_headers(
            priority,
            Some(signature_header),
            Some(UtcDateTime::now()),
        );

        // FIX: remove unwrap
        let headers_strings = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect::<HashMap<String, String>>();

        let payload = WithHeaders { headers: headers_strings, data: &encoded_order.encoding };
        let data = serde_json::to_string(&payload).unwrap().as_bytes().to_vec();
        encoded_order.encoding_tcp_forwarder = Some(data);

        let forward = Arc::new(ForwardingRequest::user_to_system(encoded_order.into(), headers));

        debug!(peers = %self.peers.len(), "sending bundle to peers");
        self.broadcast_inner(forward);
    }

    /// Broadcast request to all peers.
    fn broadcast_inner(&self, forward: Arc<ForwardingRequest>) {
        for entry in self.peers.iter() {
            let handle = entry.value();
            if let Err(e) = handle.sender.send(forward.priority(), forward.clone()) {
                error!(?e, peer = %handle.info.name,  "failed to send forwarding request to peer");
            }
        }
    }

    /// Send request only to local forwarder.
    pub fn send_to_local(
        &self,
        priority: Priority,
        method: &str,
        param: serde_json::Value,
        hash: B256,
        received_at: UtcInstant,
    ) {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": [param]
        });

        let body = serde_json::to_vec(&json).expect("to JSON serialize request");
        // TODO: raw orders have no priority, but this will change, assume medium for now
        let raw_order = RawOrderMetadata { priority: Priority::Medium, received_at, hash };
        let order = EncodedOrder::Raw(WithEncoding {
            inner: raw_order,
            encoding: body,
            encoding_tcp_forwarder: None,
        });

        let local = Arc::new(ForwardingRequest::system_to_local(order));
        let _ = self.local.send(priority, local);
    }
}

#[derive(Debug)]
pub struct PeerHandle {
    /// Peer info.
    pub info: builderhub::Peer,
    /// Sender to the peer forwarder.
    pub sender: priority::channel::UnboundedSender<Arc<ForwardingRequest>>,
    /// The protocol used to communicate with the peer.
    pub protocol: Protocol,
}

/// The direction of the forwarding request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardingDirection {
    UserToLocal,
    UserToSystem,
    SystemToLocal,
}

impl Display for ForwardingDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserToLocal => write!(f, "user_to_local"),
            Self::UserToSystem => write!(f, "user_to_system"),
            Self::SystemToLocal => write!(f, "system_to_local"),
        }
    }
}

impl ForwardingDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UserToLocal => "user_to_local",
            Self::UserToSystem => "user_to_system",
            Self::SystemToLocal => "system_to_local",
        }
    }
}

#[derive(Debug)]
pub struct ForwardingRequest {
    /// The data to be forwarded.
    pub encoded_order: EncodedOrder,
    /// The headers of the request.
    pub headers: reqwest::header::HeaderMap,

    /// The direction of the forwarding request.
    pub direction: ForwardingDirection,

    /// The tracing span associated with this request.
    pub span: tracing::Span,
}

impl ForwardingRequest {
    pub fn user_to_local(encoded_order: EncodedOrder) -> Self {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()));
        Self {
            encoded_order,
            headers,
            direction: ForwardingDirection::UserToLocal,
            span: tracing::Span::current(),
        }
    }

    pub fn user_to_system(encoded_order: EncodedOrder, headers: HeaderMap) -> Self {
        Self {
            encoded_order,
            headers,
            direction: ForwardingDirection::UserToSystem,
            span: tracing::Span::current(),
        }
    }

    pub fn system_to_local(encoded_order: EncodedOrder) -> Self {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()));
        Self {
            encoded_order,
            headers,
            direction: ForwardingDirection::SystemToLocal,
            span: tracing::Span::current(),
        }
    }

    pub fn with_span(self, span: tracing::Span) -> Self {
        Self { span, ..self }
    }

    /// Create a new forwarding request to a builder. The [`BUILDERNET_SENT_AT_HEADER`] is formatted
    /// as a UNIX timestamp in nanoseconds.
    fn create_headers(
        priority: Priority,
        signature_header: Option<String>,
        sent_at_header: Option<UtcDateTime>,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            BUILDERNET_PRIORITY_HEADER,
            priority.to_string().parse().expect("to parse priority string"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if let Some(signature_header) = signature_header {
            headers.insert(
                FLASHBOTS_SIGNATURE_HEADER,
                signature_header.parse().expect("to parse signature header"),
            );
        }

        if let Some(sent_at) = sent_at_header {
            headers.insert(BUILDERNET_SENT_AT_HEADER, sent_at.format_header());
        }

        headers
    }

    pub fn priority(&self) -> Priority {
        self.encoded_order.priority()
    }

    pub fn encoded_size(&self) -> usize {
        self.encoded_order.encoding().len()
    }

    pub fn is_big(&self) -> bool {
        self.encoded_size() > BIG_REQUEST_SIZE_THRESHOLD_KB
    }

    /// Returns the hash of the encoded order.
    pub fn hash(&self) -> B256 {
        self.encoded_order.hash()
    }
}

/// Record the end-to-end metrics for the request.
fn record_e2e_metrics(order: &EncodedOrder, direction: &ForwardingDirection, is_big: bool) {
    match order {
        EncodedOrder::Bundle(_) => {
            SYSTEM_METRICS
                .bundle_processing_time(
                    order.priority().as_str(),
                    direction.as_str(),
                    is_big.to_string(),
                )
                .observe(order.received_at().elapsed().as_secs_f64());
        }
        EncodedOrder::MevShareBundle(_) => {
            SYSTEM_METRICS
                .mev_share_bundle_processing_time(
                    order.priority().as_str(),
                    direction.as_str(),
                    is_big.to_string(),
                )
                .observe(order.received_at().elapsed().as_secs_f64());
        }
        EncodedOrder::Transaction(_) => {
            SYSTEM_METRICS
                .transaction_processing_time(
                    order.priority().as_str(),
                    direction.as_str(),
                    is_big.to_string(),
                )
                .observe(order.received_at().elapsed().as_secs_f64());
        }
        EncodedOrder::Raw(_) => {
            SYSTEM_METRICS
                .system_order_processing_time(
                    order.priority().as_str(),
                    direction.as_str(),
                    order.order_type(),
                    is_big.to_string(),
                )
                .observe(order.received_at().elapsed().as_secs_f64());
        }
    }
}

use crate::{
    builderhub,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD, BUILDERNET_ADDRESS_HEADER, BUILDERNET_PRIORITY_HEADER,
        BUILDERNET_SENT_AT_HEADER, BUILDERNET_SIGNATURE_HEADER,
    },
    metrics::SYSTEM_METRICS,
    primitives::{
        EncodedOrder, RawBundleBitcode, RawOrderMetadata, SystemOrder, UtcInstant, WithEncoding,
        WithHeaders,
    },
    priority::{self, Priority, workers::PriorityWorkers},
    utils::UtcDateTimeHeader as _,
};
use alloy_primitives::{Address, B256, keccak256};
use alloy_signer::SignerSync as _;
use alloy_signer_local::PrivateKeySigner;
use dashmap::DashMap;
use serde_json::json;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant},
};
use time::UtcDateTime;
use tracing::*;

pub mod client;
pub mod http;
pub mod tcp;

pub type Headers = HashMap<String, String>;

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
        // NOTE: this code is fairly complex and unoptimized due to keeping backwards
        // compability with:
        // 1. Local builder only accepting JSON-RPC encoded orders.

        let priority = order.priority();
        let method_name = order.method_name().to_string();

        // Start with JSON-RPC encoding, that's needed for the local builder anyway.
        let mut encoded_order = order.clone().encode();

        // Create local request first
        let local = Arc::new(ForwardingRequest::user_to_local(encoded_order.clone().into()));
        let _ = self.local.send(local.priority(), local);

        let encoding_binary = match &order {
            SystemOrder::Bundle(bundle) => {
                let bundle = RawBundleBitcode::from(bundle.raw_bundle.as_ref());
                bitcode::encode(&bundle)
            }
            // Raw txs are forwarded as is, no need to re-encode raw bytes.
            SystemOrder::Transaction(tx) => tx.raw.to_vec(),
        };
        let encoding_binary = Arc::new(encoding_binary);

        let signature = {
            let signer = self.signer.clone();
            let bin = encoding_binary.clone();
            self.workers
                .spawn_with_priority(priority, move || {
                    build_signature_header(&signer, bin.as_ref())
                })
                .await
        };

        let mut headers = ForwardingRequest::create_headers(
            priority,
            Some(signature),
            Some(UtcDateTime::now()),
            Some(self.signer.address()),
        );
        headers.insert("method".to_owned(), method_name);

        let payload = WithHeaders { headers: headers.clone(), data: encoding_binary };
        let data = bitcode::encode(&payload);

        encoded_order.encoding_tcp_forwarder = Some(data);

        let forward = Arc::new(ForwardingRequest::user_to_system(encoded_order.into(), headers));

        debug!(peers = %self.peers.len(), "sending order to peers");
        self.broadcast_inner(forward);
    }

    /// Broadcast request to all peers.
    fn broadcast_inner(&self, forward: Arc<ForwardingRequest>) {
        self.peers.retain(|peer, handle| {
            if let Err(e) = handle.sender.send(forward.priority(), forward.clone()) {
                error!(?e, %peer,  "peer channel closed, removing peer");

                // Remove the peer from the list if sending fails
                false
            } else {
                true
            }
        });
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

        tracing::debug!(?json, "sending system order to local forwarder");

        let body = serde_json::to_vec(&json).expect("to JSON serialize request");
        // TODO: raw orders have no priority, but this will change, assume medium for now
        let raw_order = RawOrderMetadata { priority: Priority::Medium, received_at, hash };
        let order = EncodedOrder::Raw(WithEncoding {
            inner: raw_order,
            encoding: Arc::new(body),
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
    pub headers: Headers,

    /// The direction of the forwarding request.
    pub direction: ForwardingDirection,

    /// The tracing span associated with this request.
    pub span: tracing::Span,
}

impl ForwardingRequest {
    pub fn user_to_local(encoded_order: EncodedOrder) -> Self {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()), None);
        Self {
            encoded_order,
            headers,
            direction: ForwardingDirection::UserToLocal,
            span: tracing::Span::current(),
        }
    }

    pub fn user_to_system(encoded_order: EncodedOrder, headers: Headers) -> Self {
        Self {
            encoded_order,
            headers,
            direction: ForwardingDirection::UserToSystem,
            span: tracing::Span::current(),
        }
    }

    pub fn system_to_local(encoded_order: EncodedOrder) -> Self {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()), None);
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
        address: Option<Address>,
    ) -> Headers {
        let mut headers = HashMap::new();
        headers.insert(BUILDERNET_PRIORITY_HEADER.to_owned(), priority.to_string());
        headers.insert("content-type".to_owned(), "application/json".to_owned());

        if let Some(signature_header) = signature_header {
            headers.insert(
                BUILDERNET_SIGNATURE_HEADER.to_owned(),
                signature_header.parse().expect("to parse signature header"),
            );
        }

        if let Some(sent_at) = sent_at_header {
            headers.insert(BUILDERNET_SENT_AT_HEADER.to_owned(), sent_at.format_header());
        }

        if let Some(a) = address {
            headers.insert(BUILDERNET_ADDRESS_HEADER.to_owned(), a.to_string());
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
        self.encoded_size() > BIG_REQUEST_SIZE_THRESHOLD
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

/// A simple rate limiter for logging, with a configurable [`Duration`] threshold. Useful to avoid
/// log spam, for example getting bursts of "connection refused"-like messages that could quickly
/// fill the machine disk with logs.
#[derive(Debug, Clone, Copy)]
pub struct LogRateLimiter {
    last_logged: Instant,
    threshold: Duration,
}

impl LogRateLimiter {
    pub fn new(threshold: Duration) -> Self {
        Self { last_logged: Instant::now() - threshold, threshold }
    }

    /// Try to log by executing the provided closure `f` if the threshold duration has passed since
    /// the last log.
    pub fn log(&mut self, f: impl FnOnce()) {
        let now = Instant::now();
        let should_log = now.duration_since(self.last_logged) > self.threshold;

        if should_log {
            self.last_logged = now;
            f();
        }
    }
}

impl Default for LogRateLimiter {
    fn default() -> Self {
        Self::new(Duration::from_millis(100))
    }
}

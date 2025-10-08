use crate::{
    builderhub::BuilderHubBuilder,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD, FLASHBOTS_SIGNATURE_HEADER,
    },
    metrics::{ForwarderMetrics, SystemMetrics},
    priority::{pchannel, Priority},
    types::{
        EncodedOrder, RawOrderMetadata, SystemBundle, SystemTransaction, UtcInstant, WithEncoding,
    },
    utils::UtcDateTimeHeader as _,
};
use alloy_primitives::Address;
use alloy_signer::SignerSync as _;
use alloy_signer_local::PrivateKeySigner;
use axum::http::HeaderValue;
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt};
use hyper::{header::CONTENT_TYPE, HeaderMap};
use reqwest::Url;
use revm_primitives::keccak256;
use serde_json::json;
use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use time::UtcDateTime;
use tracing::*;

#[derive(Debug)]
pub struct IngressForwarders {
    /// The orderflow signer.
    signer: PrivateKeySigner,
    /// The sender to the local builder forwarder.
    local: pchannel::UnboundedSender<Arc<ForwardingRequest>>,
    /// The senders to peer ingresses. Continuously updated from builderhub configuration.
    peers: Arc<DashMap<String, PeerHandle>>,
}

impl IngressForwarders {
    /// Create new ingress forwards.
    pub fn new(
        local: pchannel::UnboundedSender<Arc<ForwardingRequest>>,
        peers: Arc<DashMap<String, PeerHandle>>,
        signer: PrivateKeySigner,
    ) -> Self {
        Self { local, peers, signer }
    }

    /// Find peer name by address.
    pub fn find_peer(&self, address: Address) -> Option<String> {
        self.peers
            .iter()
            .find(|peer| peer.info.orderflow_proxy.ecdsa_pubkey_address == address)
            .map(|peer| peer.info.name.clone())
    }

    /// Broadcast bundle to all forwarders.
    pub fn broadcast_bundle(&self, bundle: SystemBundle) {
        let encoded_bundle = bundle.encode();

        // Create local request first
        let local = ForwardingRequest::user_to_local(encoded_bundle.clone().into());
        let _ = self.local.send(local.priority(), local);

        let body_hash = keccak256(encoded_bundle.encoding.as_ref());
        let signature = self.signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
        let signature_header = format!("{:?}:{}", self.signer.address(), signature);

        // Difference: we encode the whole bundle (including the signer), and we
        // add the signature header.
        let forward = ForwardingRequest::user_to_system(
            encoded_bundle.into(),
            signature_header,
            UtcDateTime::now(),
        );

        debug!(target: "ingress::forwarder", name = %ETH_SEND_BUNDLE_METHOD, peers = %self.peers.len(), "Sending bundle to peers");

        for entry in self.peers.iter() {
            let _ = entry.value().sender.send(forward.priority(), forward.clone());
        }
    }

    /// Broadcast transaction to all forwarders.
    pub fn broadcast_transaction(&self, transaction: SystemTransaction) {
        let encoded_transaction = transaction.encode();

        let local = ForwardingRequest::user_to_local(encoded_transaction.clone().into());
        let _ = self.local.send(local.priority(), local);

        let body_hash = keccak256(encoded_transaction.encoding.as_ref());
        let signature = self.signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
        let header = format!("{:?}:{}", self.signer.address(), signature);

        let forward = ForwardingRequest::user_to_system(
            encoded_transaction.into(),
            header,
            UtcDateTime::now(),
        );

        debug!(target: "ingress::forwarder", name = %ETH_SEND_RAW_TRANSACTION_METHOD, peers = %self.peers.len(), "Sending transaction to peers");

        for entry in self.peers.iter() {
            let _ = entry.value().sender.send(forward.priority(), forward.clone());
        }
    }

    /// Send request only to local forwarder.
    pub fn send_to_local(
        &self,
        priority: Priority,
        method: &str,
        param: serde_json::Value,
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
        let raw_order = RawOrderMetadata { priority: Priority::Medium, received_at };
        let order =
            EncodedOrder::RawOrder(WithEncoding { inner: raw_order, encoding: Arc::new(body) });

        let local = ForwardingRequest::system_to_local(order);
        let _ = self.local.send(priority, local);
    }
}

#[derive(Debug)]
pub struct PeerHandle {
    /// Peer info.
    pub info: BuilderHubBuilder,
    /// Sender to the peer forwarder.
    pub sender: pchannel::UnboundedSender<Arc<ForwardingRequest>>,
}

pub fn spawn_forwarder(
    name: String,
    url: String,
    client: reqwest::Client, // request client to be reused for http senders
) -> eyre::Result<pchannel::UnboundedSender<Arc<ForwardingRequest>>> {
    let (request_tx, request_rx) = pchannel::unbounded_channel();
    match Url::parse(&url)?.scheme() {
        "http" | "https" => {
            info!(%name, %url, "Spawning HTTP forwarder");
            tokio::spawn(HttpForwarder::new(client, name, url, request_rx));
        }

        scheme => {
            error!(scheme = %scheme, url = %url, builder = %name, "Unsupported URL scheme");
            eyre::bail!("unsupported url scheme {scheme}. url: {url}. builder: {name}")
        }
    }
    Ok(request_tx)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Bundle,
    Transaction,
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

#[derive(Debug)]
pub struct ForwardingRequest {
    /// The data to be forwarded.
    pub encoded_order: EncodedOrder,
    /// The headers of the request.
    pub headers: reqwest::header::HeaderMap,

    /// The direction of the forwarding request.
    pub direction: ForwardingDirection,
}

impl ForwardingRequest {
    pub fn user_to_local(encoded_order: EncodedOrder) -> Arc<Self> {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()));
        Arc::new(Self { encoded_order, headers, direction: ForwardingDirection::UserToLocal })
    }

    pub fn user_to_system(
        encoded_order: EncodedOrder,
        signature_header: String,
        sent_at_header: UtcDateTime,
    ) -> Arc<Self> {
        let headers = Self::create_headers(
            encoded_order.priority(),
            Some(signature_header),
            Some(sent_at_header),
        );
        Arc::new(Self { encoded_order, headers, direction: ForwardingDirection::UserToSystem })
    }

    pub fn system_to_local(encoded_order: EncodedOrder) -> Arc<Self> {
        let headers =
            Self::create_headers(encoded_order.priority(), None, Some(UtcDateTime::now()));
        Arc::new(Self { encoded_order, headers, direction: ForwardingDirection::SystemToLocal })
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

    pub fn is_big(&self) -> bool {
        self.encoded_order.encoding().len() > BIG_REQUEST_SIZE_THRESHOLD_KB
    }
}

#[derive(Debug)]
struct BuilderResponse<Ok, Err> {
    /// The instant at which request was sent.
    start_time: Instant,
    /// Builder response.
    response: Result<Ok, Err>,
}

type RequestFut<Ok, Err> = Pin<Box<dyn Future<Output = BuilderResponse<Ok, Err>> + Send>>;

struct HttpForwarder {
    client: reqwest::Client,
    name: String,
    url: String,
    request_rx: pchannel::UnboundedReceiver<Arc<ForwardingRequest>>,
    pending: FuturesUnordered<RequestFut<reqwest::Response, reqwest::Error>>,
}

impl HttpForwarder {
    fn new(
        client: reqwest::Client,
        name: String,
        url: String,
        request_rx: pchannel::UnboundedReceiver<Arc<ForwardingRequest>>,
    ) -> Self {
        Self { client, name, url, request_rx, pending: FuturesUnordered::new() }
    }

    fn on_builder_response(
        &mut self,
        response: BuilderResponse<reqwest::Response, reqwest::Error>,
    ) {
        // TODO: track this?
        let BuilderResponse { start_time, response, .. } = response;
        if let Err(error) = response {
            warn!(target: "ingress::forwarder", name = %self.name, ?error, elapsed = ?start_time.elapsed(), "Error forwarding request");
        }
    }
}

impl Future for HttpForwarder {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(maybe_request) = this.request_rx.poll_recv(cx) {
                let Some(request) = maybe_request else {
                    info!(target: "ingress::forwarder", name = %this.name, "Terminating forwarder");
                    return Poll::Ready(());
                };
                trace!(target: "ingress::forwarder", name = %this.name, ?request, "Sending request");
                this.pending.push(send_http_request(
                    this.client.clone(),
                    this.url.clone(),
                    request,
                ));
                continue;
            }

            if let Poll::Ready(Some(response)) = this.pending.poll_next_unpin(cx) {
                this.on_builder_response(response);
                continue;
            }

            return Poll::Pending;
        }
    }
}

fn send_http_request(
    client: reqwest::Client,
    url: String,
    request: Arc<ForwardingRequest>,
) -> RequestFut<reqwest::Response, reqwest::Error> {
    Box::pin(async move {
        let direction = request.direction;
        let is_big = request.is_big();

        // Try to avoid cloning the body and headers if there is only one reference.
        let (order, headers) = Arc::try_unwrap(request).map_or_else(
            |req| (req.encoded_order.clone(), req.headers.clone()),
            |inner| (inner.encoded_order, inner.headers),
        );

        match order {
            EncodedOrder::Bundle(_) => {
                SystemMetrics::record_e2e_bundle_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                );
            }
            EncodedOrder::Transaction(_) => {
                SystemMetrics::record_e2e_transaction_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                );
            }
            EncodedOrder::RawOrder(_) => {
                SystemMetrics::record_e2e_raw_order_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                );
            }
        }

        let start_time = Instant::now();
        let response =
            client.post(&url).body(order.encoding().to_vec()).headers(headers).send().await;
        ForwarderMetrics::record_rpc_call(url, start_time.elapsed(), is_big);

        BuilderResponse { start_time, response }
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_url_scheme() {
        assert_eq!(Url::parse("http://127.0.0.1:8080").unwrap().scheme(), "http");
        assert_eq!(Url::parse("https://127.0.0.1:8080").unwrap().scheme(), "https");
    }
}

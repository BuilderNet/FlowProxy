use crate::{
    builderhub::BuilderHubBuilder,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD, FLASHBOTS_SIGNATURE_HEADER,
    },
    metrics::{ForwarderMetrics, SystemMetrics},
    priority::{pchannel, Priority},
    types::{EncodedOrder, SystemBundle, SystemTransaction, UtcInstant},
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
        let received_at = bundle.received_at;

        // Create local request first
        let local = ForwardingRequest::user_to_local(bundle.clone().encode_local());
        let _ = self.local.send(local);

        let body = bundle.encode();

        let body_hash = keccak256(&body);
        let signature = self.signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
        let signature_header = format!("{:?}:{}", self.signer.address(), signature);

        // Difference: we encode the whole bundle (including the signer), and we
        // add the signature header.
        let forward = ForwardingRequest::user_to_system(
            priority,
            body,
            signature_header,
            UtcDateTime::now(),
            received_at,
        );

        debug!(target: "ingress::forwarder", name = %ETH_SEND_BUNDLE_METHOD, peers = %self.peers.len(), "Sending bundle to peers");

        for entry in self.peers.iter() {
            let _ = entry.value().sender.send(priority, forward.clone());
        }
    }

    /// Broadcast transaction to all forwarders.
    pub fn broadcast_transaction(&self, priority: Priority, transaction: SystemTransaction) {
        let body = transaction.encode();

        let local =
            ForwardingRequest::user_to_local(priority, body.clone(), transaction.received_at);
        let _ = self.local.send(priority, local);

        let body_hash = keccak256(&body);
        let signature = self.signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
        let header = format!("{:?}:{}", self.signer.address(), signature);

        let forward = ForwardingRequest::user_to_system(
            priority,
            body,
            header,
            UtcDateTime::now(),
            transaction.received_at,
        );

        debug!(target: "ingress::forwarder", name = %ETH_SEND_RAW_TRANSACTION_METHOD, peers = %self.peers.len(), "Sending transaction to peers");

        for entry in self.peers.iter() {
            let _ = entry.value().sender.send(priority, forward.clone());
        }
    }

    /// Send request only to local forwarder.
    pub fn send_to_local(&self, priority: Priority, method: &str, param: serde_json::Value) {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": [param]
        });

        let body = serde_json::to_vec(&json).unwrap();

        let local = ForwardingRequest::system_to_local(priority, body, UtcInstant::now());
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

pub enum OrderType {
    Bundle,
    Transaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardingType {
    UserToLocal,
    UserToSystem,
    SystemToLocal,
}

impl Display for ForwardingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForwardingType::UserToLocal => write!(f, "user_to_local"),
            ForwardingType::UserToSystem => write!(f, "user_to_system"),
            ForwardingType::SystemToLocal => write!(f, "system_to_local"),
        }
    }
}

/// The arguments for creating a [`ForwardingRequest`].
#[derive(Debug, Clone)]
struct ForwardingRequestArgs {
    encoded_order: EncodedOrder,
    /// The optional signature header.
    signature_header: Option<String>,
    /// The optional sent-at header.
    sent_at_header: Option<UtcDateTime>,

    _type: ForwardingType,
}

#[derive(Debug)]
pub struct ForwardingRequest {
    /// The data to be forwarded.
    pub encoded_order: EncodedOrder,
    /// The headers of the request.
    pub headers: reqwest::header::HeaderMap,
    /// The priority of data forwarded.
    pub priority: Priority,
    /// The instant at which we first received the data we're forwarding.
    pub received_at: UtcInstant,

    pub forwarding_type: ForwardingType,
}

impl ForwardingRequest {
    pub fn user_to_local(encoded_order: EncodedOrder) -> Arc<Self> {
        let args = ForwardingRequestArgs {
            encoded_order,
            signature_header: None,
            sent_at_header: None,
            _type: ForwardingType::UserToLocal,
        };
        Self::create_request(args)
    }

    pub fn user_to_system(
        encoded_order: EncodedOrder,
        signature_header: String,
        sent_at_header: UtcDateTime,
    ) -> Arc<Self> {
        let args = ForwardingRequestArgs {
            encoded_order,
            signature_header: Some(signature_header),
            sent_at_header: Some(sent_at_header),
            _type: ForwardingType::UserToSystem,
        };
        Self::create_request(args)
    }

    pub fn system_to_local(encoded_order: EncodedOrder) -> Arc<Self> {
        let args = ForwardingRequestArgs {
            encoded_order,
            signature_header: None,
            sent_at_header: None,
            _type: ForwardingType::SystemToLocal,
        };
        Self::create_request(args)
    }

    /// Create a new forwarding request to a builder. The [`BUILDERNET_SENT_AT_HEADER`] is formatted
    /// as a UNIX timestamp in nanoseconds.
    fn create_request(args: ForwardingRequestArgs) -> Arc<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(BUILDERNET_PRIORITY_HEADER, args.priority().to_string().parse().unwrap());
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if let Some(signature_header) = args.signature_header {
            headers.insert(FLASHBOTS_SIGNATURE_HEADER, signature_header.parse().unwrap());
        }

        if let Some(sent_at) = args.sent_at_header {
            headers.insert(BUILDERNET_SENT_AT_HEADER, sent_at.format_header());
        }

        let req = ForwardingRequest {
            body: args.body,
            headers,
            priority: args.priority,
            received_at: args.received_at,
            forwarding_type: args._type,
        };

        Arc::new(req)
    }

    pub fn is_big(&self) -> bool {
        self.body.len() > BIG_REQUEST_SIZE_THRESHOLD_KB
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
        let received_at = request.received_at;
        let priority = request.priority;
        let forwarding_type = request.forwarding_type;
        let is_big = request.is_big();

        // Try to avoid cloning the body and headers if there is only one reference.
        let (body, headers) = Arc::try_unwrap(request).map_or_else(
            |req| (req.body.clone(), req.headers.clone()),
            |inner| (inner.body, inner.headers),
        );

        SystemMetrics::record_e2e_order_processing_time(
            received_at.elapsed(),
            priority,
            forwarding_type,
        );

        let start_time = Instant::now();
        let response = client.post(&url).body(body).headers(headers).send().await;
        ForwarderMetrics::record_rpc_call(start_time.elapsed(), url, is_big);

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

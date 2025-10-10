use crate::{
    builderhub::BuilderHubBuilder,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD, FLASHBOTS_SIGNATURE_HEADER,
        MEV_SEND_BUNDLE_METHOD,
    },
    jsonrpc::{JsonRpcResponse, JsonRpcResponseTy},
    metrics::{ForwarderMetrics, SystemMetrics},
    priority::{pchannel, Priority},
    tasks::TaskExecutor,
    types::{
        EncodedOrder, RawOrderMetadata, SystemBundle, SystemMevShareBundle, SystemTransaction,
        UtcInstant, WithEncoding,
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
    time::{Duration, Instant},
};
use time::UtcDateTime;
use tokio::sync::mpsc;
use tracing::*;

const FORWARDER: &str = "ingress::forwarder";

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

        let signature_header = self.build_signature_header(encoded_bundle.encoding.as_ref());

        // Difference: we add the signature header.
        let forward = ForwardingRequest::user_to_system(
            encoded_bundle.into(),
            signature_header,
            UtcDateTime::now(),
        );

        debug!(target: FORWARDER, name = %ETH_SEND_BUNDLE_METHOD, peers = %self.peers.len(), "Sending bundle to peers");
        self.broadcast(forward);
    }

    /// Broadcast MEV share bundle to all forwarders.
    pub fn broadcast_mev_share_bundle(&self, priority: Priority, bundle: SystemMevShareBundle) {
        let encoded_bundle = bundle.encode();
        // Create local request first
        let local = ForwardingRequest::user_to_local(encoded_bundle.clone().into());
        let _ = self.local.send(priority, local);

        let signature_header = self.build_signature_header(encoded_bundle.encoding.as_ref());

        // Difference: we add the signature header.
        let forward = ForwardingRequest::user_to_system(
            encoded_bundle.into(),
            signature_header,
            UtcDateTime::now(),
        );

        debug!(target: FORWARDER, name = %MEV_SEND_BUNDLE_METHOD, peers = %self.peers.len(), "Sending bundle to peers");
        self.broadcast(forward);
    }

    /// Broadcast transaction to all forwarders.
    pub fn broadcast_transaction(&self, transaction: SystemTransaction) {
        let encoded_transaction = transaction.encode();

        let local = ForwardingRequest::user_to_local(encoded_transaction.clone().into());
        let _ = self.local.send(local.priority(), local);

        let signature_header = self.build_signature_header(encoded_transaction.encoding.as_ref());

        // Difference: we add the signature header.
        let forward = ForwardingRequest::user_to_system(
            encoded_transaction.into(),
            signature_header,
            UtcDateTime::now(),
        );

        debug!(target: FORWARDER, name = %ETH_SEND_RAW_TRANSACTION_METHOD, peers = %self.peers.len(), "Sending transaction to peers");
        self.broadcast(forward);
    }

    /// Sign and build the signature header in the form of `signer_address:signature`.
    fn build_signature_header(&self, body: &[u8]) -> String {
        let body_hash = keccak256(body);
        let signature = self.signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
        format!("{:?}:{}", self.signer.address(), signature)
    }

    /// Broadcast request to all peers.
    fn broadcast(&self, forward: Arc<ForwardingRequest>) {
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
            EncodedOrder::SystemOrder(WithEncoding { inner: raw_order, encoding: Arc::new(body) });

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
    task_executor: TaskExecutor,
) -> eyre::Result<pchannel::UnboundedSender<Arc<ForwardingRequest>>> {
    let (request_tx, request_rx) = pchannel::unbounded_channel();
    match Url::parse(&url)?.scheme() {
        "http" | "https" => {
            info!(%name, %url, "Spawning HTTP forwarder");
            let (forwarder, decoder) =
                HttpForwarder::new(client.clone(), name.clone(), url, request_rx);
            task_executor.spawn(forwarder);
            task_executor.spawn(decoder.run());
        }

        scheme => {
            error!(scheme = %scheme, url = %url, builder = %name, "Unsupported URL scheme");
            eyre::bail!("unsupported url scheme {scheme}. url: {url}. builder: {name}")
        }
    }
    Ok(request_tx)
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
struct Response<Ok, Err> {
    /// The instant at which request was sent.
    start_time: Instant,
    /// Builder response.
    response: Result<Ok, Err>,
}

type RequestFut<Ok, Err> = Pin<Box<dyn Future<Output = Response<Ok, Err>> + Send>>;

/// An HTTP forwarder that forwards requests to a builder.
struct HttpForwarder {
    client: reqwest::Client,
    /// The name of the builder we're forwarding to.
    peer_name: String,
    /// The URL of the peer.
    peer_url: String,
    /// The receiver of forwarding requests.
    request_rx: pchannel::UnboundedReceiver<Arc<ForwardingRequest>>,
    /// The sender to decode [`reqwest::Response`] errors.
    error_decoder_tx: mpsc::Sender<(reqwest::Response, Duration)>,
    /// The pending responses that need to be processed.
    pending: FuturesUnordered<RequestFut<reqwest::Response, reqwest::Error>>,
}

impl HttpForwarder {
    fn new(
        client: reqwest::Client,
        name: String,
        url: String,
        request_rx: pchannel::UnboundedReceiver<Arc<ForwardingRequest>>,
    ) -> (Self, ResponseErrorDecoder) {
        let (error_decoder_tx, error_decoder_rx) = mpsc::channel(8192);
        let decoder = ResponseErrorDecoder {
            peer_name: name.clone(),
            peer_url: url.clone(),
            rx: error_decoder_rx,
        };
        (
            Self {
                client,
                peer_name: name,
                peer_url: url,
                request_rx,
                pending: FuturesUnordered::new(),
                error_decoder_tx,
            },
            decoder,
        )
    }

    fn on_response(&mut self, response: Response<reqwest::Response, reqwest::Error>) {
        let Response { start_time, response: response_result, .. } = response;
        let elapsed = start_time.elapsed();

        match response_result {
            Ok(response) => {
                if let Err(e) = self.error_decoder_tx.try_send((response, elapsed)) {
                    error!(target: FORWARDER, peer_name = %self.peer_name, ?e, "Failed to send error response to decoder");
                }
            }
            Err(error) => {
                error!(target: FORWARDER, peer_name = %self.peer_name, ?error, ?elapsed, "Error forwarding request");

                error!(target: FORWARDER, name = %self.peer_name, ?error, ?elapsed, "Error from builder");

                // Parse the reason, which is either the status code reason of the error message
                // itself. If the request fails for non-network reasons, the status code may be
                // None.
                let reason = error
                    .status()
                    .map(|s| s.canonical_reason().map(|s| s.to_owned()))
                    .flatten()
                    .unwrap_or(error.to_string());

                ForwarderMetrics::increment_http_call_failures(self.peer_name.clone(), reason);
            }
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
                    info!(target: FORWARDER, name = %this.peer_name, "Terminating forwarder");
                    return Poll::Ready(());
                };
                trace!(target: FORWARDER, name = %this.peer_name, ?request, "Sending request");
                this.pending.push(send_http_request(
                    this.client.clone(),
                    this.peer_name.clone(),
                    this.peer_url.clone(),
                    request,
                ));

                ForwarderMetrics::set_inflight_requests(this.pending.len());
                continue;
            }

            if let Poll::Ready(Some(response)) = this.pending.poll_next_unpin(cx) {
                this.on_response(response);
                continue;
            }

            return Poll::Pending;
        }
    }
}

/// A [`reqwest::Response`] error decoder, associated to a certain [`HttpForwarder`] which traces
/// errors from client error responses.
#[derive(Debug)]
pub struct ResponseErrorDecoder {
    /// The name of the builder
    pub peer_name: String,
    /// The url of the builder
    pub peer_url: String,
    /// The receiver of the error responses.
    pub rx: mpsc::Receiver<(reqwest::Response, Duration)>,
}

impl ResponseErrorDecoder {
    pub async fn run(mut self) {
        while let Some((response, elapsed)) = self.rx.recv().await {
            let status = response.status();

            match response.json::<JsonRpcResponse<serde_json::Value>>().await {
                Ok(body) => {
                    if let JsonRpcResponseTy::Error { code, message } = body.result_or_error {
                        error!(target: FORWARDER, peer_name = %self.peer_name, peer_url = %self.peer_url, %code, %message, ?elapsed, "Decoded error response from builder");
                        ForwarderMetrics::increment_rpc_call_failures(self.peer_name.clone(), code);
                    }
                }
                Err(e) => {
                    warn!(target: FORWARDER,  ?e, peer_name = %self.peer_name, peer_url = %self.peer_url, %status, ?elapsed, "Failed decode response into JSON-RPC");
                    ForwarderMetrics::increment_json_rpc_decoding_failures(self.peer_name.clone());
                }
            }
        }
    }
}

fn send_http_request(
    client: reqwest::Client,
    peer_name: String,
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

        let order_type = order.order_type();

        match order {
            EncodedOrder::Bundle(_) => {
                SystemMetrics::record_e2e_bundle_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                    is_big,
                );
            }
            EncodedOrder::MevShareBundle(_) => {
                SystemMetrics::record_e2e_mev_share_bundle_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                    is_big,
                );
            }
            EncodedOrder::Transaction(_) => {
                SystemMetrics::record_e2e_transaction_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                    is_big,
                );
            }
            EncodedOrder::SystemOrder(_) => {
                SystemMetrics::record_e2e_system_order_processing_time(
                    order.received_at().elapsed(),
                    order.priority(),
                    direction,
                    order_type,
                    is_big,
                );
            }
        }

        let start_time = Instant::now();
        let response =
            client.post(&url).body(order.encoding().to_vec()).headers(headers).send().await;

        if response.is_ok() {
            ForwarderMetrics::record_rpc_call(peer_name, order_type, start_time.elapsed(), is_big);
        }

        Response { start_time, response }
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

use crate::{
    builderhub,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        FLASHBOTS_SIGNATURE_HEADER,
    },
    jsonrpc::{JsonRpcResponse, JsonRpcResponseTy},
    metrics::{ForwarderMetrics, SYSTEM_METRICS},
    primitives::{
        EncodedOrder, RawOrderMetadata, SystemBundle, SystemMevShareBundle, SystemTransaction,
        UtcInstant, WithEncoding,
    },
    priority::{self, workers::PriorityWorkers, Priority},
    utils::UtcDateTimeHeader as _,
};
use alloy_primitives::{Address, B256};
use alloy_signer::SignerSync as _;
use alloy_signer_local::PrivateKeySigner;
use axum::http::HeaderValue;
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt};
use hyper::{header::CONTENT_TYPE, HeaderMap, StatusCode};
use rbuilder_utils::tasks::TaskExecutor;
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

pub mod client;

use client::ClientPool;

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

    /// Broadcast bundle to all forwarders.
    pub(crate) async fn broadcast_bundle(&self, bundle: SystemBundle) {
        let priority = bundle.metadata.priority;
        let encoded_bundle = bundle.encode();

        // Create local request first
        let local = Arc::new(ForwardingRequest::user_to_local(encoded_bundle.clone().into()));
        let _ = self.local.send(local.priority(), local);

        let signer = self.signer.clone();
        let encoding = encoded_bundle.encoding.clone();
        let signature_header = self
            .workers
            .spawn_with_priority(priority, move || {
                build_signature_header(&signer, encoding.as_ref())
            })
            .await;

        // Difference: we add the signature header.
        let forward = Arc::new(ForwardingRequest::user_to_system(
            encoded_bundle.into(),
            signature_header,
            UtcDateTime::now(),
        ));

        debug!(peers = %self.peers.len(), "sending bundle to peers");
        self.broadcast(forward);
    }

    /// Broadcast MEV share bundle to all forwarders.
    pub(crate) async fn broadcast_mev_share_bundle(
        &self,
        priority: Priority,
        bundle: SystemMevShareBundle,
    ) {
        let encoded_bundle = bundle.encode();
        // Create local request first
        let local = Arc::new(ForwardingRequest::user_to_local(encoded_bundle.clone().into()));
        let _ = self.local.send(priority, local);

        let signer = self.signer.clone();
        let encoding = encoded_bundle.encoding.clone();
        let signature_header = self
            .workers
            .spawn_with_priority(priority, move || {
                build_signature_header(&signer, encoding.as_ref())
            })
            .await;

        // Difference: we add the signature header.
        let forward = Arc::new(ForwardingRequest::user_to_system(
            encoded_bundle.into(),
            signature_header,
            UtcDateTime::now(),
        ));

        debug!(peers = %self.peers.len(), "sending bundle to peers");
        self.broadcast(forward);
    }

    /// Broadcast transaction to all forwarders.
    pub(crate) async fn broadcast_transaction(&self, transaction: SystemTransaction) {
        let priority = transaction.priority;
        let encoded_transaction = transaction.encode();

        let local = Arc::new(ForwardingRequest::user_to_local(encoded_transaction.clone().into()));
        let _ = self.local.send(local.priority(), local);

        let signer = self.signer.clone();
        let encoding = encoded_transaction.encoding.clone();
        let signature_header = self
            .workers
            .spawn_with_priority(priority, move || {
                build_signature_header(&signer, encoding.as_ref())
            })
            .await;

        // Difference: we add the signature header.
        let forward = Arc::new(ForwardingRequest::user_to_system(
            encoded_transaction.into(),
            signature_header,
            UtcDateTime::now(),
        ));

        debug!(peers = %self.peers.len(), "sending transaction to peers");
        self.broadcast(forward);
    }

    /// Broadcast request to all peers.
    fn broadcast(&self, forward: Arc<ForwardingRequest>) {
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
        let order =
            EncodedOrder::SystemOrder(WithEncoding { inner: raw_order, encoding: Arc::new(body) });

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

pub fn spawn_forwarder(
    name: String,
    url: String,
    client: ClientPool, // request client to be reused for http senders
    task_executor: &TaskExecutor,
) -> eyre::Result<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
    let (request_tx, request_rx) = priority::channel::unbounded_channel();
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

/// Sign and build the signature header in the form of `signer_address:signature`.
fn build_signature_header(signer: &PrivateKeySigner, body: &[u8]) -> String {
    let body_hash = keccak256(body);
    let signature = signer.sign_message_sync(format!("{body_hash:?}").as_bytes()).unwrap();
    format!("{:?}:{}", signer.address(), signature)
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

    pub fn user_to_system(
        encoded_order: EncodedOrder,
        signature_header: String,
        sent_at_header: UtcDateTime,
    ) -> Self {
        let headers = Self::create_headers(
            encoded_order.priority(),
            Some(signature_header),
            Some(sent_at_header),
        );
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

    pub fn is_big(&self) -> bool {
        self.encoded_order.encoding().len() > BIG_REQUEST_SIZE_THRESHOLD_KB
    }

    /// Returns the hash of the encoded order.
    pub fn hash(&self) -> B256 {
        self.encoded_order.hash()
    }
}

/// The response received by the [`HttpForwarder`] after sending a request.
#[derive(Debug)]
struct ForwarderResponse<Ok, Err> {
    /// Whether this was a big request.
    is_big: bool,
    /// The type of the order.
    order_type: &'static str,
    /// The hash of the order forwarded.
    hash: B256,
    /// The instant at which request was sent.
    start_time: Instant,
    /// Builder response.
    response: Result<Ok, Err>,

    /// The parent span associated with this response.
    span: tracing::Span,
}

type RequestFut<Ok, Err> = Pin<Box<dyn Future<Output = ForwarderResponse<Ok, Err>> + Send>>;

/// An HTTP forwarder that forwards requests to a peer.
struct HttpForwarder {
    client: ClientPool,
    /// The name of the builder we're forwarding to.
    peer_name: String,
    /// The URL of the peer.
    peer_url: String,
    /// The receiver of forwarding requests.
    request_rx: priority::channel::UnboundedReceiver<Arc<ForwardingRequest>>,
    /// The sender to decode [`reqwest::Response`] errors.
    error_decoder_tx: mpsc::Sender<ErrorDecoderInput>,
    /// The pending responses that need to be processed.
    pending: FuturesUnordered<RequestFut<reqwest::Response, reqwest::Error>>,
    /// The metrics for the forwarder.
    metrics: ForwarderMetrics,
}

impl HttpForwarder {
    fn new(
        client: ClientPool,
        name: String,
        url: String,
        request_rx: priority::channel::UnboundedReceiver<Arc<ForwardingRequest>>,
    ) -> (Self, ResponseErrorDecoder) {
        let (error_decoder_tx, error_decoder_rx) = mpsc::channel(8192);
        let metrics = ForwarderMetrics::builder().with_label("peer_name", name.clone()).build();
        let decoder = ResponseErrorDecoder {
            peer_name: name.clone(),
            peer_url: url.clone(),
            rx: error_decoder_rx,
            metrics: metrics.clone(),
        };

        (
            Self {
                client,
                peer_name: name,
                peer_url: url,
                request_rx,
                pending: FuturesUnordered::new(),
                error_decoder_tx,
                metrics,
            },
            decoder,
        )
    }

    /// Send an HTTP request to the peer, returning a future that resolves to the response.
    fn send_http_request(
        &self,
        request: Arc<ForwardingRequest>,
    ) -> RequestFut<reqwest::Response, reqwest::Error> {
        let client_pool = self.client.clone();
        let peer_url = self.peer_url.clone();

        let request_span = request.span.clone();
        let span = tracing::info_span!(parent: request_span.clone(), "http_forwarder_request", peer_url = %self.peer_url, is_big = request.is_big());

        let fut = async move {
            let direction = request.direction;
            let is_big = request.is_big();
            let hash = request.hash();

            // Try to avoid cloning the body and headers if there is only one reference.
            let (order, headers) = Arc::try_unwrap(request).map_or_else(
                |req| (req.encoded_order.clone(), req.headers.clone()),
                |inner| (inner.encoded_order, inner.headers),
            );

            record_e2e_metrics(&order, &direction, is_big);

            let order_type = order.order_type();
            let start_time = Instant::now();
            let response = client_pool
                .client(is_big)
                .post(peer_url)
                .body(order.encoding().to_vec())
                .headers(headers)
                .send()
                .await;
            tracing::trace!(elapsed = ?start_time.elapsed(), "received response");

            ForwarderResponse { start_time, response, is_big, order_type, hash, span: request_span }
        } // We first want to enter the parent span, then the local span.
        .instrument(span);

        Box::pin(fut)
    }

    #[tracing::instrument(skip_all, name = "http_forwarder_response"
        fields(
            peer_url = %self.peer_url,
            is_big = response.is_big,
            elapsed = tracing::field::Empty,
            status = tracing::field::Empty,
    ))]
    fn on_response(&mut self, response: ForwarderResponse<reqwest::Response, reqwest::Error>) {
        let ForwarderResponse {
            start_time,
            response: response_result,
            order_type,
            is_big,
            hash,
            ..
        } = response;
        let elapsed = start_time.elapsed();

        tracing::Span::current().record("elapsed", tracing::field::debug(&elapsed));

        match response_result {
            Ok(response) => {
                let status = response.status();
                tracing::Span::current().record("status", tracing::field::debug(&status));

                // Print warning if the RPC call took more than 1 second.
                if elapsed > Duration::from_secs(1) {
                    warn!("long rpc call");
                }

                if status.is_success() {
                    trace!("received success response");

                    if status != StatusCode::OK {
                        warn!("non-ok status code");
                    }

                    // Only record success if the status is OK.
                    self.metrics
                        .rpc_call_duration(order_type, is_big.to_string())
                        .observe(elapsed.as_secs_f64());
                } else {
                    // If we have a non-OK status code, also record it.
                    error!("failed to forward request");
                    let reason =
                        status.canonical_reason().map(String::from).unwrap_or(status.to_string());

                    self.metrics.http_call_failures(reason).inc();

                    if let Err(e) =
                        self.error_decoder_tx.try_send(ErrorDecoderInput::new(hash, response))
                    {
                        error!(?e, "failed to send error response to decoder");
                    }
                }
            }
            Err(error) => {
                error!("error forwarding request");

                // Parse the reason, which is either the status code reason of the error message
                // itself. If the request fails for non-network reasons, the status code may be
                // None.
                let reason = error
                    .status()
                    .and_then(|s| s.canonical_reason().map(String::from))
                    .unwrap_or(format!("{error:?}"));

                if error.is_connect() {
                    warn!(?reason, "connection error");
                    self.metrics.http_connect_failures(reason).inc();
                } else {
                    self.metrics.http_call_failures(reason).inc();
                }
            }
        }
    }
}

impl Future for HttpForwarder {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // First poll for completed work.
            if let Poll::Ready(Some(response)) = this.pending.poll_next_unpin(cx) {
                response.span.clone().in_scope(|| {
                    this.on_response(response);
                });
                continue;
            }

            // Then accept new requests.
            if let Poll::Ready(maybe_request) = this.request_rx.poll_recv(cx) {
                let Some(request) = maybe_request else {
                    info!(name = %this.peer_name, "terminating forwarder");
                    return Poll::Ready(());
                };

                let fut = this.send_http_request(request);
                this.pending.push(fut);

                this.metrics.inflight_requests().set(this.pending.len());
                continue;
            }

            return Poll::Pending;
        }
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
        EncodedOrder::SystemOrder(_) => {
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

/// The input to the error decoder, containing the response to the request and its associated order
/// hash.
#[derive(Debug)]
pub struct ErrorDecoderInput {
    /// The hash of the order forwarded.
    pub hash: B256,
    /// The error response to be decoded.
    pub response: reqwest::Response,

    /// The tracing span associated with this data.
    pub span: tracing::Span,
}

impl ErrorDecoderInput {
    /// Create a new error decoder input.
    pub fn new(hash: B256, response: reqwest::Response) -> Self {
        Self { hash, response, span: tracing::Span::current() }
    }

    /// Set the tracing span for this input.
    pub fn with_span(self, span: tracing::Span) -> Self {
        Self { span, ..self }
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
    pub rx: mpsc::Receiver<ErrorDecoderInput>,
    /// Metrics from the associated forwarder.
    pub(crate) metrics: ForwarderMetrics,
}

impl ResponseErrorDecoder {
    #[tracing::instrument(skip_all, name = "response_error_decode")]
    async fn decode(&self, input: ErrorDecoderInput) {
        match input.response.json::<JsonRpcResponse<serde_json::Value>>().await {
            Ok(body) => {
                if let JsonRpcResponseTy::Error { code, message } = body.result_or_error {
                    error!(%code, %message, "decoded error response from builder");
                    self.metrics.rpc_call_failures(code.to_string()).inc();
                }
            }
            Err(e) => {
                warn!(?e, "failed to decode response into json-rpc");
                self.metrics.json_rpc_decoding_failures().inc();
            }
        }
    }

    /// Run the error decoder actor in loop.
    pub async fn run(mut self) {
        while let Some(input) = self.rx.recv().await {
            let span = input.span.clone();
            self.decode(input).instrument(span).await;
        }
    }
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

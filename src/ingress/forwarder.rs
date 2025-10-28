use crate::{
    builderhub,
    consts::{
        BIG_REQUEST_SIZE_THRESHOLD_KB, BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER,
        FLASHBOTS_SIGNATURE_HEADER,
    },
    jsonrpc::{JsonRpcResponse, JsonRpcResponseTy},
    metrics::{ForwarderMetrics, SystemMetrics},
    primitives::{
        EncodedOrder, RawOrderMetadata, SystemBundle, SystemMevShareBundle, SystemTransaction,
        UtcInstant, WithEncoding,
    },
    priority::{pchannel, Priority},
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
    pub(crate) fn broadcast_bundle(&self, bundle: SystemBundle) {
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

        debug!(peers = %self.peers.len(), "sending bundle to peers");
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

        debug!(peers = %self.peers.len(), "sending bundle to peers");
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

        debug!(peers = %self.peers.len(), "sending transaction to peers");
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

        let local = ForwardingRequest::system_to_local(order);
        let _ = self.local.send(priority, local);
    }
}

#[derive(Debug)]
pub struct PeerHandle {
    /// Peer info.
    pub info: builderhub::Peer,
    /// Sender to the peer forwarder.
    pub sender: pchannel::UnboundedSender<Arc<ForwardingRequest>>,
}

pub fn spawn_forwarder(
    name: String,
    url: String,
    client: reqwest::Client, // request client to be reused for http senders
    task_executor: &TaskExecutor,
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
}

type RequestFut<Ok, Err> = Pin<Box<dyn Future<Output = ForwarderResponse<Ok, Err>> + Send>>;

/// An HTTP forwarder that forwards requests to a peer.
struct HttpForwarder {
    client: reqwest::Client,
    /// The name of the builder we're forwarding to.
    peer_name: String,
    /// The URL of the peer.
    peer_url: String,
    /// The receiver of forwarding requests.
    request_rx: pchannel::UnboundedReceiver<Arc<ForwardingRequest>>,
    /// The sender to decode [`reqwest::Response`] errors.
    error_decoder_tx: mpsc::Sender<ErrorDecoderInput>,
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

    /// Send an HTTP request to the peer, returning a future that resolves to the response.
    #[tracing::instrument(skip_all, name = "http_forwarder_request"
        fields(
            hash = %request.hash(),
            order_type = %request.encoded_order.order_type(),
            direction = %request.direction,
            is_big = request.is_big(),
    ))]
    fn send_http_request(
        &self,
        request: Arc<ForwardingRequest>,
    ) -> RequestFut<reqwest::Response, reqwest::Error> {
        let client = self.client.clone();
        let peer_url = self.peer_url.clone();

        let span = tracing::Span::current();
        let fut = async move {
            let direction = request.direction;
            let is_big = request.is_big();
            let hash = request.hash();

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

            let order_type = order.order_type();
            let start_time = Instant::now();
            let response =
                client.post(peer_url).body(order.encoding().to_vec()).headers(headers).send().await;
            tracing::trace!(elapsed = ?start_time.elapsed(), "received response");

            ForwarderResponse { start_time, response, is_big, order_type, hash }
        }
        .instrument(span);

        Box::pin(fut)
    }

    #[tracing::instrument(skip_all, name = "http_forwarder_response"
        fields(
            peer = %self.peer_name,
            hash = %response.hash,
            order_type = response.order_type,
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
                    if status != StatusCode::OK {
                        warn!("non-ok status code");
                    }

                    // Only record success if the status is OK.
                    ForwarderMetrics::record_rpc_call(
                        self.peer_name.clone(),
                        order_type,
                        elapsed,
                        is_big,
                    );
                } else {
                    // If we have a non-OK status code, also record it.
                    error!("failed to forward request");
                    ForwarderMetrics::increment_http_call_failures(
                        self.peer_name.clone(),
                        status.canonical_reason().map(String::from).unwrap_or(status.to_string()),
                    );

                    if let Err(e) =
                        self.error_decoder_tx.try_send(ErrorDecoderInput { hash, response })
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
                    ForwarderMetrics::increment_http_connect_failures(
                        self.peer_name.clone(),
                        reason,
                    );
                } else {
                    ForwarderMetrics::increment_http_call_failures(self.peer_name.clone(), reason);
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
                this.on_response(response);
                continue;
            }

            // Then accept new requests.
            if let Poll::Ready(maybe_request) = this.request_rx.poll_recv(cx) {
                let Some(request) = maybe_request else {
                    info!(name = %this.peer_name, "terminating forwarder");
                    return Poll::Ready(());
                };

                this.pending.push(this.send_http_request(request));

                ForwarderMetrics::set_inflight_requests(this.pending.len());
                continue;
            }

            return Poll::Pending;
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
}

impl ResponseErrorDecoder {
    #[tracing::instrument(skip_all, name = "response_error_decode"
        fields(
            peer = %self.peer_name,
            peer_url = %self.peer_url,
            hash = %input.hash,
            status = %input.response.status(),
    ))]
    async fn decode(&self, input: ErrorDecoderInput) {
        match input.response.json::<JsonRpcResponse<serde_json::Value>>().await {
            Ok(body) => {
                if let JsonRpcResponseTy::Error { code, message } = body.result_or_error {
                    error!(%code, %message, "decoded error response from builder");
                    ForwarderMetrics::increment_rpc_call_failures(self.peer_name.clone(), code);
                }
            }
            Err(e) => {
                warn!(?e, "failed to decode response into json-rpc");
                ForwarderMetrics::increment_json_rpc_decoding_failures(self.peer_name.clone());
            }
        }
    }

    /// Run the error decoder actor in loop.
    pub async fn run(mut self) {
        while let Some(input) = self.rx.recv().await {
            self.decode(input).await;
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

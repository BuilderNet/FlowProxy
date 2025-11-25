use crate::{
    forwarder::{
        client::{AsyncTransport, ReqSocketIpPool},
        record_e2e_metrics, ForwardingRequest,
    },
    jsonrpc::{JsonRpcResponse, JsonRpcResponseTy},
    metrics::ForwarderMetrics,
    priority::{self},
};
use alloy_primitives::B256;
use alloy_rlp::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use msg_socket::ReqError;
use rbuilder_utils::tasks::TaskExecutor;
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::Instrument as _;

pub fn spawn_tcp_forwarder<T: AsyncTransport>(
    name: String,
    address: SocketAddr,
    client: ReqSocketIpPool<T>,
    task_executor: &TaskExecutor,
) -> priority::channel::UnboundedSender<Arc<ForwardingRequest>> {
    let (request_tx, request_rx) = priority::channel::unbounded_channel();

    tracing::info!(addr = %address, "spawning tcp forwarder");
    let forwarder = TcpForwarder::new(client, name.clone(), address, request_rx);
    task_executor.spawn(forwarder);

    request_tx
}

/// The response received by the [`TcpForwarder`] after sending a request.
#[derive(Debug)]
struct ForwarderResponse<Ok, Err> {
    /// Whether this was a big request.
    is_big: bool,
    /// The type of the order.
    order_type: &'static str,
    /// The hash of the order forwarded.
    #[allow(dead_code)]
    hash: B256,
    /// The instant at which request was sent.
    start_time: Instant,
    /// Builder response.
    response: Result<Ok, Err>,

    /// The parent span associated with this response.
    span: tracing::Span,
}

type RequestFut<Ok, Err> = Pin<Box<dyn Future<Output = ForwarderResponse<Ok, Err>> + Send>>;

/// An TCP forwarder that forwards requests to a peer.
struct TcpForwarder<T: AsyncTransport> {
    client: ReqSocketIpPool<T>,
    /// The name of the builder we're forwarding to.
    peer_name: String,
    /// The URL of the peer.
    peer_address: SocketAddr,
    /// The receiver of forwarding requests.
    request_rx: priority::channel::UnboundedReceiver<Arc<ForwardingRequest>>,
    /// The pending responses that need to be processed.
    pending: FuturesUnordered<RequestFut<Bytes, ReqError>>,
    /// The metrics for the forwarder.
    metrics: ForwarderMetrics,
}

impl<T: AsyncTransport> TcpForwarder<T> {
    fn new(
        client: ReqSocketIpPool<T>,
        name: String,
        address: SocketAddr,
        request_rx: priority::channel::UnboundedReceiver<Arc<ForwardingRequest>>,
    ) -> Self {
        let metrics = ForwarderMetrics::builder()
            .with_label("peer_name", name.clone())
            .with_label("transport", "tcp")
            .build();
        Self {
            client,
            peer_name: name,
            peer_address: address,
            request_rx,
            pending: FuturesUnordered::new(),
            metrics,
        }
    }

    /// Send an TCP request to the peer, returning a future that resolves to the response.
    fn send_tcp_request(&self, request: Arc<ForwardingRequest>) -> RequestFut<Bytes, ReqError> {
        let client_pool = self.client.clone();

        let request_span = request.span.clone();
        let span = tracing::info_span!(parent: request_span.clone(), "forwarder", protocol = "tcp", peer_url = %self.peer_address, is_big = request.is_big());
        let span_clone = span.clone();

        let fut = async move {
            let direction = request.direction;
            let is_big = request.is_big();
            let hash = request.hash();

            let order_type = request.encoded_order.order_type();

            record_e2e_metrics(&request.encoded_order, &direction, is_big);

            let bytes = request.encoded_order.encoding_tcp_forwarder().expect("tcp bytes to send");

            tracing::trace!(bytes_len = bytes.len(), "sending tcp request");
            let start_time = Instant::now();
            let response = client_pool.socket().request(bytes.into()).await;

            ForwarderResponse { start_time, response, is_big, order_type, hash, span: span_clone }
        } // We first want to enter the parent span, then the local span.
        .instrument(span);

        Box::pin(fut)
    }

    fn on_response(&mut self, response: ForwarderResponse<Bytes, ReqError>) {
        let ForwarderResponse {
            start_time,
            response: response_result,
            order_type,
            is_big,
            span,
            ..
        } = response;
        let elapsed = start_time.elapsed();

        let _g = span.enter();
        tracing::trace!(elapsed = ?start_time.elapsed(), "received response");

        let Ok(response) = response_result.inspect_err(|e| {
            tracing::error!(?e, "failed to forward request");
            self.metrics.tcp_call_failures(e.to_string()).inc();
        }) else {
            return;
        };

        self.metrics
            .rpc_call_duration(order_type, is_big.to_string())
            .observe(elapsed.as_secs_f64());

        let response =
            match serde_json::from_slice::<JsonRpcResponse<serde_json::Value>>(response.as_ref()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(?e, "failed to deserialize response");
                    return;
                }
            };

        // Print warning if the RPC call took more than 1 second.
        if elapsed > Duration::from_secs(1) {
            tracing::warn!("long rpc call");
        }

        if let JsonRpcResponseTy::Error { code, message } = response.result_or_error {
            tracing::error!(?code, ?message, "received error response");
            self.metrics.rpc_call_failures(code.to_string()).inc();
        }
    }
}

impl<T: AsyncTransport> Future for TcpForwarder<T> {
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
                    tracing::info!(name = %this.peer_name, "terminating forwarder");
                    return Poll::Ready(());
                };

                let fut = this.send_tcp_request(request);
                this.pending.push(fut);

                this.metrics.inflight_requests().set(this.pending.len());
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

/// A [`reqwest::Response`] error decoder, associated to a certain [`tcpForwarder`] which traces
/// errors from client error responses.
#[derive(Debug)]
pub struct ResponseErrorDecoder {
    /// The name of the builder
    pub peer_name: String,
    /// The socket address of the peer
    pub peer_address: SocketAddr,
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
                    tracing::error!(%code, %message, "decoded error response from builder");
                    self.metrics.rpc_call_failures(code.to_string()).inc();
                }
            }
            Err(e) => {
                tracing::warn!(?e, "failed to decode response into json-rpc");
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

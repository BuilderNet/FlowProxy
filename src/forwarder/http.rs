use crate::{
    forwarder::{client::HttpClientPool, record_e2e_metrics, ForwardingRequest},
    jsonrpc::{JsonRpcResponse, JsonRpcResponseTy},
    metrics::ForwarderMetrics,
    priority::{self},
};
use alloy_primitives::B256;
use futures::{stream::FuturesUnordered, StreamExt};
use hyper::StatusCode;
use rbuilder_utils::tasks::TaskExecutor;
use reqwest::Url;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::*;

pub fn spawn_http_forwarder(
    name: String,
    url: String,
    client: HttpClientPool, // request client to be reused for http senders
    task_executor: &TaskExecutor,
) -> eyre::Result<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
    let (request_tx, request_rx) = priority::channel::unbounded_channel();
    match Url::parse(&url)?.scheme() {
        "http" | "https" => {
            info!(name, %url, "spawning http forwarder");
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
    client: HttpClientPool,
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
        client: HttpClientPool,
        name: String,
        url: String,
        request_rx: priority::channel::UnboundedReceiver<Arc<ForwardingRequest>>,
    ) -> (Self, ResponseErrorDecoder) {
        let (error_decoder_tx, error_decoder_rx) = mpsc::channel(8192);
        let metrics = ForwarderMetrics::builder()
            .with_label("peer_name", name.clone())
            .with_label("transport", "http")
            .build();
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

        let span = tracing::info_span!(parent: request.span.clone(), "forwarder", protocol = "http", peer_url = %self.peer_url, is_big = request.is_big());
        let span_clone = span.clone();

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
                .client()
                .post(peer_url)
                .body(order.encoding().to_vec())
                .headers(headers)
                .send()
                .await;
            tracing::trace!(elapsed = ?start_time.elapsed(), "received response");

            ForwarderResponse { start_time, response, is_big, order_type, hash, span: span_clone }
        } // We first want to enter the parent span, then the local span.
        .instrument(span);

        Box::pin(fut)
    }

    fn on_response(&mut self, response: ForwarderResponse<reqwest::Response, reqwest::Error>) {
        let ForwarderResponse {
            start_time,
            response: response_result,
            order_type,
            is_big,
            span,
            hash,
        } = response;
        let elapsed = start_time.elapsed();

        match response_result {
            Ok(response) => {
                let status = response.status();
                let _g = tracing::info_span!(parent: span.clone(), "response", ?elapsed, ?status)
                    .entered();

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
    async fn decode(&self, input: ErrorDecoderInput) {
        let _g = input.span.enter();
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
    use reqwest::Url;

    #[test]
    fn parse_url_scheme() {
        assert_eq!(Url::parse("http://127.0.0.1:8080").unwrap().scheme(), "http");
        assert_eq!(Url::parse("https://127.0.0.1:8080").unwrap().scheme(), "https");
    }
}

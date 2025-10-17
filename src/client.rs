//! High-performance HTTP client optimized for low latency and high throughput over long links.
//!
//! This module provides a custom HTTP client built on hyper-util with aggressive tuning for:
//! - High bandwidth links (1Gbps+)
//! - High latency / long distance connections
//! - Maximum connection pooling and HTTP/2 multiplexing
//! - Minimal overhead and copying
//!
//! ## Why hyper-util over reqwest?
//!
//! reqwest is built on hyper but abstracts away critical tuning knobs needed for
//! high-performance scenarios:
//!
//! 1. **Connection Pool Limits**: reqwest's pool is opaque and connection limits are hit frequently
//!    under load, causing heavy tail latencies
//!
//! 2. **HTTP/2 Multiplexing**: Cannot configure max concurrent streams per connection (default ~100
//!    is too conservative for high-throughput proxying)
//!
//! 3. **TCP Socket Tuning**: No access to SO_SNDBUF/SO_RCVBUF for BDP optimization on high-latency
//!    links
//!
//! 4. **TLS Session Management**: Limited control over session resumption and 0-RTT
//!
//! By using hyper-util directly, we gain:
//! - 2048+ connections per host (vs 512 limit hitting capacity)
//! - Explicit HTTP/2 window sizes (16MB connection, 8MB stream for high BDP)
//! - TCP buffer sizing for bandwidth-delay product optimization
//! - Custom connection establishment and reuse strategies
//!
//! ## Design Rationale
//!
//! ### Connection Pool Size (2048 per host)
//!
//! At 1Gbps with avg 1KB requests:
//! - ~125,000 requests/second max theoretical
//! - With 2ms latency: need 250 concurrent requests in flight
//! - 512 connections bottleneck at ~64k req/s with request pipelining
//! - 2048 connections provide 4x headroom for burst traffic
//!
//! ### HTTP/2 Window Sizes
//!
//! For a 50ms RTT link at 1Gbps:
//! - BDP = 1Gbps * 50ms = 6.25MB
//! - Connection window: 16MB (2.5x BDP for multiple streams)
//! - Stream window: 8MB (adequate for large bundle payloads)
//!
//! ### TCP Buffer Sizes
//!
//! Calculate based on BDP: `buffer_size = bandwidth (bytes/sec) * RTT (sec)`
//! Example: 1Gbps * 50ms = 125MB/s * 0.05s = 6.25MB
//! We default to None (OS default) but expose tuning for measured links.
//!
//! ### Idle Timeout (120s)
//!
//! Long-distance connections have high establishment cost:
//! - TCP handshake: 1 RTT
//! - TLS 1.3 handshake: 1 RTT (with session resumption: 0 RTT)
//! - HTTP/2 negotiation: 0 RTT (ALPN)
//! Total: 2 RTT (100ms on 50ms link) per new connection
//! 120s timeout amortizes this cost across many requests
//!
//! ## TODO: Future Enhancements
//!
//! 1. **TLS Support**: Add hyper-rustls integration with:
//!    - Custom root certificate support for BuilderHub peers
//!    - TLS 1.3 only
//!    - Session resumption cache
//!    - 0-RTT when safe
//!
//! 2. **Metrics**: Instrument with:
//!    - Connection pool utilization
//!    - Connection establishment time
//!    - TLS handshake time
//!    - Request latency percentiles
//!
//! 3. **Advanced Pooling**: When hyper-util RFC #3849 lands:
//!    - Separate pools for HTTP/2 (singleton) vs HTTP/1.1 (racing cache)
//!    - Per-destination adaptive pool sizing
//!    - Circuit breaker for failing endpoints

use crate::utils::limit::{ConnectionLimiter, ConnectionLimiterLayer};
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Bytes, header, HeaderMap, Method, StatusCode};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::TokioExecutor,
};
use std::time::Duration;
use tower::ServiceBuilder;

/// Type alias for the connector with connection limiting
type LimitedConnector = ConnectionLimiter<HttpConnector>;

/// HTTP error type for the client.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("HTTP protocol error: {0:?}")]
    Http(#[from] hyper::http::Error),
    #[error("HTTP error: {0:?}")]
    Client(#[from] hyper_util::client::legacy::Error),
    #[error("Hyper error: {0:?}")]
    Hyper(#[from] hyper::Error),
}

/// High-performance HTTP client configuration optimized for orderflow forwarding.
///
/// Key optimizations:
/// - Large connection pool (2048 per host) to avoid pool exhaustion under high load
/// - Aggressive HTTP/2 settings:
///   - 16MB connection window (up from default 64KB)
///   - 8MB stream window (up from default 64KB)
///   - Max frame size 16MB (up from 16KB)
/// - TCP buffer tuning via HttpConnector
/// - Connection limiting via semaphore
/// - Long idle timeout (120s) for connection reuse over long links
#[derive(Debug, Clone)]
pub struct HttpClient {
    inner: HyperClient<LimitedConnector, Full<Bytes>>,
}

/// A response from the client.
#[derive(Debug)]
pub struct Response(hyper::Response<hyper::body::Incoming>);

impl Response {
    pub fn status(&self) -> StatusCode {
        self.0.status()
    }

    pub async fn read(self) -> Result<Bytes, HttpError> {
        Ok(self.0.into_body().collect().await?.to_bytes())
    }
}

impl HttpClient {
    /// Create a new high-performance client with default tuning.
    ///
    /// Default configuration:
    /// - 2048 max idle connections per host
    /// - 120s idle timeout
    /// - 16MB HTTP/2 connection window
    /// - 8MB HTTP/2 stream window
    /// - HTTP/2 keep-alive every 10s with 20s timeout
    /// - TCP_NODELAY enabled
    pub fn new(id: impl Into<String>) -> Self {
        Self::builder(id).build()
    }

    /// Create a new client builder for custom configuration.
    pub fn builder(id: impl Into<String>) -> HttpClientBuilder {
        HttpClientBuilder::new(id)
    }

    /// Get a reference to the underlying hyper client.
    pub fn inner(&self) -> &HyperClient<LimitedConnector, Full<Bytes>> {
        &self.inner
    }

    /// Create a POST request builder (reqwest-compatible API).
    ///
    /// # Example
    /// ```ignore
    /// let response = client.post("https://example.com/api")
    ///     .body(vec![1, 2, 3])
    ///     .headers(headers)
    ///     .send()
    ///     .await?;
    /// ```
    pub async fn post(
        &self,
        url: impl AsRef<str>,
        body: Vec<u8>,
        headers: HeaderMap,
    ) -> Result<Response, HttpError> {
        let mut req = hyper::Request::builder()
            .method(Method::POST)
            .uri(url.as_ref())
            .header(header::CONTENT_TYPE, "application/json");

        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }

        let req = req.body(Full::from(body))?;

        self.inner.request(req).await.map(Response).map_err(HttpError::Client)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new("default_client")
    }
}

/// Builder for configuring a high-performance orderflow client.
#[derive(Debug)]
pub struct HttpClientBuilder {
    /// Maximum idle connections per host (default: 2048)
    pool_max_idle_per_host: usize,
    /// Pool idle timeout (default: 120s)
    pool_idle_timeout: Duration,
    /// Request timeout (default: 2s)
    request_timeout: Duration,
    /// Connect timeout (default: 1s)
    connect_timeout: Duration,
    /// Maximum concurrent connections (default: None, unlimited)
    max_concurrent_connections: Option<usize>,
    /// Identifier for connection limiter metrics (default: "client")
    connection_limiter_id: String,
    /// HTTP/2 connection window size (default: 16MB)
    http2_connection_window_size: u32,
    /// HTTP/2 stream window size (default: 8MB)
    http2_stream_window_size: u32,
    /// HTTP/2 max frame size (default: 16MB)
    http2_max_frame_size: u32,
    /// HTTP/2 keep-alive interval (default: 10s)
    http2_keep_alive_interval: Duration,
    /// HTTP/2 keep-alive timeout (default: 20s)
    http2_keep_alive_timeout: Duration,
    /// Enable TCP_NODELAY (default: true)
    tcp_nodelay: bool,
    /// TCP send buffer size in bytes (default: None, use system default)
    tcp_send_buffer_size: Option<usize>,
    /// TCP receive buffer size in bytes (default: None, use system default)
    tcp_recv_buffer_size: Option<usize>,
    /// TCP keepalive idle duration (default: None, use system default)
    tcp_keepalive: Option<Duration>,
}

impl HttpClientBuilder {
    /// Create a new builder with default high-performance settings.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            pool_max_idle_per_host: 2048,
            pool_idle_timeout: Duration::from_secs(120),
            request_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(1),
            max_concurrent_connections: None,
            connection_limiter_id: id.into(),
            http2_connection_window_size: 16 * 1024 * 1024, // 16MB
            http2_stream_window_size: 8 * 1024 * 1024,      // 8MB
            http2_max_frame_size: 16 * 1024 * 1024,         // 16MB
            http2_keep_alive_interval: Duration::from_secs(10),
            http2_keep_alive_timeout: Duration::from_secs(20),
            tcp_nodelay: true,
            tcp_send_buffer_size: None,
            tcp_recv_buffer_size: None,
            tcp_keepalive: None,
        }
    }

    /// Set maximum idle connections per host.
    ///
    /// Higher values reduce connection establishment overhead but use more memory.
    /// For high-throughput proxies, 2048+ is recommended to avoid pool exhaustion.
    pub fn pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = max;
        self
    }

    /// Set pool idle timeout.
    ///
    /// Connections idle longer than this duration will be closed.
    /// For long-distance links, use 90-120s to maximize connection reuse.
    pub fn pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = timeout;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set maximum concurrent connections.
    ///
    /// This enforces a hard limit on concurrent in-flight requests using a semaphore.
    /// When the limit is reached, new requests will wait until a slot is available.
    ///
    /// Use this to prevent overwhelming the remote endpoint or exhausting local resources.
    /// Set to None (default) for unlimited connections.
    pub fn max_concurrent_connections(mut self, max: usize) -> Self {
        self.max_concurrent_connections = Some(max);
        self
    }

    /// Set HTTP/2 connection window size.
    ///
    /// This is the total amount of data that can be in-flight on the entire connection.
    /// For high BDP (bandwidth-delay product) links, use 8-16MB.
    pub fn http2_connection_window_size(mut self, size: u32) -> Self {
        self.http2_connection_window_size = size;
        self
    }

    /// Set HTTP/2 stream window size.
    ///
    /// This is the amount of data that can be in-flight per stream.
    /// For large payloads over high-latency links, use 4-8MB.
    pub fn http2_stream_window_size(mut self, size: u32) -> Self {
        self.http2_stream_window_size = size;
        self
    }

    /// Set HTTP/2 maximum frame size.
    ///
    /// Larger frames reduce per-frame overhead. Use 1-16MB for bulk transfers.
    pub fn http2_max_frame_size(mut self, size: u32) -> Self {
        self.http2_max_frame_size = size;
        self
    }

    /// Set HTTP/2 keep-alive ping interval.
    ///
    /// Send a PING frame at this interval to keep connections alive.
    pub fn http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.http2_keep_alive_interval = interval;
        self
    }

    /// Set HTTP/2 keep-alive ping timeout.
    ///
    /// Close the connection if a PING is not acknowledged within this timeout.
    pub fn http2_keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.http2_keep_alive_timeout = timeout;
        self
    }

    /// Enable or disable TCP_NODELAY (Nagle's algorithm).
    ///
    /// For low-latency applications, TCP_NODELAY should be enabled (true).
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = enabled;
        self
    }

    /// Set TCP send buffer size (SO_SNDBUF).
    ///
    /// For high BDP links (high bandwidth * high latency), calculate:
    /// `buffer_size = bandwidth (bytes/sec) * RTT (sec)`
    ///
    /// Example: 1Gbps link with 50ms RTT = 125MB/s * 0.05s = 6.25MB
    ///
    /// Use None to let the OS auto-tune (recommended unless you've measured).
    pub fn tcp_send_buffer_size(mut self, size: usize) -> Self {
        self.tcp_send_buffer_size = Some(size);
        self
    }

    /// Set TCP receive buffer size (SO_RCVBUF).
    ///
    /// See [`tcp_send_buffer_size`](Self::tcp_send_buffer_size) for sizing guidance.
    pub fn tcp_recv_buffer_size(mut self, size: usize) -> Self {
        self.tcp_recv_buffer_size = Some(size);
        self
    }

    /// Set TCP keepalive idle duration (SO_KEEPALIVE).
    ///
    /// Send keepalive probes after this duration of inactivity.
    /// Useful for detecting dead connections on long-lived links.
    pub fn tcp_keepalive(mut self, duration: Duration) -> Self {
        self.tcp_keepalive = Some(duration);
        self
    }

    /// Build the configured client.
    pub fn build(self) -> HttpClient {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(self.connect_timeout));

        // Configure TCP socket options
        connector.set_nodelay(self.tcp_nodelay);
        connector.enforce_http(false); // Allow HTTPS (when TLS is added)

        if let Some(size) = self.tcp_send_buffer_size {
            connector.set_send_buffer_size(Some(size));
        }

        if let Some(size) = self.tcp_recv_buffer_size {
            connector.set_recv_buffer_size(Some(size));
        }

        if let Some(duration) = self.tcp_keepalive {
            connector.set_keepalive(Some(duration));
        }

        // Wrap connector with connection limiter
        // Use effectively unlimited (usize::MAX) if no limit specified
        let max_connections = self.max_concurrent_connections.unwrap_or(usize::MAX);
        let connector = ServiceBuilder::new()
            .layer(ConnectionLimiterLayer::new(max_connections, self.connection_limiter_id))
            .service(connector);

        // Build client with connection pool and HTTP/2 settings
        let client = HyperClient::builder(TokioExecutor::new())
            .pool_max_idle_per_host(self.pool_max_idle_per_host)
            .pool_idle_timeout(self.pool_idle_timeout)
            .pool_timer(hyper_util::rt::TokioTimer::new())
            .http2_initial_stream_window_size(self.http2_stream_window_size)
            .http2_initial_connection_window_size(self.http2_connection_window_size)
            .http2_max_frame_size(self.http2_max_frame_size)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .http2_keep_alive_timeout(self.http2_keep_alive_timeout)
            .timer(hyper_util::rt::TokioTimer::new())
            .build(connector);

        HttpClient { inner: client }
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new("default_client")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let builder = HttpClientBuilder::new("test_client");
        assert_eq!(builder.pool_max_idle_per_host, 2048);
        assert_eq!(builder.pool_idle_timeout, Duration::from_secs(120));
        assert_eq!(builder.http2_connection_window_size, 16 * 1024 * 1024);
        assert_eq!(builder.http2_stream_window_size, 8 * 1024 * 1024);
        assert!(builder.tcp_nodelay);
        assert!(builder.tcp_send_buffer_size.is_none());
        assert!(builder.tcp_recv_buffer_size.is_none());
    }

    #[test]
    fn test_builder_customization() {
        let builder = HttpClientBuilder::new("test_client")
            .pool_max_idle_per_host(4096)
            .pool_idle_timeout(Duration::from_secs(60))
            .http2_connection_window_size(32 * 1024 * 1024)
            .tcp_nodelay(false)
            .tcp_send_buffer_size(8 * 1024 * 1024);

        assert_eq!(builder.pool_max_idle_per_host, 4096);
        assert_eq!(builder.pool_idle_timeout, Duration::from_secs(60));
        assert_eq!(builder.http2_connection_window_size, 32 * 1024 * 1024);
        assert!(!builder.tcp_nodelay);
        assert_eq!(builder.tcp_send_buffer_size, Some(8 * 1024 * 1024));
    }

    #[test]
    fn test_client_creation() {
        let _client = HttpClient::default();
    }

    #[test]
    fn test_builder_pattern() {
        let _client = HttpClient::builder("test_client")
            .pool_max_idle_per_host(1024)
            .tcp_send_buffer_size(4 * 1024 * 1024)
            .tcp_recv_buffer_size(4 * 1024 * 1024)
            .tcp_keepalive(Duration::from_secs(30))
            .build();
    }

    #[test]
    fn test_bdp_calculation() {
        // For 1Gbps link with 50ms RTT:
        // BDP = 1,000,000,000 bits/s * 0.05s = 50,000,000 bits = 6,250,000 bytes
        let bandwidth_gbps = 1.0;
        let rtt_ms = 50.0;
        let bdp_bytes = (bandwidth_gbps * 1_000_000_000.0 / 8.0) * (rtt_ms / 1000.0);

        assert_eq!(bdp_bytes as usize, 6_250_000);

        // Our default connection window (16MB) should be ~2.5x BDP
        let default_window = 16 * 1024 * 1024;
        assert!(default_window as f64 > bdp_bytes * 2.0);
    }
}

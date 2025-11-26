//! Configuration for HTTP clients used to spawn forwarders.
use std::{
    net::SocketAddr,
    num::NonZero,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use msg_socket::ReqSocket;
use msg_transport::Transport;

/// The default HTTP timeout in seconds.
pub const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 2;
/// The default connect timeout in milliseconds.
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 800;
/// The default pool idle timeout in seconds.
pub const DEFAULT_POOL_IDLE_TIMEOUT_SECS: u64 = 28;
/// The default HTTP connection limit per host.
pub const DEFAULT_CONNECTION_LIMIT_PER_HOST: usize = 64;

/// Create a default reqwest client builder for forwarders with optimized settings.
///
/// ### Params:
///   * `peer_name`: The name of the peer, used for connection limiting metrics.
///   * `id`: An idenfitier or index that makes the pair (peer_name, id) unique, used for connection
///     limiting metrics.
pub fn default_http_builder() -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS));

    // HTTP/1.x configuration
    builder = builder
        .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
        .pool_max_idle_per_host(DEFAULT_CONNECTION_LIMIT_PER_HOST);

    // HTTP/2 configuration
    builder = builder
        // Enforces HTTP/2 only for creating clients. We probably do NOT want it, since the ALPN
        // (Application Layer Protocol Negotation) during the TLS handshake is used to negotiate
        // the HTTP protocol used, enforcing a single version can break compatiblity between
        // deployments
        // .http2_prior_knowledge()
        // ---------------------------
        // The initial size of a single stream, which corresponds to a single request. It defaults
        // to 64KiB. Bumping it increases memory usage (not a problem), but most importantly it
        // increases the TCP send socket buffer usage of a given stream (remember there is a single
        // connection behind it). However, there are many "big requests" to forward in the proxy
        // above 50KiB, so to be safe we could set it to a value like 128KiB or 256KiB.
        // Note well: this configures window size for HAProxy responses, not requests. So if you
        // want to optimize for requests, you need to configure the peer server (HAProxy).
        // .http2_initial_stream_window_size(HTTP2_INITIAL_STREAM_WINDOW_SIZE)
        // Defines the total number of bytes that can be outstanding (sent but unacknowledged)
        // across all streams in a single HTTP/2 connection. By default (as of RFC 7540),
        // default size is 64KiB. This is too low for any use-case like ours, so we should
        // definitely bump it probably to be a bit higher (1.5x/2x) than BDP for that peer.
        // Given that we don’t discriminate by expected RTT, we have to find a common value
        // for all of them as of now.
        // Note well: this configures window size for HAProxy responses, not requests. So if you
        // want to optimize for requests, you need to configure the peer server (HAProxy).
        // .http2_initial_connection_window_size(HTTP2_INITIAL_CONNECTION_WINDOW_SIZE as u32)
        // Adaptively adjust stream window size based on responses received, to be closer to BDP.
        // It's okay to use it, although with our use-case it won't make a big difference given
        // responses are always small, either small hashes or error messages.
        .http2_adaptive_window(true)
        // Sets the maximum number of bytes allowed in the payload of a frame. Defaults to 16,384
        // bytes, HAProxy doesn’t recommend to change it and probably we should not do it even here.
        .http2_max_frame_size(16_384)
        // Related to header size, we can leave the default of 16KiB.
        .http2_max_header_list_size(16_384)
        // Interval to send ping frames to keep the HTTP connection alive. By default it is
        // disabled, however we should a message to a peer multiple times per second so the
        // connection should never become idle. We can still set it to something like 10s if
        // needed.
        .http2_keep_alive_interval(Duration::from_secs(10))
        // By default disabled, we don’t want to close connections so we can avoid it.
        // .http2_keep_alive_timeout(timeout)
        // ----------------------------
        // Sets whether HTTP2 keep-alive should apply while the connection is idle. Again we could
        // enable it, but the connection should never become idle with HTTP/2.
        .http2_keep_alive_while_idle(true);
    builder
}

pub type ReqSocketIp<T> = ReqSocket<T, SocketAddr>;

/// A pool of TCP [`ReqSocket`] clients, with TLS support.
#[allow(missing_debug_implementations)]
pub struct ReqSocketIpPool<T: Transport<SocketAddr>> {
    /// The clients in the pool.
    clients: Arc<[ReqSocketIp<T>]>,
    /// The number of clients in the pool, so you don't have to deference the arc every time.
    num_clients: usize,
    /// The index of the last used client. Used for round-robin load balancing.
    last_used: Arc<AtomicU8>,
}

// Custom [`Clone`] implementation to avoid requiring T: Clone.
impl<T: Transport<SocketAddr>> Clone for ReqSocketIpPool<T> {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
            num_clients: self.num_clients,
            last_used: self.last_used.clone(),
        }
    }
}

impl<T: Transport<SocketAddr>> ReqSocketIpPool<T> {
    pub fn new(sockets: Vec<ReqSocketIp<T>>) -> Self {
        let num_clients = sockets.len();
        let clients = Arc::from(sockets);
        Self { clients, num_clients, last_used: Arc::new(AtomicU8::new(0)) }
    }

    pub fn socket(&self) -> &ReqSocketIp<T> {
        // NOTE: This will automatically wrap.
        let index = self.last_used.fetch_add(1, Ordering::Relaxed);
        &self.clients[(index as usize) % self.num_clients]
    }
}

/// A pool of HTTP clients for load balancing. Works with round-robin selection.
#[derive(Debug, Clone)]
pub struct HttpClientPool {
    /// The clients in the pool.
    clients: Arc<[reqwest::Client]>,
    /// The number of clients in the pool, so you don't have to deference the arc every time.
    num_clients: usize,
    /// The index of the last used client. Used for round-robin load balancing.
    last_used: Arc<AtomicU8>,
}

impl HttpClientPool {
    /// Create a new client pool with `num_clients` clients, created by the `make_client` function.
    pub fn new(num_clients: NonZero<usize>, make_client: impl Fn() -> reqwest::Client) -> Self {
        let clients = (0..num_clients.get()).map(|_| make_client()).collect();
        Self { clients, num_clients: num_clients.get(), last_used: Arc::new(AtomicU8::new(0)) }
    }

    /// Get a client from the pool.
    pub fn client(&self) -> &reqwest::Client {
        // NOTE: This will automatically wrap.
        let index = self.last_used.fetch_add(1, Ordering::Relaxed);
        &self.clients[(index as usize) % self.num_clients]
    }
}

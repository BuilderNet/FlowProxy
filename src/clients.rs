//! Configuration for HTTP clients used to spawn forwarders.

use std::time::Duration;

use crate::utils;

/// The default HTTP timeout in seconds.
pub const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 2;

/// The default connect timeout in milliseconds.
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 800;

/// The default pool idle timeout in seconds.
pub const DEFAULT_POOL_IDLE_TIMEOUT_SECS: u64 = 28;

/// The default HTTP connection limit per host.
pub const DEFAULT_CONNECTION_LIMIT_PER_HOST: usize = 512;

const GIGABIT: u64 = 1024 * 1024 * 1024;
// Should be customized per peer RTT
const EXPECTED_RTT_MS: u64 = 80;
const EXPECTED_BDP: u64 = (GIGABIT / 8 * EXPECTED_RTT_MS) / 1000; // in bytes

const HTTP2_INITIAL_STREAM_WINDOW_SIZE: u32 = 256 * 1024 * 1024; // 256 KB

const HTTP2_INITIAL_CONNECTION_WINDOW_SIZE: u64 = EXPECTED_BDP * 3 / 2; // 1.5x BDP

/// Create a default reqwest client builder for forwarders with optimized settings.
pub fn default_forwarder_client() -> reqwest::ClientBuilder {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
        .connector_layer(utils::limit::ConnectionLimiterLayer::new(
            DEFAULT_CONNECTION_LIMIT_PER_HOST,
            "local-builder".to_string(),
        ));
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
        //The initial size of a single stream, which corresponds to a single request. It defaults
        //to 64KiB. Bumping it increases memory usage (not a problem), but most importantly it
        //increases the TCP send socket buffer usage of a given stream (remember there is a single
        //connection behind it). However, there are many “big requests” to forward in the proxy
        //above 50KiB, so to be safe we could set it to a value like 128KiB or 256KiB.
        .http2_initial_stream_window_size(HTTP2_INITIAL_STREAM_WINDOW_SIZE)
        // Defines the total number of bytes that can be outstanding (sent but unacknowledged)
        // across all streams in a single HTTP/2 connection. By default (as of RFC 7540),
        // default size is 64KiB. This is too low for any use-case like ours, so we should
        // definitely bump it probably to be a bit higher (1.5x/2x) than BDP for that peer.
        // Given that we don’t discriminate by expected RTT, we have to find a common value
        // for all of them as of now.
        .http2_initial_connection_window_size(HTTP2_INITIAL_CONNECTION_WINDOW_SIZE as u32)
        // Adaptively adjust stream window size based on responses received, to be closer to BDP. I
        // don’t know if we need it, we should look at the reqwest implementation what it
        // does exactly to avoid weird surprises.
        .http2_adaptive_window(false)
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

use std::time::{Duration, Instant};

use metrics::{counter, gauge, histogram};

use crate::{forwarder::ForwardingDirection, priority::Priority};

#[derive(Debug, Clone)]
pub struct ForwarderMetrics;

impl ForwarderMetrics {
    /// The duration of the RPC call to a peer.
    #[inline]
    pub fn record_rpc_call(url: String, duration: Duration, big_request: bool) {
        histogram!("forwarder_rpc_call_duration_s", "url" => url, "big_request" => big_request.to_string()).record(duration.as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub struct PriorityQueueMetrics;

impl PriorityQueueMetrics {
    #[inline]
    pub fn set_queue_size(size: usize, priority: &'static str) {
        gauge!("priority_queue_size", "priority" => priority).set(size as f64);
    }
}

pub trait IngressHandlerMetricsExt {
    const HANDLER: &str;

    // MISC

    #[inline]
    fn increment_requests_rate_limited() {
        counter!("ingress_requests_rate_limited", "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_parse_errors() {
        counter!("ingress_json_rpc_parse_errors", "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_unknown_method() {
        counter!("ingress_json_rpc_unknown_method", "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_order_cache_hit() {
        counter!("ingress_order_cache_hit", "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn record_http_request(method: String, path: String, status: String, duration: Duration) {
        let labels = [
            ("handler", Self::HANDLER.to_string()),
            ("method", method),
            ("path", path),
            ("status", status),
        ];

        histogram!("ingress_http_request_duration_s", &labels).record(duration.as_secs_f64());
    }

    // BUNDLES

    #[inline]
    fn increment_bundles_received(priority: Priority) {
        counter!("ingress_send_bundle_requests_total", "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    #[inline]
    fn increment_mev_share_bundles_received(priority: Priority) {
        counter!("ingress_send_mev_share_bundle_requests_total", "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    /// The duration of the `eth_sendBundle` RPC call.
    #[inline]
    fn record_bundle_rpc_duration(duration: Duration) {
        histogram!("ingress_send_bundle_request_duration_s", "handler" => Self::HANDLER)
            .record(duration.as_secs_f64());
    }

    // TRANSACTIONS

    #[inline]
    fn increment_transactions_received(priority: Priority) {
        counter!("ingress_send_transaction_requests_total", "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    /// The duration of the `eth_sendRawTransaction` RPC call.
    #[inline]
    fn record_transaction_rpc_duration(duration: Duration) {
        histogram!("ingress_send_transaction_request_duration_s", "handler" => Self::HANDLER)
            .record(duration.as_secs_f64());
    }
}

#[derive(Clone, Debug)]
pub struct IngressUserMetrics;

impl IngressHandlerMetricsExt for IngressUserMetrics {
    const HANDLER: &'static str = "user";
}

#[derive(Clone, Debug)]
pub struct IngressSystemMetrics;

impl IngressHandlerMetricsExt for IngressSystemMetrics {
    const HANDLER: &'static str = "system";
}

/// Metrics related to the whole system.
#[derive(Debug, Clone)]
pub struct SystemMetrics;

#[allow(missing_debug_implementations)]
impl SystemMetrics {
    #[inline]
    pub fn entity_count(count: usize) {
        gauge!("ingress_entity_count").set(count as f64);
    }

    #[inline]
    pub fn record_e2e_bundle_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let labels = [
            ("priority", priority.to_string()),
            ("direction", direction.to_string()),
            ("big_request", big_request.to_string()),
        ];
        histogram!("ingress_e2e_bundle_processing_time_s", &labels).record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_e2e_mev_share_bundle_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let labels = [
            ("priority", priority.to_string()),
            ("direction", direction.to_string()),
            ("big_request", big_request.to_string()),
        ];
        histogram!("ingress_e2e_mev_share_bundle_processing_time_s", &labels)
            .record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_e2e_transaction_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let labels = [
            ("priority", priority.to_string()),
            ("direction", direction.to_string()),
            ("big_request", big_request.to_string()),
        ];
        histogram!("ingress_e2e_transaction_processing_time_s", &labels)
            .record(duration.as_secs_f64());
    }

    pub fn record_e2e_raw_order_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let labels = [
            ("priority", priority.to_string()),
            ("direction", direction.to_string()),
            ("big_request", big_request.to_string()),
        ];
        histogram!("ingress_e2e_raw_order_processing_time_s", &labels)
            .record(duration.as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub struct BuilderHubMetrics;

#[allow(missing_debug_implementations)]
impl BuilderHubMetrics {
    #[inline]
    pub fn increment_builderhub_peer_request_failures(err: String) {
        counter!("builderhub_peer_request_failures", "error" => err).increment(1);
    }

    #[inline]
    pub fn builderhub_peer_count(count: usize) {
        gauge!("builderhub_peer_count").set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct IndexerMetrics;

#[allow(missing_debug_implementations)]
impl IndexerMetrics {
    #[inline]
    pub fn increment_bundle_indexing_failures(err: String) {
        counter!("indexer_bundle_indexing_failures", "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_bundle_receipt_indexing_failures(err: String) {
        counter!("indexer_bundle_receipt_indexing_failures", "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_write_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_commit_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }

    #[inline]
    pub fn set_clickhouse_queue_size(size: usize, order: &'static str) {
        gauge!("indexer_clickhouse_queue_size", "order" => order).set(size as f64);
    }

    #[inline]
    pub fn set_parquet_queue_size(size: usize, order: &'static str) {
        gauge!("indexer_parquet_queue_size", "order" => order).set(size as f64);
    }
}

/// A simple sampler that executes a closure every `sample_size` calls, or if a certain amount of
/// time has passed since last sampling call.
#[derive(Debug, Clone)]
pub struct Sampler {
    sample_size: usize,
    counter: usize,
    start: Instant,
    interval: Duration,
}

impl Default for Sampler {
    fn default() -> Self {
        Self {
            sample_size: 4096,
            counter: 0,
            start: Instant::now(),
            interval: Duration::from_secs(10),
        }
    }
}

impl Sampler {
    pub fn with_sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.start = Instant::now() - interval;
        self
    }

    /// Call this function to potentially execute the sample closure if we have reached the sample
    /// size, or enough time has passed. Otherwise, it increments the internal counter.
    pub fn sample(&mut self, f: impl FnOnce()) {
        if self.counter >= self.sample_size || self.start.elapsed() >= self.interval {
            self.counter = 0;
            self.start = Instant::now();
            f();
        } else {
            self.counter += 1;
        }
    }
}

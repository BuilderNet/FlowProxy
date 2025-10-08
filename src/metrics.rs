use std::time::{Duration, Instant};

use metrics::{counter, gauge, histogram};

use crate::{
    consts::{ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD},
    forwarder::ForwardingType,
    priority::Priority,
};

#[derive(Debug, Clone)]
pub struct ForwarderMetrics;

impl ForwarderMetrics {
    pub fn record_rpc_call(elapsed: Duration, url: String, big_request: bool) {
        let labels = [("url", url), ("big_request", big_request.to_string())];
        histogram!("forwarder_rpc_call_duration_milliseconds", &labels)
            .record(elapsed.as_millis() as f64);
    }
}

#[derive(Debug, Clone)]
pub struct PriorityQueueMetrics;

impl PriorityQueueMetrics {
    pub fn set_queue_size(size: usize, priority: &'static str) {
        gauge!("priority_queue_size", "priority" => priority).set(size as f64);
    }
}

pub trait IngressHandlerMetricsExt {
    const HANDLER: &str;

    // MISC

    fn increment_requests_received() {
        counter!("ingress_requests_received", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_requests_rate_limited() {
        counter!("ingress_requests_rate_limited", "handler" => Self::HANDLER).increment(1);
    }
    fn increment_json_rpc_parse_errors() {
        counter!("ingress_json_rpc_parse_errors", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_json_rpc_unknown_method() {
        counter!("ingress_json_rpc_unknown_method", "handler" => Self::HANDLER).increment(1);
    }

    fn record_processed_in(duration: Duration) {
        histogram!("ingress_processed_in_seconds", "handler" => Self::HANDLER)
            .record(duration.as_millis() as f64);
    }

    fn observe_raw_request(method: String, path: String, status: String, duration: Duration) {
        let labels = [
            ("handler", Self::HANDLER.to_string()),
            ("method", method),
            ("path", path),
            ("status", status),
        ];
        counter!("ingress_requests_total", &labels).increment(1);
        histogram!("ingress_request_duration_seconds", &labels).record(duration.as_millis() as f64);
    }

    fn record_method_metrics(method: &str, received_at: Instant) {
        let elapsed = received_at.elapsed();
        Self::record_processed_in(elapsed);
        match method {
            ETH_SEND_BUNDLE_METHOD => {
                Self::record_bundle_processed_in(elapsed);
                Self::increment_bundles_received();
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                Self::record_transaction_processed_in(elapsed);
                Self::increment_transactions_received();
            }
            _ => {
                Self::increment_json_rpc_unknown_method();
            }
        };
    }

    // BUNDLES

    fn increment_bundles_received() {
        counter!("ingress_bundles_received", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_bundles_with_priority_received(priority: Priority) {
        let labels = [("handler", Self::HANDLER.to_string()), ("priority", priority.to_string())];
        counter!("ingress_bundles_with_priority_received", &labels).increment(1);
    }

    fn record_bundle_processed_in(duration: Duration) {
        histogram!("ingress_bundle_processed_in_seconds", "handler" => Self::HANDLER)
            .record(duration.as_millis() as f64);
    }

    // TRANSACTIONS

    fn increment_transactions_received() {
        counter!("ingress_transactions_received", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_transactions_with_priority_received(priority: Priority) {
        let labels = [("handler", Self::HANDLER.to_string()), ("priority", priority.to_string())];
        counter!("ingress_transactions_with_priority_received", &labels).increment(1);
    }

    fn record_transaction_processed_in(duration: Duration) {
        histogram!("ingress_transaction_processed_in_seconds", "handler" => Self::HANDLER)
            .record(duration.as_millis() as f64);
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

#[derive(Debug, Clone)]
pub struct IngressMetrics;

#[allow(missing_debug_implementations)]
impl IngressMetrics {
    pub fn entity_count(count: usize) {
        gauge!("ingress_entity_count").set(count as f64);
    }

    pub fn record_e2e_order_processing_time(
        duration: Duration,
        priority: Priority,
        forward_type: ForwardingType,
    ) {
        let labels =
            [("priority", priority.to_string()), ("forward_type", forward_type.to_string())];
        histogram!("ingress_e2e_order_processing_time_seconds", &labels)
            .record(duration.as_millis() as f64);
    }
}

#[derive(Debug, Clone)]
pub struct BuilderHubMetrics;

#[allow(missing_debug_implementations)]
impl BuilderHubMetrics {
    pub fn increment_builderhub_peer_request_failures(err: String) {
        counter!("builderhub_peer_request_failures", "error" => err).increment(1);
    }

    pub fn builderhub_peer_count(count: usize) {
        gauge!("builderhub_peer_count").set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct IndexerMetrics;

#[allow(missing_debug_implementations)]
impl IndexerMetrics {
    pub fn increment_bundle_indexing_failures(err: String) {
        counter!("indexer_bundle_indexing_failures", "error" => err).increment(1);
    }

    pub fn increment_bundle_receipt_indexing_failures(err: String) {
        counter!("indexer_bundle_receipt_indexing_failures", "error" => err).increment(1);
    }

    pub fn increment_clickhouse_write_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }

    pub fn increment_clickhouse_commit_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }

    pub fn set_clickhouse_queue_size(size: usize, order: &'static str) {
        gauge!("indexer_clickhouse_queue_size", "order" => order).set(size as f64);
    }

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

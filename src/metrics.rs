use std::time::{Duration, Instant};

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::{forwarder::ForwardingDirection, priority::Priority};

#[rustfmt::skip]
mod name {
    pub(crate) const BUILDERHUB_PEER_COUNT: &str = "builderhub_peer_count";
    pub(crate) const BUILDERHUB_PEER_REQUEST_FAILURES: &str = "builderhub_peer_request_failures";
    pub(crate) const FORWARDER_HTTP_CALL_FAILURES: &str = "forwarder_http_call_failures";
    pub(crate) const FORWARDER_JSON_RPC_DECODING_FAILURES: &str = "forwarder_json_rpc_decoding_failures";
    pub(crate) const FORWARDER_REQUEST_PROCESSING_FAILURES: &str = "forwarder_request_processing_failures";
    pub(crate) const FORWARDER_RPC_CALL_DURATION_S: &str = "forwarder_rpc_call_duration_s";
    pub(crate) const FORWARDER_RPC_CALL_FAILURES: &str = "forwarder_rpc_call_failures";
    pub(crate) const INDEXER_BUNDLE_INDEXING_FAILURES: &str = "indexer_bundle_indexing_failures";
    pub(crate) const INDEXER_BUNDLE_RECEIPT_INDEXING_FAILURES: &str = "indexer_bundle_receipt_indexing_failures";
    pub(crate) const INDEXER_CLICKHOUSE_COMMIT_FAILURES: &str = "indexer_clickhouse_commit_failures";
    pub(crate) const INDEXER_CLICKHOUSE_QUEUE_SIZE: &str = "indexer_clickhouse_queue_size";
    pub(crate) const INDEXER_CLICKHOUSE_WRITE_FAILURES: &str = "indexer_clickhouse_write_failures";
    pub(crate) const INDEXER_PARQUET_QUEUE_SIZE: &str = "indexer_parquet_queue_size";
    pub(crate) const INGRESS_E2E_BUNDLE_PROCESSING_TIME_S: &str = "ingress_e2e_bundle_processing_time_s";
    pub(crate) const INGRESS_E2E_MEV_SHARE_BUNDLE_PROCESSING_TIME_S: &str = "ingress_e2e_mev_share_bundle_processing_time_s";
    pub(crate) const INGRESS_E2E_RAW_ORDER_PROCESSING_TIME_S: &str = "ingress_e2e_raw_order_processing_time_s";
    pub(crate) const INGRESS_E2E_TRANSACTION_PROCESSING_TIME_S: &str = "ingress_e2e_transaction_processing_time_s";
    pub(crate) const INGRESS_ENTITY_COUNT: &str = "ingress_entity_count";
    pub(crate) const INGRESS_HTTP_REQUEST_DURATION_S: &str = "ingress_http_request_duration_s";
    pub(crate) const INGRESS_JSON_RPC_PARSE_ERRORS: &str = "ingress_json_rpc_parse_errors";
    pub(crate) const INGRESS_JSON_RPC_UNKNOWN_METHOD: &str = "ingress_json_rpc_unknown_method";
    pub(crate) const INGRESS_ORDER_CACHE_HIT: &str = "ingress_order_cache_hit";
    pub(crate) const INGRESS_REQUESTS_RATE_LIMITED: &str = "ingress_requests_rate_limited";
    pub(crate) const INGRESS_SEND_BUNDLE_REQUESTS_TOTAL: &str = "ingress_send_bundle_requests_total";
    pub(crate) const INGRESS_SEND_BUNDLE_REQUEST_DURATION_S: &str = "ingress_send_bundle_request_duration_s";
    pub(crate) const INGRESS_SEND_MEV_SHARE_BUNDLE_REQUESTS_TOTAL: &str = "ingress_send_mev_share_bundle_requests_total";
    pub(crate) const INGRESS_SEND_TRANSACTION_REQUESTS_TOTAL: &str = "ingress_send_transaction_requests_total";
    pub(crate) const INGRESS_SEND_TRANSACTION_REQUEST_DURATION_S: &str = "ingress_send_transaction_request_duration_s";
    pub(crate) const INGRESS_VALIDATION_ERRORS: &str = "ingress_validation_errors";
    pub(crate) const PRIORITY_QUEUE_SIZE: &str = "priority_queue_size";
}

use name::*;

pub struct MetricsDescriber;

impl MetricsDescriber {
    pub fn describe() {
        // Forwarder metrics
        describe_histogram!(
            "forwarder_rpc_call_duration_s",
            "The duration of the RPC call to a peer."
        );
        describe_counter!(
            "forwarder_request_processing_failures",
            "The number of request processing failures."
        );
        describe_counter!(FORWARDER_HTTP_CALL_FAILURES, "The number of HTTP call failures.");
        describe_counter!(FORWARDER_RPC_CALL_FAILURES, "The number of RPC call failures.");
        describe_counter!(
            "forwarder_json_rpc_decoding_failures",
            "The number of JSON-RPC decoding failures."
        );

        // Priority queue metrics
        describe_gauge!(PRIORITY_QUEUE_SIZE, "The size of the priority queue.");

        // Ingress handler metrics
        describe_counter!(
            "ingress_requests_rate_limited",
            "The number of requests that were rate limited."
        );
        describe_counter!(INGRESS_JSON_RPC_PARSE_ERRORS, "The number of JSON-RPC parse errors.");
        describe_counter!(
            "ingress_json_rpc_unknown_method",
            "The number of JSON-RPC unknown method errors."
        );
        describe_counter!(INGRESS_VALIDATION_ERRORS, "The number of validation errors.");
        describe_counter!(INGRESS_ORDER_CACHE_HIT, "The number of order cache hits.");
        describe_histogram!(INGRESS_HTTP_REQUEST_DURATION_S, "The duration of HTTP requests.");
        describe_counter!(
            "ingress_send_bundle_requests_total",
            "The total number of eth_sendBundle requests received."
        );
        describe_histogram!(
            "ingress_send_bundle_request_duration_s",
            "The duration of eth_sendBundle RPC calls."
        );
        describe_counter!(
            "ingress_send_mev_share_bundle_requests_total",
            "The total number of mev_share bundle requests received."
        );
        describe_histogram!(
            "ingress_send_mev_share_bundle_request_duration_s",
            "The duration of mev_share bundle RPC calls."
        );
        describe_counter!(
            "ingress_send_transaction_requests_total",
            "The total number of eth_sendRawTransaction requests received."
        );
        describe_histogram!(
            "ingress_send_transaction_request_duration_s",
            "The duration of eth_sendRawTransaction RPC calls."
        );
    }
}

#[derive(Debug, Clone)]
pub struct ForwarderMetrics;

impl ForwarderMetrics {
    /// The duration of the RPC call to a peer.
    #[inline]
    pub fn record_rpc_call(url: String, duration: Duration, big_request: bool) {
        histogram!(FORWARDER_RPC_CALL_DURATION_S, "url" => url, "big_request" => big_request.to_string()).record(duration.as_secs_f64());
    }

    pub fn increment_request_processing_failures(peer_name: String) {
        counter!(FORWARDER_REQUEST_PROCESSING_FAILURES, "peer_name" => peer_name).increment(1);
    }

    pub fn increment_http_call_failures(peer_name: String, status_code: String) {
        counter!(FORWARDER_HTTP_CALL_FAILURES, "peer_name" => peer_name, "status_code" => status_code).increment(1);
    }

    pub fn increment_rpc_call_failures(peer_name: String, rpc_code: i32) {
        counter!(FORWARDER_RPC_CALL_FAILURES, "peer_name" => peer_name, "rpc_code" => rpc_code.to_string()).increment(1);
    }

    pub fn increment_json_rpc_decoding_failures(peer_name: String) {
        counter!(FORWARDER_JSON_RPC_DECODING_FAILURES, "peer_name" => peer_name).increment(1);
    }
}

#[derive(Debug, Clone)]
pub struct PriorityQueueMetrics;

impl PriorityQueueMetrics {
    #[inline]
    pub fn set_queue_size(size: usize, priority: &'static str) {
        gauge!(PRIORITY_QUEUE_SIZE, "priority" => priority).set(size as f64);
    }
}

pub trait IngressHandlerMetricsExt {
    const HANDLER: &str;

    // MISC

    #[inline]
    fn increment_requests_rate_limited() {
        counter!(INGRESS_REQUESTS_RATE_LIMITED, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_parse_errors() {
        counter!(INGRESS_JSON_RPC_PARSE_ERRORS, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_unknown_method() {
        counter!(INGRESS_JSON_RPC_UNKNOWN_METHOD, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_validation_errors<E: std::error::Error>(error: &E) {
        counter!(INGRESS_VALIDATION_ERRORS, "handler" => Self::HANDLER, "error" => error.to_string()).increment(1);
    }

    #[inline]
    fn increment_order_cache_hit() {
        counter!(INGRESS_ORDER_CACHE_HIT, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn record_http_request(method: String, path: String, status: String, duration: Duration) {
        let labels = [
            ("handler", Self::HANDLER.to_string()),
            ("method", method),
            ("path", path),
            ("status", status),
        ];

        histogram!(INGRESS_HTTP_REQUEST_DURATION_S, &labels).record(duration.as_secs_f64());
    }

    // BUNDLES

    #[inline]
    fn increment_bundles_received(priority: Priority) {
        counter!(INGRESS_SEND_BUNDLE_REQUESTS_TOTAL, "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    #[inline]
    fn increment_mev_share_bundles_received(priority: Priority) {
        counter!(INGRESS_SEND_MEV_SHARE_BUNDLE_REQUESTS_TOTAL, "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    /// The duration of the `eth_sendBundle` RPC call.
    #[inline]
    fn record_bundle_rpc_duration(duration: Duration) {
        histogram!(INGRESS_SEND_BUNDLE_REQUEST_DURATION_S, "handler" => Self::HANDLER)
            .record(duration.as_secs_f64());
    }

    // TRANSACTIONS

    #[inline]
    fn increment_transactions_received(priority: Priority) {
        counter!(INGRESS_SEND_TRANSACTION_REQUESTS_TOTAL, "handler" => Self::HANDLER, "priority" => priority.to_string()).increment(1);
    }

    /// The duration of the `eth_sendRawTransaction` RPC call.
    #[inline]
    fn record_transaction_rpc_duration(duration: Duration) {
        histogram!(INGRESS_SEND_TRANSACTION_REQUEST_DURATION_S, "handler" => Self::HANDLER)
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
        gauge!(INGRESS_ENTITY_COUNT).set(count as f64);
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
        histogram!(INGRESS_E2E_BUNDLE_PROCESSING_TIME_S, &labels).record(duration.as_secs_f64());
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
        histogram!(INGRESS_E2E_MEV_SHARE_BUNDLE_PROCESSING_TIME_S, &labels)
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
        histogram!(INGRESS_E2E_TRANSACTION_PROCESSING_TIME_S, &labels)
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
        histogram!(INGRESS_E2E_RAW_ORDER_PROCESSING_TIME_S, &labels).record(duration.as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub struct BuilderHubMetrics;

#[allow(missing_debug_implementations)]
impl BuilderHubMetrics {
    #[inline]
    pub fn increment_builderhub_peer_request_failures(err: String) {
        counter!(BUILDERHUB_PEER_REQUEST_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn builderhub_peer_count(count: usize) {
        gauge!(BUILDERHUB_PEER_COUNT).set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct IndexerMetrics;

#[allow(missing_debug_implementations)]
impl IndexerMetrics {
    #[inline]
    pub fn increment_bundle_indexing_failures(err: String) {
        counter!(INDEXER_BUNDLE_INDEXING_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_bundle_receipt_indexing_failures(err: String) {
        counter!(INDEXER_BUNDLE_RECEIPT_INDEXING_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_write_failures(err: String) {
        counter!(INDEXER_CLICKHOUSE_WRITE_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_commit_failures(err: String) {
        counter!(INDEXER_CLICKHOUSE_COMMIT_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn set_clickhouse_queue_size(size: usize, order: &'static str) {
        gauge!(INDEXER_CLICKHOUSE_QUEUE_SIZE, "order" => order).set(size as f64);
    }

    #[inline]
    pub fn set_parquet_queue_size(size: usize, order: &'static str) {
        gauge!(INDEXER_PARQUET_QUEUE_SIZE, "order" => order).set(size as f64);
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

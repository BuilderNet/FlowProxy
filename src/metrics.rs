//! Metrics for the system. We don't use `metrics_derive` here because it doesn't allow for dynamic
//! label
use std::time::Duration;

use clickhouse::inserter::Quantities;
use hyper::{Method, StatusCode};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::{forwarder::ForwardingDirection, priority::Priority};

mod name {
    /// BuilderHub metrics.
    pub(crate) mod builderhub {
        pub(crate) const PEER_COUNT: &str = "builderhub_peer_count";
        pub(crate) const REGISTRATION_FAILURES: &str = "builderhub_registration_failures";
        pub(crate) const PEER_REQUEST_FAILURES: &str = "builderhub_peer_request_failures";
    }

    /// Forwarder metrics.
    pub(crate) mod forwarder {
        pub(crate) const HTTP_CALL_FAILURES: &str = "forwarder_http_call_failures";
        pub(crate) const INFLIGHT_REQUESTS: &str = "forwarder_inflight_http_calls";
        pub(crate) const JSON_RPC_DECODING_FAILURES: &str = "forwarder_rpc_decoding_failures";
        pub(crate) const RPC_CALL_DURATION: &str = "forwarder_rpc_call_duration";
        pub(crate) const RPC_CALL_FAILURES: &str = "forwarder_rpc_call_failures";
    }

    /// Indexer metrics.
    pub(crate) mod indexer {
        pub(crate) const BUNDLE_INDEXING_FAILURES: &str = "indexer_bundle_indexing_failures";
        pub(crate) const BUNDLE_RECEIPT_INDEXING_FAILURES: &str =
            "indexer_bundle_receipt_indexing_failures";

        pub(crate) const CLICKHOUSE_COMMIT_FAILURES: &str = "indexer_clickhouse_commit_failures";
        pub(crate) const CLICKHOUSE_QUEUE_SIZE: &str = "indexer_clickhouse_queue_size";
        pub(crate) const CLICKHOUSE_WRITE_FAILURES: &str = "indexer_clickhouse_write_failures";
        pub(crate) const CLICKHOUSE_ROWS_COMMITTED: &str = "indexer_clickhouse_rows_committed";
        pub(crate) const CLICKHOUSE_BYTES_COMMITTED: &str = "indexer_clickhouse_bytes_committed";
        pub(crate) const CLICKHOUSE_BATCHES_COMMITTED: &str =
            "indexer_clickhouse_batches_committed";

        pub(crate) const PARQUET_QUEUE_SIZE: &str = "indexer_parquet_queue_size";
    }

    /// Ingress metrics.
    pub(crate) mod ingress {
        pub(crate) const ENTITY_COUNT: &str = "ingress_entity_count";
        pub(crate) const HTTP_REQUEST_DURATION: &str = "ingress_http_request_duration";
        pub(crate) const JSON_RPC_PARSE_ERRORS: &str = "ingress_json_rpc_parse_errors";
        pub(crate) const JSON_RPC_UNKNOWN_METHOD: &str = "ingress_json_rpc_unknown_method";
        pub(crate) const ORDER_CACHE_HIT: &str = "ingress_order_cache_hit";
        pub(crate) const REQUESTS_RATE_LIMITED: &str = "ingress_requests_rate_limited";
        pub(crate) const SEND_BUNDLE_REQUEST_DURATION: &str =
            "ingress_eth_sendBundle_request_duration";
        pub(crate) const SEND_TRANSACTION_REQUEST_DURATION: &str =
            "ingress_eth_sendRawTransaction_request_duration";
        pub(crate) const SEND_MEV_SHARE_BUNDLE_REQUEST_DURATION: &str =
            "ingress_mev_sendBundle_request_duration";
        pub(crate) const VALIDATION_ERRORS: &str = "ingress_validation_errors";

        pub(crate) const TXS_PER_BUNDLE: &str = "ingress_txs_per_bundle";
        pub(crate) const TXS_PER_MEV_SHARE_BUNDLE: &str = "ingress_txs_per_mev_share_bundle";
        pub(crate) const TOTAL_EMPTY_BUNDLES: &str = "ingress_total_empty_bundles";
    }

    /// System processing metrics.
    pub(crate) mod system {
        pub(crate) const E2E_BUNDLE_PROCESSING_TIME: &str = "system_e2e_bundle_processing_time";
        pub(crate) const E2E_MEV_SHARE_BUNDLE_PROCESSING_TIME: &str =
            "system_e2e_mev_share_bundle_processing_time";
        pub(crate) const E2E_TRANSACTION_PROCESSING_TIME: &str =
            "system_e2e_transaction_processing_time";
        pub(crate) const E2E_SYSTEM_ORDER_PROCESSING_TIME: &str =
            "system_e2e_system_order_processing_time";
    }
}

use name::*;

pub fn describe() {
    // BuilderHub metrics
    describe_gauge!(builderhub::PEER_COUNT, "Number of active BuilderHub peers");
    describe_counter!(
        builderhub::PEER_REQUEST_FAILURES,
        "Total number of failed get peer requests to BuilderHub"
    );
    describe_counter!(
        builderhub::REGISTRATION_FAILURES,
        "Total number of failed BuilderHub registrations"
    );

    // Forwarder metrics
    describe_counter!(forwarder::HTTP_CALL_FAILURES, "Total number of failed HTTP calls to peers");
    describe_counter!(
        forwarder::JSON_RPC_DECODING_FAILURES,
        "Total number of JSON-RPC response decoding failures"
    );
    describe_histogram!(forwarder::RPC_CALL_DURATION, "Duration of RPC calls to peers in seconds");
    describe_counter!(forwarder::RPC_CALL_FAILURES, "Total number of failed RPC calls to peers");
    describe_gauge!(forwarder::INFLIGHT_REQUESTS, "Number of inflight HTTP requests to peers");

    // Indexer metrics
    describe_counter!(
        indexer::BUNDLE_INDEXING_FAILURES,
        "Total number of bundle indexing failures"
    );
    describe_counter!(
        indexer::BUNDLE_RECEIPT_INDEXING_FAILURES,
        "Total number of bundle receipt indexing failures"
    );
    describe_counter!(
        indexer::CLICKHOUSE_COMMIT_FAILURES,
        "Total number of ClickHouse commit failures"
    );
    describe_gauge!(indexer::CLICKHOUSE_QUEUE_SIZE, "Current size of ClickHouse write queue");
    describe_counter!(
        indexer::CLICKHOUSE_WRITE_FAILURES,
        "Total number of ClickHouse write failures (not the same as commit failures)"
    );
    describe_counter!(
        indexer::CLICKHOUSE_ROWS_COMMITTED,
        "Total number of rows committed to ClickHouse"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BYTES_COMMITTED,
        "Total number of bytes committed to ClickHouse"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BATCHES_COMMITTED,
        "Total number of batches committed to ClickHouse"
    );
    describe_gauge!(indexer::PARQUET_QUEUE_SIZE, "Current size of Parquet write queue");

    // Ingress metrics
    describe_gauge!(ingress::ENTITY_COUNT, "Current number of tracked entities");
    describe_histogram!(
        ingress::HTTP_REQUEST_DURATION,
        "Duration of incoming HTTP request handling in seconds"
    );
    describe_counter!(ingress::JSON_RPC_PARSE_ERRORS, "Total number of JSON-RPC parsing errors");
    describe_counter!(
        ingress::JSON_RPC_UNKNOWN_METHOD,
        "Total number of incoming unknown JSON-RPC method calls"
    );
    describe_counter!(ingress::ORDER_CACHE_HIT, "Total number of order cache hits");
    describe_counter!(
        ingress::REQUESTS_RATE_LIMITED,
        "Total number of incoming rate-limited requests"
    );
    describe_histogram!(
        ingress::SEND_BUNDLE_REQUEST_DURATION,
        "Duration of eth_sendBundle requests in seconds"
    );
    describe_histogram!(
        ingress::SEND_MEV_SHARE_BUNDLE_REQUEST_DURATION,
        "Duration of mev_sendBundle requests in seconds"
    );
    describe_histogram!(
        ingress::SEND_TRANSACTION_REQUEST_DURATION,
        "Duration of eth_sendRawTransaction requests in seconds"
    );
    describe_counter!(ingress::VALIDATION_ERRORS, "Total number of validation errors");
    describe_histogram!(ingress::TXS_PER_BUNDLE, "Number of transactions per bundle");
    describe_histogram!(
        ingress::TXS_PER_MEV_SHARE_BUNDLE,
        "Number of transactions per MEV-share bundle"
    );
    describe_counter!(ingress::TOTAL_EMPTY_BUNDLES, "Total number of cancellation bundles");

    // System end-to-end processing metrics
    describe_histogram!(
        system::E2E_BUNDLE_PROCESSING_TIME,
        "End-to-end bundle processing time in seconds"
    );
    describe_histogram!(
        system::E2E_MEV_SHARE_BUNDLE_PROCESSING_TIME,
        "End-to-end MEV-share bundle processing time in seconds"
    );
    describe_histogram!(
        system::E2E_TRANSACTION_PROCESSING_TIME,
        "End-to-end transaction processing time in seconds"
    );
    describe_histogram!(
        system::E2E_SYSTEM_ORDER_PROCESSING_TIME,
        "End-to-end system order processing time in seconds"
    );
}

#[derive(Debug)]
pub struct ForwarderMetrics;

impl ForwarderMetrics {
    /// Set the number of inflight HTTP requests.
    #[inline]
    pub fn set_inflight_requests(count: usize) {
        gauge!(forwarder::INFLIGHT_REQUESTS).set(count as f64);
    }

    /// The duration of the RPC call to a peer.
    #[inline]
    pub fn record_rpc_call(
        peer_name: String,
        order_type: &'static str,
        duration: Duration,
        big_request: bool,
    ) {
        histogram!(forwarder::RPC_CALL_DURATION, "peer_name" => peer_name, "order_type" => order_type, "big_request" => big_request.to_string()).record(duration.as_secs_f64());
    }

    #[inline]
    pub fn increment_http_call_failures(peer_name: String, reason: String) {
        counter!(forwarder::HTTP_CALL_FAILURES, "peer_name" => peer_name, "reason" => reason)
            .increment(1);
    }

    #[inline]
    pub fn increment_rpc_call_failures(peer_name: String, rpc_code: i32) {
        counter!(forwarder::RPC_CALL_FAILURES, "peer_name" => peer_name, "rpc_code" => rpc_code.to_string()).increment(1);
    }

    #[inline]
    pub fn increment_json_rpc_decoding_failures(peer_name: String) {
        counter!(forwarder::JSON_RPC_DECODING_FAILURES, "peer_name" => peer_name).increment(1);
    }
}

pub trait IngressHandlerMetricsExt {
    const HANDLER: &str;

    #[inline]
    fn set_entity_count(count: usize) {
        gauge!(ingress::ENTITY_COUNT).set(count as f64);
    }

    #[inline]
    fn increment_requests_rate_limited() {
        counter!(ingress::REQUESTS_RATE_LIMITED, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_parse_errors() {
        counter!(ingress::JSON_RPC_PARSE_ERRORS, "handler" => Self::HANDLER).increment(1);
    }

    #[inline]
    fn increment_json_rpc_unknown_method(method: String) {
        counter!(ingress::JSON_RPC_UNKNOWN_METHOD, "handler" => Self::HANDLER, "method" => method)
            .increment(1);
    }

    #[inline]
    fn increment_validation_errors<E: std::error::Error>(error: &E) {
        counter!(ingress::VALIDATION_ERRORS, "handler" => Self::HANDLER, "error" => error.to_string()).increment(1);
    }

    #[inline]
    fn increment_order_cache_hit(order_type: &'static str) {
        counter!(ingress::ORDER_CACHE_HIT, "handler" => Self::HANDLER, "order_type" => order_type)
            .increment(1);
    }

    #[inline]
    fn record_http_request(method: &Method, path: String, status: StatusCode, duration: Duration) {
        let method = match *method {
            Method::GET => "GET",
            Method::POST => "POST",
            Method::PUT => "PUT",
            _ => "Unhandled",
        };

        let reason = status.canonical_reason().unwrap_or("unknown");

        histogram!(ingress::HTTP_REQUEST_DURATION, "handler" => Self::HANDLER, "method" => method, "path" => path, "status" => reason).record(duration.as_secs_f64());
    }

    // BUNDLES
    #[inline]
    fn record_txs_per_bundle(txs: usize) {
        histogram!(ingress::TXS_PER_BUNDLE, "handler" => Self::HANDLER).record(txs as f64);
    }

    #[inline]
    fn record_txs_per_mev_share_bundle(txs: usize) {
        histogram!(ingress::TXS_PER_MEV_SHARE_BUNDLE, "handler" => Self::HANDLER)
            .record(txs as f64);
    }

    /// The duration of the `eth_sendBundle` RPC call.
    #[inline]
    fn record_bundle_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::SEND_BUNDLE_REQUEST_DURATION, "handler" => Self::HANDLER, "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    /// The duration of the `mev_sendBundle` RPC call.
    #[inline]
    fn record_mev_share_bundle_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::SEND_MEV_SHARE_BUNDLE_REQUEST_DURATION, "handler" => Self::HANDLER, "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    /// The duration of the `eth_sendRawTransaction` RPC call.
    #[inline]
    fn record_transaction_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::SEND_TRANSACTION_REQUEST_DURATION, "handler" => Self::HANDLER, "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    #[inline]
    fn increment_empty_bundles() {
        counter!(ingress::TOTAL_EMPTY_BUNDLES, "handler" => Self::HANDLER).increment(1);
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
    pub fn record_e2e_bundle_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let big_request = if big_request { "true" } else { "false" };
        let labels = [
            ("priority", priority.as_str()),
            ("direction", direction.as_str()),
            ("big_request", big_request),
        ];

        histogram!(system::E2E_BUNDLE_PROCESSING_TIME, &labels).record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_e2e_mev_share_bundle_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let big_request = if big_request { "true" } else { "false" };
        let labels = [
            ("priority", priority.as_str()),
            ("direction", direction.as_str()),
            ("big_request", big_request),
        ];

        histogram!(system::E2E_MEV_SHARE_BUNDLE_PROCESSING_TIME, &labels)
            .record(duration.as_secs_f64());
    }

    #[inline]
    pub fn record_e2e_transaction_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        big_request: bool,
    ) {
        let big_request = if big_request { "true" } else { "false" };
        let labels = [
            ("priority", priority.as_str()),
            ("direction", direction.as_str()),
            ("big_request", big_request),
        ];

        histogram!(system::E2E_TRANSACTION_PROCESSING_TIME, &labels).record(duration.as_secs_f64());
    }

    pub fn record_e2e_system_order_processing_time(
        duration: Duration,
        priority: Priority,
        direction: ForwardingDirection,
        order_type: &'static str,
        big_request: bool,
    ) {
        let big_request = if big_request { "true" } else { "false" };
        let labels = [
            ("order_type", order_type),
            ("priority", priority.as_str()),
            ("direction", direction.as_str()),
            ("big_request", big_request),
        ];

        histogram!(system::E2E_SYSTEM_ORDER_PROCESSING_TIME, &labels)
            .record(duration.as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub struct BuilderHubMetrics;

#[allow(missing_debug_implementations)]
impl BuilderHubMetrics {
    #[inline]
    pub fn increment_builderhub_peer_request_failures(err: String) {
        counter!(builderhub::PEER_REQUEST_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_builderhub_registration_failures(err: String) {
        counter!(builderhub::REGISTRATION_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn builderhub_peer_count(count: usize) {
        gauge!(builderhub::PEER_COUNT).set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct IndexerMetrics;

impl IndexerMetrics {
    #[inline]
    pub fn increment_bundle_indexing_failures(err: String) {
        counter!(indexer::BUNDLE_INDEXING_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_bundle_receipt_indexing_failures(err: String) {
        counter!(indexer::BUNDLE_RECEIPT_INDEXING_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_write_failures(err: String) {
        counter!(indexer::CLICKHOUSE_WRITE_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_clickhouse_commit_failures(err: String) {
        counter!(indexer::CLICKHOUSE_COMMIT_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn set_clickhouse_queue_size(size: usize, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_QUEUE_SIZE, "order" => order).set(size as f64);
    }

    #[inline]
    pub fn set_parquet_queue_size(size: usize, order: &'static str) {
        gauge!(indexer::PARQUET_QUEUE_SIZE, "order" => order).set(size as f64);
    }

    /// Process the quantities from the Clickhouse inserter. No-op if the quantities are zero.
    #[inline]
    pub fn process_clickhouse_quantities(quantities: &Quantities) {
        if quantities == &Quantities::ZERO {
            return;
        }

        Self::increment_clickhouse_rows_committed(quantities.rows);
        Self::increment_clickhouse_bytes_committed(quantities.bytes);
        Self::increment_clickhouse_batches_committed();
    }

    #[inline]
    fn increment_clickhouse_rows_committed(rows: u64) {
        counter!(indexer::CLICKHOUSE_ROWS_COMMITTED).increment(rows);
    }

    #[inline]
    fn increment_clickhouse_bytes_committed(bytes: u64) {
        counter!(indexer::CLICKHOUSE_BYTES_COMMITTED).increment(bytes);
    }

    fn increment_clickhouse_batches_committed() {
        counter!(indexer::CLICKHOUSE_BATCHES_COMMITTED).increment(1);
    }
}

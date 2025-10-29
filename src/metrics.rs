//! Metrics for the system. We don't use `metrics_derive` here because it doesn't allow for dynamic
//! label
use std::time::Duration;

use hyper::{Method, StatusCode};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::{forwarder::ForwardingDirection, primitives::Quantities, priority::Priority};

/// Forwarder metrics.
#[prom_derive::metrics(scope = "forwarder")]
#[derive(Debug, Clone)]
pub(crate) struct ForwarderMetrics {
    /// The number of HTTP connection failures.
    #[metric(labels = ["peer_name", "reason"])]
    http_connect_failures: IntCounter,

    /// The number of HTTP call failures.
    #[metric(labels = ["peer_name", "reason"])]
    http_call_failures: IntCounter,

    /// The number of inflight HTTP requests.
    #[metric(labels = ["peer_name"])]
    inflight_requests: IntGauge,

    #[metric(labels = ["peer_name"])]
    open_http_connections: IntGauge,

    /// The number of JSON-RPC decoding failures.
    #[metric(labels = ["peer_name"])]
    json_rpc_decoding_failures: IntCounter,

    /// The duration of RPC calls.
    #[metric(labels = ["peer_name", "order_type", "big_request"])]
    rpc_call_duration: Histogram,

    /// The number of RPC call failures.
    #[metric(labels = ["peer_name", "rpc_code"])]
    rpc_call_failures: IntCounter,
}

mod name {
    /// BuilderHub metrics.
    pub(crate) mod builderhub {
        pub(crate) const PEER_COUNT: &str = "builderhub_peer_count";
        pub(crate) const REGISTRATION_FAILURES: &str = "builderhub_registration_failures";
        pub(crate) const PEER_REQUEST_FAILURES: &str = "builderhub_peer_request_failures";
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
        pub(crate) const CLICKHOUSE_BATCH_COMMIT_TIME: &str =
            "indexer_clickhouse_batch_commit_time";

        pub(crate) const CLICKHOUSE_BACKUP_SIZE_BYTES: &str =
            "indexer_clickhouse_backup_size_bytes";
        pub(crate) const CLICKHOUSE_BACKUP_SIZE_BATCHES: &str =
            "indexer_clickhouse_backup_size_batches";
        pub(crate) const CLICKHOUSE_BACKUP_DATA_BYTES: &str =
            "indexer_clickhouse_backup_data_bytes_total";
        pub(crate) const CLICKHOUSE_BACKUP_DATA_ROWS: &str =
            "indexer_clickhouse_backup_data_rows_total";
        pub(crate) const CLICKHOUSE_BACKUP_DATA_LOST_BYTES: &str =
            "indexer_clickhouse_backup_data_lost_bytes_total";
        pub(crate) const CLICKHOUSE_BACKUP_DATA_LOST_ROWS: &str =
            "indexer_clickhouse_backup_data_lost_rows_total";
        pub(crate) const CLICKHOUSE_BACKUP_DISK_ERRORS: &str =
            "indexer_clickhouse_backup_disk_errors";

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
        pub(crate) const RPC_REQUEST_DURATION: &str = "ingress_rpc_request_duration";
        pub(crate) const VALIDATION_ERRORS: &str = "ingress_validation_errors";

        pub(crate) const REQUEST_BODY_SIZE_DECOMPRESSED: &str =
            "ingress_request_body_size_decompressed";
        pub(crate) const TXS_PER_BUNDLE: &str = "ingress_txs_per_bundle";
        pub(crate) const TXS_PER_MEV_SHARE_BUNDLE: &str = "ingress_txs_per_mev_share_bundle";
        pub(crate) const TOTAL_EMPTY_BUNDLES: &str = "ingress_total_empty_bundles";
        pub(crate) const ORDER_CACHE_HIT_RATIO: &str = "ingress_order_cache_hit_ratio";
        pub(crate) const SIGNER_CACHE_HIT_RATIO: &str = "ingress_signer_cache_hit_ratio";
        pub(crate) const ORDER_CACHE_ENTRY_COUNT: &str = "ingress_order_cache_entry_count";
        pub(crate) const SIGNER_CACHE_ENTRY_COUNT: &str = "ingress_signer_cache_entry_count";
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
        pub(crate) const QUEUE_CAPACITY_HITS: &str = "system_queue_capacity_hits";
        pub(crate) const QUEUE_CAPACITY_ALMOST_HITS: &str = "system_queue_capacity_almost_hits";
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

    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_SIZE_BYTES,
        "Current size of Clickhouse backup in bytes"
    );

    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_SIZE_BATCHES,
        "Current size of Clickhouse backup in batches"
    );

    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_DATA_BYTES,
        "Total number of bytes sent to Clickhouse backup"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_DATA_ROWS,
        "Total number of rows sent to Clickhouse backup"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_DATA_LOST_BYTES,
        "Total number of bytes lost due to pressure on Clickhouse backup"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_DATA_LOST_ROWS,
        "Total number of rows lost due to pressure on Clickhouse backup"
    );
    describe_counter!(
        indexer::CLICKHOUSE_BACKUP_DISK_ERRORS,
        "Errors encountered during Clickhouse disk backup"
    );

    describe_histogram!(
        indexer::CLICKHOUSE_BATCH_COMMIT_TIME,
        "Duration of Clickhouse batch commits in seconds"
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
    describe_gauge!(
        ingress::ORDER_CACHE_HIT_RATIO,
        "Ratio of order cache hits (successful cache hits / total orders) by order type"
    );
    describe_counter!(
        ingress::REQUESTS_RATE_LIMITED,
        "Total number of incoming rate-limited requests"
    );
    describe_histogram!(
        ingress::RPC_REQUEST_DURATION,
        "Duration of incoming RPC requests in seconds"
    );
    describe_counter!(ingress::VALIDATION_ERRORS, "Total number of validation errors");

    describe_histogram!(
        ingress::REQUEST_BODY_SIZE_DECOMPRESSED,
        "Size of incoming request bodies in bytes, decompressed"
    );
    describe_histogram!(ingress::TXS_PER_BUNDLE, "Number of transactions per bundle");
    describe_histogram!(
        ingress::TXS_PER_MEV_SHARE_BUNDLE,
        "Number of transactions per MEV-share bundle"
    );
    describe_counter!(ingress::TOTAL_EMPTY_BUNDLES, "Total number of cancellation bundles");
    describe_gauge!(
        ingress::SIGNER_CACHE_HIT_RATIO,
        "Ratio of transaction signer cache hits (successful cache hits / total transactions)"
    );
    describe_gauge!(ingress::ORDER_CACHE_ENTRY_COUNT, "Number of entries in the order cache");
    describe_gauge!(ingress::SIGNER_CACHE_ENTRY_COUNT, "Number of entries in the signer cache");

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
    describe_counter!(
        system::QUEUE_CAPACITY_HITS,
        "Number of times the queue capacity was hit per priority"
    );
    describe_counter!(
        system::QUEUE_CAPACITY_ALMOST_HITS,
        "Number of times the queue capacity was almost hit per priority (>= 75% of capacity)"
    );
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
    fn increment_json_rpc_parse_errors(method: &'static str) {
        counter!(ingress::JSON_RPC_PARSE_ERRORS, "handler" => Self::HANDLER, "method" => method)
            .increment(1);
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

    #[inline]
    fn record_request_body_size_bytes(size: usize, method: &'static str) {
        histogram!(ingress::REQUEST_BODY_SIZE_DECOMPRESSED, "handler" => Self::HANDLER, method => "method")
            .record(size as f64);
    }

    /// The duration of the `eth_sendBundle` RPC call.
    #[inline]
    fn record_bundle_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::RPC_REQUEST_DURATION, "handler" => Self::HANDLER, "method" => "eth_sendBundle", "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    /// The duration of the `mev_sendBundle` RPC call.
    #[inline]
    fn record_mev_share_bundle_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::RPC_REQUEST_DURATION, "handler" => Self::HANDLER, "method" => "mev_sendBundle", "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    /// The duration of the `eth_sendRawTransaction` RPC call.
    #[inline]
    fn record_transaction_rpc_duration(priority: Priority, duration: Duration) {
        histogram!(ingress::RPC_REQUEST_DURATION, "handler" => Self::HANDLER, "method" => "eth_sendRawTransaction", "priority" => priority.as_str())
            .record(duration.as_secs_f64());
    }

    #[inline]
    fn increment_empty_bundles() {
        counter!(ingress::TOTAL_EMPTY_BUNDLES, "handler" => Self::HANDLER).increment(1);
    }

    /// Set the order cache hit ratio.
    #[inline]
    fn set_order_cache_hit_ratio(ratio: f64) {
        gauge!(ingress::ORDER_CACHE_HIT_RATIO, "handler" => Self::HANDLER).set(ratio);
    }

    #[inline]
    fn set_order_cache_entry_count(count: u64) {
        gauge!(ingress::ORDER_CACHE_ENTRY_COUNT, "handler" => Self::HANDLER).set(count as f64);
    }

    /// Set the signer cache hit ratio.
    #[inline]
    fn set_signer_cache_hit_ratio(ratio: f64) {
        gauge!(ingress::SIGNER_CACHE_HIT_RATIO, "handler" => Self::HANDLER).set(ratio);
    }

    #[inline]
    fn set_signer_cache_entry_count(count: u64) {
        gauge!(ingress::SIGNER_CACHE_ENTRY_COUNT, "handler" => Self::HANDLER).set(count as f64);
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

    #[inline]
    pub fn increment_queue_capacity_hit(priority: Priority) {
        counter!(system::QUEUE_CAPACITY_HITS, "priority" => priority.as_str()).increment(1);
    }

    #[inline]
    pub fn increment_queue_capacity_almost_hit(priority: Priority) {
        counter!(system::QUEUE_CAPACITY_ALMOST_HITS, "priority" => priority.as_str()).increment(1);
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
    // Counters

    #[inline]
    pub fn increment_bundle_indexing_failures(err: &'static str) {
        counter!(indexer::BUNDLE_INDEXING_FAILURES, "error" => err).increment(1);
    }

    #[inline]
    pub fn increment_bundle_receipt_indexing_failures(err: &'static str) {
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
    fn increment_clickhouse_backup_data_bytes(size: u64) {
        counter!(indexer::CLICKHOUSE_BACKUP_DATA_BYTES).increment(size);
    }

    fn increment_clickhouse_backup_data_rows(count: u64) {
        counter!(indexer::CLICKHOUSE_BACKUP_DATA_ROWS).increment(count);
    }

    /// Process the quantities of data lost because pressure has been applied to backup.
    #[inline]
    pub fn process_clickhouse_backup_data_quantities(quantities: &Quantities) {
        Self::increment_clickhouse_backup_data_bytes(quantities.bytes);
        Self::increment_clickhouse_backup_data_rows(quantities.rows);
    }

    #[inline]
    fn increment_clickhouse_backup_data_lost_bytes(size: u64) {
        counter!(indexer::CLICKHOUSE_BACKUP_DATA_LOST_BYTES).increment(size);
    }

    fn increment_clickhouse_backup_data_lost_rows(count: u64) {
        counter!(indexer::CLICKHOUSE_BACKUP_DATA_LOST_ROWS).increment(count);
    }

    /// Process the quantities of data lost because pressure has been applied to backup.
    #[inline]
    pub fn process_clickhouse_backup_data_lost_quantities(quantities: &Quantities) {
        Self::increment_clickhouse_backup_data_lost_bytes(quantities.bytes);
        Self::increment_clickhouse_backup_data_lost_rows(quantities.rows);
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

    // Gauges

    #[inline]
    pub fn set_clickhouse_queue_size(size: usize, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_QUEUE_SIZE, "order" => order).set(size as f64);
    }

    #[inline]
    pub fn set_clickhouse_memory_backup_size(size_bytes: u64, batches: usize, order: &'static str) {
        Self::set_clickhouse_backup_memory_size_bytes(size_bytes, order);
        Self::set_clickhouse_backup_memory_size_batches(batches, order);
    }

    #[inline]
    pub fn set_clickhouse_backup_memory_size_bytes(size: u64, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_BACKUP_SIZE_BYTES, "backup" => "memory", "order" => order)
            .set(size as f64);
    }

    #[inline]
    pub fn set_clickhouse_backup_memory_size_batches(size: usize, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_BACKUP_SIZE_BATCHES, "backup" => "memory", "order" => order)
            .set(size as f64);
    }

    #[inline]
    pub fn set_clickhouse_disk_backup_size(size_bytes: u64, batches: usize, order: &'static str) {
        Self::set_clickhouse_backup_disk_size_bytes(size_bytes, order);
        Self::set_clickhouse_backup_disk_size_batches(batches, order);
    }

    #[inline]
    pub fn set_clickhouse_backup_disk_size_bytes(size: u64, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_BACKUP_SIZE_BYTES, "backup" => "disk", "order" => order)
            .set(size as f64);
    }

    #[inline]
    pub fn set_clickhouse_backup_disk_size_batches(size: usize, order: &'static str) {
        gauge!(indexer::CLICKHOUSE_BACKUP_SIZE_BATCHES, "backup" => "disk", "order" => order)
            .set(size as f64);
    }

    #[inline]
    pub fn set_clickhouse_backup_empty_size(order: &'static str) {
        Self::set_clickhouse_backup_memory_size_bytes(0, order);
        Self::set_clickhouse_backup_memory_size_batches(0, order);
        Self::set_clickhouse_backup_disk_size_bytes(0, order);
        Self::set_clickhouse_backup_disk_size_batches(0, order);
    }

    #[inline]
    pub fn increment_clickhouse_backup_disk_errors(order: &'static str, error: &str) {
        counter!(indexer::CLICKHOUSE_BACKUP_DISK_ERRORS, "order" => order, "error" => error.to_string())
            .increment(1);
    }

    #[inline]
    pub fn set_parquet_queue_size(size: usize, order: &'static str) {
        gauge!(indexer::PARQUET_QUEUE_SIZE, "order" => order).set(size as f64);
    }

    // Histograms

    #[inline]
    pub fn record_clickhouse_batch_commit_time(duration: Duration) {
        histogram!(indexer::CLICKHOUSE_BATCH_COMMIT_TIME).record(duration.as_secs_f64());
    }
}

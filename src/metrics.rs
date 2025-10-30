//! Metrics for the system. We don't use `metrics_derive` here because it doesn't allow for dynamic
//! label
use std::time::Duration;

use hyper::{Method, StatusCode};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use prometric::{Counter, Gauge, Histogram};

use crate::{forwarder::ForwardingDirection, primitives::Quantities, priority::Priority};

#[prometric_derive::metrics(scope = "builderhub")]
pub(crate) struct BuilderHubMetrics {
    /// The peer count.
    #[metric]
    peer_count: Gauge,
    /// The number of peer request failures.
    #[metric(labels = ["error"])]
    peer_request_failures: Counter,
}

/// Forwarder metrics.
#[prometric_derive::metrics(scope = "forwarder")]
#[derive(Debug, Clone)]
pub(crate) struct ForwarderMetrics {
    /// The number of HTTP connection failures.
    #[metric(labels = ["reason"])]
    http_connect_failures: Counter,
    /// The number of HTTP call failures.
    #[metric(labels = ["reason"])]
    http_call_failures: Counter,
    /// The number of inflight HTTP requests.
    #[metric]
    inflight_requests: Gauge,
    /// The number of open HTTP connections.
    #[metric]
    open_http_connections: Gauge,
    /// The number of JSON-RPC decoding failures.
    #[metric]
    json_rpc_decoding_failures: Counter,
    /// The duration of RPC calls.
    #[metric(labels = ["order_type", "big_request"])]
    rpc_call_duration: Histogram,
    /// The number of RPC call failures.
    #[metric(labels = ["rpc_code"])]
    rpc_call_failures: Counter,
}

#[derive(Debug)]
#[prometric_derive::metrics(scope = "ingress")]
pub(crate) struct IngressMetrics {
    /// The number of entities.
    #[metric]
    entity_count: Gauge,
    /// The number of requests rate limited.
    #[metric]
    requests_rate_limited: Counter,
    /// The number of JSON-RPC parsing errors.
    #[metric(labels = ["method"])]
    json_rpc_parse_errors: Counter,
    /// The number of JSON-RPC unknown methods.
    #[metric(labels = ["method"])]
    json_rpc_unknown_method: Counter,
    /// The number of order cache hits.
    #[metric(labels = ["order_type"])]
    order_cache_hit: Counter,
    /// The number of order cache misses.
    #[metric(labels = ["order_type"])]
    order_cache_miss: Counter,
    /// Request body size in bytes.
    #[metric(rename = "request_body_size_bytes", labels = ["method"])]
    request_body_size: Histogram,
    /// The number of signer cache hits.
    #[metric]
    signer_cache_hit: Counter,
    /// The number of signer cache misses.
    #[metric]
    signer_cache_miss: Counter,
    /// The number of validation errors.
    #[metric(labels = ["error"])]
    validation_errors: Counter,
    /// The duration of HTTP requests.
    #[metric(labels = ["method", "path", "status"])]
    http_request_duration: Histogram,
    /// The duration of RPC calls.
    #[metric(labels = ["method", "priority"])]
    rpc_request_duration: Histogram,
    /// The number of transactions per bundle.
    #[metric]
    txs_per_bundle: Histogram,
    /// The number of transactions per MEV-share bundle.
    #[metric]
    txs_per_mev_share_bundle: Histogram,
    /// The number of empty bundles.
    #[metric]
    total_empty_bundles: Counter,
    /// The order cache hit ratio.
    #[metric]
    order_cache_hit_ratio: Gauge,
    /// The signer cache hit ratio.
    #[metric]
    signer_cache_hit_ratio: Gauge,
    /// The order cache entry count.
    #[metric]
    order_cache_entry_count: Gauge,
    /// The signer cache entry count.
    #[metric]
    signer_cache_entry_count: Gauge,
}

mod name {
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

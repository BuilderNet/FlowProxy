//! Metrics for the system. We don't use `metrics_derive` here because it doesn't allow for dynamic
//! label
use std::time::Duration;

use metrics::{counter, describe_counter, describe_histogram, histogram};
use prometric::{Counter, Gauge, Histogram};

use crate::{forwarder::ForwardingDirection, priority::Priority};

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

#[prometric_derive::metrics(scope = "indexer")]
pub struct IndexerMetrics {
    /// Total number of bundle indexing failures.
    #[metric(labels = ["error"])]
    bundle_indexing_failures: Counter,
    /// Total number of bundle receipt indexing failures.
    #[metric(labels = ["error"])]
    bundle_receipt_indexing_failures: Counter,
}

#[prometric_derive::metrics(scope = "indexer_clickhouse")]
pub struct ClickhouseMetrics {
    /// Total number of ClickHouse commit failures.
    #[metric(labels = ["error"])]
    commit_failures: Counter,
    /// Current size of ClickHouse write queue.
    #[metric(rename = "queue_size", labels = ["order"])]
    queue_len: Gauge,
    /// Total number of ClickHouse write failures.
    #[metric(labels = ["error"])]
    write_failures: Counter,
    /// Total number of rows committed to ClickHouse.
    #[metric]
    rows_committed: Counter,
    /// Total number of bytes committed to ClickHouse.
    #[metric]
    bytes_committed: Counter,
    /// Total number of batches committed to ClickHouse.
    #[metric]
    batches_committed: Counter,
    /// Duration of Clickhouse batch commits in seconds.
    #[metric]
    batch_commit_time: Histogram,
    /// Current size of ClickHouse backup in bytes.
    #[metric(labels = ["order", "backend"])]
    backup_size_bytes: Gauge,
    /// Current size of ClickHouse backup in batches.
    #[metric(labels = ["order", "backend"])]
    backup_size_batches: Gauge,
    /// Total number of bytes sent to Clickhouse backup.
    #[metric(rename = "clickhouse_backup_data_bytes_total")]
    backup_data_bytes: Counter,
    /// Total number of rows sent to Clickhouse backup.
    #[metric(rename = "clickhouse_backup_data_rows_total")]
    backup_data_rows: Counter,
    /// Total number of bytes lost due to pressure on Clickhouse backup.
    #[metric(rename = "clickhouse_backup_data_lost_bytes_total")]
    backup_data_lost_bytes: Counter,
    /// Total number of rows lost due to pressure on Clickhouse backup.
    #[metric(rename = "clickhouse_backup_data_lost_rows_total")]
    backup_data_lost_rows: Counter,
    /// Errors encountered during Clickhouse disk backup.
    #[metric(labels = ["order", "error"])]
    backup_disk_errors: Counter,
}

#[prometric_derive::metrics(scope = "indexer_parquet")]
pub struct ParquetMetrics {
    /// Current size of Parquet write queue.
    #[metric(labels = ["order"])]
    queue_size: Gauge,
}

mod name {
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

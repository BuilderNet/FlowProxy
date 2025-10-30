//! FlowProxy metrics with [`prometric_derive`].
use std::sync::LazyLock;

use prometric::{Counter, Gauge, Histogram};

/// The system metrics. We use a lazy lock here to make sure they're globally accessible and
/// initialized only once.
pub(crate) static SYSTEM_METRICS: LazyLock<SystemMetrics> = LazyLock::new(SystemMetrics::default);

/// Global HTTP metrics.
pub(crate) static HTTP_METRICS: LazyLock<HttpMetrics> = LazyLock::new(HttpMetrics::default);

/// Global Clickhouse metrics.
pub(crate) static CLICKHOUSE_METRICS: LazyLock<ClickhouseMetrics> =
    LazyLock::new(ClickhouseMetrics::default);

#[derive(Debug)]
#[prometric_derive::metrics(scope = "builderhub")]
pub(crate) struct BuilderHubMetrics {
    /// The peer count.
    #[metric]
    peer_count: Gauge,
    /// The number of peer request failures.
    #[metric(labels = ["error"])]
    peer_request_failures: Counter,
}

#[prometric_derive::metrics(scope = "forwarder")]
pub(crate) struct HttpMetrics {
    /// The number of open HTTP connections.
    #[metric(labels = ["peer_name"])]
    open_http_connections: Gauge,
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

#[derive(Debug, Clone)]
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
    /// Request body size in bytes.
    #[metric(rename = "request_body_size_bytes", labels = ["method"])]
    request_body_size: Histogram,
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

#[derive(Debug)]
#[prometric_derive::metrics(scope = "indexer")]
pub struct IndexerMetrics {
    /// Total number of bundle indexing failures.
    #[metric(labels = ["error"])]
    bundle_indexing_failures: Counter,
    /// Total number of bundle receipt indexing failures.
    #[metric(labels = ["error"])]
    bundle_receipt_indexing_failures: Counter,
}

#[derive(Debug, Clone)]
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
pub(crate) struct ParquetMetrics {
    /// Current size of Parquet write queue.
    #[metric(labels = ["order"])]
    queue_size: Gauge,
}

#[derive(Debug, Clone)]
#[prometric_derive::metrics(scope = "system")]
pub(crate) struct SystemMetrics {
    /// End-to-end bundle processing time in seconds.
    #[metric(rename = "e2e_bundle_processing_time", labels = ["priority", "direction", "big_request"])]
    bundle_processing_time: Histogram,
    /// End-to-end MEV-share bundle processing time in seconds.
    #[metric(rename = "e2e_mev_share_bundle_processing_time", labels = ["priority", "direction", "big_request"])]
    mev_share_bundle_processing_time: Histogram,
    /// End-to-end transaction processing time in seconds.
    #[metric(rename = "e2e_transaction_processing_time", labels = ["priority", "direction", "big_request"])]
    transaction_processing_time: Histogram,
    /// End-to-end system order processing time in seconds.
    #[metric(rename = "e2e_system_order_processing_time", labels = ["priority", "direction", "order_type", "big_request"])]
    system_order_processing_time: Histogram,
    /// Number of times the queue capacity was hit per priority.
    #[metric(labels = ["priority"])]
    queue_capacity_hits: Counter,
    /// Number of times the queue capacity was almost hit per priority (>= 75% of capacity).
    #[metric(labels = ["priority"])]
    queue_capacity_almost_hits: Counter,
}

// pub struct Metrics {
//     /// Total user and system CPU time spent in seconds.
//     pub cpu_seconds_total: Option<f64>,
//     /// Number of open file descriptors.
//     pub open_fds: Option<u64>,
//     /// Maximum number of open file descriptors.
//     ///
//     /// 0 indicates 'unlimited'.
//     pub max_fds: Option<u64>,
//     /// Virtual memory size in bytes.
//     pub virtual_memory_bytes: Option<u64>,
//     /// Maximum amount of virtual memory available in bytes.
//     ///
//     /// 0 indicates 'unlimited'.
//     pub virtual_memory_max_bytes: Option<u64>,
//     /// Resident memory size in bytes.
//     pub resident_memory_bytes: Option<u64>,
//     /// Start time of the process since unix epoch in seconds.
//     pub start_time_seconds: Option<u64>,
//     /// Numberof OS threads in the process.
//     pub threads: Option<u64>,
// }
#[derive(Debug)]
#[prometric_derive::metrics(scope = "process")]
pub struct ProcessMetrics {
    /// Total user and system CPU time spent in seconds.
    #[metric]
    cpu_seconds_total: Gauge,
    /// Number of open file descriptors.
    #[metric]
    open_fds: Gauge,
    /// Maximum number of open file descriptors.
    #[metric]
    max_fds: Gauge,
    /// Virtual memory size in bytes.
    #[metric]
    virtual_memory_bytes: Gauge,
    /// Maximum amount of virtual memory available in bytes.
    #[metric]
    virtual_memory_max_bytes: Gauge,
    /// Resident memory size in bytes.
    #[metric]
    resident_memory_bytes: Gauge,
    /// Start time of the process since unix epoch in seconds.
    #[metric]
    start_time_seconds: Gauge,
    /// Numberof OS threads in the process.
    #[metric]
    threads: Gauge,
}

impl ProcessMetrics {
    pub fn update(&self, metrics: metrics_process::collector::Metrics) {
        self.cpu_seconds_total().set(metrics.cpu_seconds_total.unwrap_or(0.0) as i64);
        self.open_fds().set(metrics.open_fds.unwrap_or(0) as i64);
        self.max_fds().set(metrics.max_fds.unwrap_or(0) as i64);
        self.virtual_memory_bytes().set(metrics.virtual_memory_bytes.unwrap_or(0) as i64);
        self.virtual_memory_max_bytes().set(metrics.virtual_memory_max_bytes.unwrap_or(0) as i64);
        self.resident_memory_bytes().set(metrics.resident_memory_bytes.unwrap_or(0) as i64);
        self.start_time_seconds().set(metrics.start_time_seconds.unwrap_or(0) as i64);
        self.threads().set(metrics.threads.unwrap_or(0) as i64);
    }
}

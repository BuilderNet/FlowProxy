//! FlowProxy metrics with [`prometric_derive`].
use std::{sync::LazyLock, time::Duration};

use prometric::{process::ProcessCollector, Counter, Gauge, Histogram};
use prometric_derive::metrics;

/// The system metrics. We use a lazy lock here to make sure they're globally accessible and
/// initialized only once.
pub(crate) static SYSTEM_METRICS: LazyLock<SystemMetrics> = LazyLock::new(SystemMetrics::default);

/// Global Clickhouse metrics.
pub(crate) static CLICKHOUSE_METRICS: LazyLock<ClickhouseMetrics> =
    LazyLock::new(ClickhouseMetrics::default);

#[derive(Debug)]
#[metrics(scope = "builderhub")]
pub(crate) struct BuilderHubMetrics {
    /// The peer count.
    #[metric]
    peer_count: Gauge,
    /// The total number of peer request failures.
    #[metric(labels = ["error"])]
    peer_request_failures: Counter,
}

/// Forwarder metrics.
#[metrics(scope = "forwarder")]
#[derive(Debug, Clone)]
pub(crate) struct ForwarderMetrics {
    /// The total number of HTTP connection failures.
    #[metric(labels = ["reason"])]
    http_connect_failures: Counter,
    /// The total number of HTTP call failures.
    #[metric(labels = ["reason"])]
    http_call_failures: Counter,
    /// The current number of inflight HTTP requests.
    #[metric]
    inflight_requests: Gauge,
    /// The total number of JSON-RPC decoding failures.
    #[metric]
    json_rpc_decoding_failures: Counter,
    /// The duration of RPC calls in seconds.
    #[metric(labels = ["order_type", "big_request"], buckets = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.02, 0.035, 0.05, 0.0625, 0.075, 0.0875, 0.100, 0.125, 0.150, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0])]
    rpc_call_duration: Histogram,
    /// The total number of RPC call failures.
    #[metric(labels = ["rpc_code"])]
    rpc_call_failures: Counter,
}

#[derive(Debug, Clone)]
#[metrics(scope = "ingress")]
pub(crate) struct IngressMetrics {
    /// The current number of entities.
    #[metric]
    entity_count: Gauge,
    /// The total number of requests rate limited.
    #[metric]
    requests_rate_limited: Counter,
    /// The total number of JSON-RPC parsing errors.
    #[metric(labels = ["method"])]
    json_rpc_parse_errors: Counter,
    /// The total number of JSON-RPC unknown methods.
    #[metric(labels = ["method"])]
    json_rpc_unknown_method: Counter,
    /// The total number of order cache hits.
    #[metric(labels = ["order_type"])]
    order_cache_hit: Counter,
    /// Request body size in bytes.
    #[metric(rename = "request_body_size_bytes", labels = ["method"], buckets = [128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0, 262144.0, 524288.0, 1048576.0, 2097152.0, 4194304.0])]
    request_body_size: Histogram,
    /// The total number of validation errors.
    #[metric(labels = ["error"])]
    validation_errors: Counter,
    /// The duration of HTTP requests.
    #[metric(labels = ["method", "path", "status"], buckets = [0.0001, 0.0005, 0.001, 0.005, 0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1.0, 2.0])]
    http_request_duration: Histogram,
    /// The duration of RPC calls.
    #[metric(labels = ["method", "priority"], buckets = [0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0025, 0.005, 0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1.0, 2.0])]
    rpc_request_duration: Histogram,
    /// The number of transactions per bundle.
    #[metric(buckets = [0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0])]
    txs_per_bundle: Histogram,
    /// The number of transactions per MEV-share bundle.
    #[metric(buckets = [0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0])]
    txs_per_mev_share_bundle: Histogram,
    /// The total number of empty bundles.
    #[metric]
    total_empty_bundles: Counter,
    /// The order cache hit ratio.
    #[metric]
    order_cache_hit_ratio: Gauge<f64>,
    /// The signer cache hit ratio.
    #[metric]
    signer_cache_hit_ratio: Gauge<f64>,
    /// The current order cache entry count.
    #[metric]
    order_cache_entry_count: Gauge,
    /// The current signer cache entry count.
    #[metric]
    signer_cache_entry_count: Gauge,
    /// The one-way latency of an RPC call (not round-trip).
    #[metric(rename = "rpc_latency_oneway_seconds", labels = ["source", "method"], buckets = [0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0025, 0.005, 0.010, 0.020, 0.035, 0.050, 0.075, 0.100, 0.150, 0.200, 0.500, 1.0])]
    rpc_latency_oneway: Histogram,
}

#[derive(Debug)]
#[metrics(scope = "indexer")]
pub struct IndexerMetrics {
    /// The total number of bundle indexing failures.
    #[metric(labels = ["error"])]
    bundle_indexing_failures: Counter,
    /// The total number of bundle receipt indexing failures.
    #[metric(labels = ["error"])]
    bundle_receipt_indexing_failures: Counter,
}

#[derive(Debug, Clone)]
#[metrics(scope = "indexer_clickhouse")]
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
    #[metric(buckets = [0.020, 0.050, 0.100, 0.200, 0.500, 1.0, 2.0, 4.0, 8.0, 16.0])]
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

#[metrics(scope = "indexer_parquet")]
pub(crate) struct ParquetMetrics {
    /// Current size of Parquet write queue.
    #[metric(labels = ["order"])]
    queue_size: Gauge,
}

#[derive(Debug, Clone)]
#[metrics(scope = "system")]
pub(crate) struct SystemMetrics {
    /// End-to-end bundle processing time in seconds.
    #[metric(rename = "e2e_bundle_processing_time", labels = ["priority", "direction", "big_request"], buckets = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.035, 0.05, 0.075, 0.1, 0.15, 0.2, 0.35, 0.5, 1.0, 2.0])]
    bundle_processing_time: Histogram,
    /// End-to-end MEV-share bundle processing time in seconds.
    #[metric(rename = "e2e_mev_share_bundle_processing_time", labels = ["priority", "direction", "big_request"], buckets = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.035, 0.05, 0.075, 0.1, 0.15, 0.2, 0.35, 0.5, 1.0, 2.0])]
    mev_share_bundle_processing_time: Histogram,
    /// End-to-end transaction processing time in seconds.
    #[metric(rename = "e2e_transaction_processing_time", labels = ["priority", "direction", "big_request"], buckets = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.035, 0.05, 0.075, 0.1, 0.15, 0.2, 0.35, 0.5, 1.0, 2.0])]
    transaction_processing_time: Histogram,
    /// End-to-end system order processing time in seconds.
    #[metric(rename = "e2e_system_order_processing_time", labels = ["priority", "direction", "order_type", "big_request"], buckets = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.035, 0.05, 0.075, 0.1, 0.15, 0.2, 0.35, 0.5, 1.0, 2.0])]
    system_order_processing_time: Histogram,
    /// Number of times the queue capacity was hit per priority.
    #[metric(labels = ["priority"])]
    queue_capacity_hits: Counter,
    /// Number of times the queue capacity was almost hit per priority (>= 75% of capacity).
    #[metric(labels = ["priority"])]
    queue_capacity_almost_hits: Counter,
}

#[derive(Debug, Clone)]
#[metrics(scope = "worker")]
pub(crate) struct WorkerMetrics {
    /// The duration of worker tasks in seconds, per priority. Includes the time spent waiting for
    /// the permit.
    #[metric(rename = "task_duration_seconds", labels = ["priority"], buckets = [0.00005, 0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.200, 0.500, 1.0, 2.0])]
    task_durations: Histogram,
}

pub(crate) async fn spawn_process_collector() -> eyre::Result<()> {
    let mut process_metrics = ProcessCollector::new(prometheus::default_registry());

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let start = std::time::Instant::now();
            process_metrics.collect();
            tracing::debug!(elapsed = ?start.elapsed(), "collected process metrics");
        }
    });

    Ok(())
}

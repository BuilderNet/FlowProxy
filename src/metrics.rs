//! FlowProxy metrics with [`prometric_derive`].
use std::{sync::LazyLock, time::Duration};

use prometric::{process::ProcessCollector, Counter, Gauge, Summary};
use prometric_derive::metrics;

/// The system metrics. We use a lazy lock here to make sure they're globally accessible and
/// initialized only once.
pub(crate) static SYSTEM_METRICS: LazyLock<SystemMetrics> = LazyLock::new(SystemMetrics::default);

/// Global Clickhouse metrics.
pub(crate) static CLICKHOUSE_METRICS: LazyLock<ClickhouseMetrics> =
    LazyLock::new(ClickhouseMetrics::default);

/// Build information metrics.
#[derive(Debug)]
#[metrics(scope = "build")]
pub(crate) struct BuildInfoMetrics {
    /// Build information metric. Exposes version and git commit hash.
    #[metric(labels = ["version", "commit"])]
    info: Gauge,
}

pub(crate) static BUILD_INFO_METRICS: LazyLock<BuildInfoMetrics> =
    LazyLock::new(BuildInfoMetrics::default);

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
    /// The total number of TCP call failures.
    #[metric(labels = ["reason"])]
    tcp_call_failures: Counter,
    /// The total number of TCP response failures.
    #[metric(labels = ["reason"])]
    tcp_response_failures: Counter,
    /// The current number of inflight HTTP requests.
    #[metric]
    inflight_requests: Gauge,
    /// The total number of JSON-RPC decoding failures.
    #[metric]
    json_rpc_decoding_failures: Counter,
    /// The duration of RPC calls in seconds.
    #[metric(labels = ["order_type", "big_request"])]
    rpc_call_duration: Summary,
    /// The total number of RPC call failures.
    #[metric(labels = ["rpc_code"])]
    rpc_call_failures: Counter,
}

/// Metrics related to the TCP sockets used for communication
#[metrics(scope = "socket")]
#[derive(Debug, Clone)]
pub(crate) struct SocketMetrics {
    /// The congestion window, in bytes.
    pub congestion_window: Gauge<u64>,
    /// Our receive window in bytes.
    pub receive_window: Gauge<u64>,

    /// Total sender retransmitted bytes on the socket.
    pub retransmitted_bytes: Gauge<u64>,
    /// Total sender retransmitted packets on the socket.
    pub retransmitted_packets: Gauge<u64>,
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
    #[metric(rename = "request_body_size_bytes", labels = ["method"])]
    request_body_size: Summary,
    /// The total number of validation errors.
    #[metric(labels = ["error"])]
    validation_errors: Counter,
    /// The duration of HTTP requests.
    #[metric(labels = ["method", "path", "status"])]
    http_request_duration: Summary,
    /// The duration of RPC calls.
    #[metric(labels = ["method", "priority"])]
    rpc_request_duration: Summary,
    /// The number of transactions per bundle.
    #[metric]
    txs_per_bundle: Summary,
    /// The number of transactions per MEV-share bundle.
    #[metric]
    txs_per_mev_share_bundle: Summary,
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
    /// The one-way latency of an inbound RPC call (not round-trip).
    #[metric(rename = "rpc_latency_oneway_seconds", labels = ["source", "method"])]
    rpc_latency_oneway: Summary,
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
    #[metric]
    batch_commit_time: Summary,
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
    #[metric(rename = "e2e_bundle_processing_time", labels = ["priority", "direction", "big_request"])]
    bundle_processing_time: Summary,
    /// End-to-end MEV-share bundle processing time in seconds.
    #[metric(rename = "e2e_mev_share_bundle_processing_time", labels = ["priority", "direction", "big_request"])]
    mev_share_bundle_processing_time: Summary,
    /// End-to-end transaction processing time in seconds.
    #[metric(rename = "e2e_transaction_processing_time", labels = ["priority", "direction", "big_request"])]
    transaction_processing_time: Summary,
    /// End-to-end system order processing time in seconds.
    #[metric(rename = "e2e_system_order_processing_time", labels = ["priority", "direction", "order_type", "big_request"])]
    system_order_processing_time: Summary,
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
    #[metric(rename = "task_duration_seconds", labels = ["priority"])]
    task_durations: Summary,
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

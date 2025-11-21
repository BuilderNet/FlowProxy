use std::{convert::Infallible, net::SocketAddr, num::NonZero, path::PathBuf, str::FromStr};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use clap::{Args, Parser, ValueHint};
use rbuilder_utils::clickhouse::indexer::{
    default_disk_backup_database_path, MAX_DISK_BACKUP_SIZE_BYTES, MAX_MEMORY_BACKUP_SIZE_BYTES,
};

use crate::{
    indexer::{BUNDLE_RECEIPTS_TABLE_NAME, BUNDLE_TABLE_NAME},
    SystemBundleDecoder,
};

/// The maximum request size in bytes (10 MiB).
const MAX_REQUEST_SIZE_BYTES: usize = 10 * 1024 * 1024;

/// Arguments required to create a clickhouse client.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
#[group(id = "clickhouse", requires_all = ["CLICKHOUSE_HOST", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DATABASE"])]
pub struct ClickhouseArgs {
    #[arg(
        long = "indexer.clickhouse.host",
        env = "CLICKHOUSE_HOST",
        id = "CLICKHOUSE_HOST",
        hide_env_values = true
    )]
    pub host: Option<String>,

    #[arg(
        long = "indexer.clickhouse.username",
        env = "CLICKHOUSE_USERNAME",
        id = "CLICKHOUSE_USERNAME",
        hide_env_values = true
    )]
    pub username: Option<String>,

    #[arg(
        long = "indexer.clickhouse.password",
        env = "CLICKHOUSE_PASSWORD",
        id = "CLICKHOUSE_PASSWORD",
        hide_env_values = true
    )]
    pub password: Option<String>,

    #[arg(
        long = "indexer.clickhouse.database",
        env = "CLICKHOUSE_DATABASE",
        id = "CLICKHOUSE_DATABASE"
    )]
    pub database: Option<String>,

    /// The clickhouse table name to store bundles data.
    #[arg(
        long = "indexer.clickhouse.bundles-table-name",
        env = "CLICKHOUSE_BUNDLES_TABLE_NAME",
        id = "CLICKHOUSE_BUNDLES_TABLE_NAME",
        default_value = BUNDLE_TABLE_NAME
    )]
    pub bundles_table_name: String,

    /// The clickhouse table name to store bundle receipts data.
    #[arg(
        long = "indexer.clickhouse.bundle-receipts-table-name",
        env = "CLICKHOUSE_BUNDLE_RECEIPTS_TABLE_NAME",
        id = "CLICKHOUSE_BUNDLE_RECEIPTS_TABLE_NAME",
        default_value = BUNDLE_RECEIPTS_TABLE_NAME,
    )]
    pub bundle_receipts_table_name: String,

    /// The maximum size in bytes for the in-memory backup in case of of disk-backup failure, for a
    /// certain data type (bundles or bundle receipts). Defaults to 1GiB.
    #[arg(
        long = "indexer.clickhouse.backup.memory-max-size-bytes",
        env = "CLICKHOUSE_BACKUP_MEMORY_SIZE_BYTES",
        id = "CLICKHOUSE_BACKUP_MEMORY_SIZE_BYTES",
        default_value_t = MAX_MEMORY_BACKUP_SIZE_BYTES,
    )]
    pub backup_memory_max_size_bytes: u64,

    /// The path of the (redb) database used to store failed clickhouse commits for retry. If not
    /// set, a default path of `~/.buildernet-of-proxy/clickhouse-backup.db` will be used.
    #[arg(
        long = "indexer.clickhouse.backup.disk-database-path",
        env = "CLICKHOUSE_BACKUP_DISK_DATABASE_PATH",
        id = "CLICKHOUSE_BACKUP_DISK_DATABASE_PATH",
        default_value_t = default_disk_backup_database_path()
    )]
    pub backup_disk_database_path: String,

    /// The maximum size in bytes for the disk-backed backup database.
    /// If the database exceeds this size, new entries will not be added until space is freed.
    /// Defaults to 10GiB.
    #[arg(
        long = "indexer.clickhouse.backup.disk-max-size-bytes",
        env = "CLICKHOUSE_BACKUP_DISK_MAX_SIZE_BYTES",
        id = "CLICKHOUSE_BACKUP_DISK_MAX_SIZE_BYTES",
        default_value_t = MAX_DISK_BACKUP_SIZE_BYTES
    )]
    pub backup_disk_max_size_bytes: u64,
}

/// Arguments required to setup file-based parquet indexing.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
#[group(id = "parquet", conflicts_with = "clickhouse")]
pub struct ParquetArgs {
    /// The file path to store bundle receipts data.
    #[arg(
        long = "indexer.parquet.bundle-receipts-file-path",
        env = "PARQUET_BUNDLE_RECEIPTS_FILE_PATH",
        id = "PARQUET_BUNDLE_RECEIPTS_FILE_PATH",
        value_hint = ValueHint::FilePath,
    )]
    pub bundle_receipts_file_path: Option<PathBuf>,
}

/// Arguments required to setup indexing.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
pub struct IndexerArgs {
    #[command(flatten)]
    pub clickhouse: Option<ClickhouseArgs>,
    #[command(flatten)]
    pub parquet: Option<ParquetArgs>,
}

/// Arguments required to setup caching.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
pub struct CacheArgs {
    /// The order cache TTL in seconds.
    #[clap(long = "order-cache.ttl", default_value_t = 60)]
    pub order_cache_ttl: u64,

    /// The order cache size.
    ///
    /// Defaults to 1,048,576 entries (~1 million). Since each entry is just a 32-byte hash, this
    /// results in a maximum memory usage of ~32 MiB.
    #[clap(long = "order-cache.size", default_value_t = 1_048_576)]
    pub order_cache_size: u64,

    /// The signer cache TTL in seconds.
    #[clap(long = "signer-cache.ttl", default_value_t = 36)]
    pub signer_cache_ttl: u64,

    /// The signer cache size.
    #[clap(long = "signer-cache.size", default_value_t = 16384)]
    pub signer_cache_size: u64,
}

#[derive(Parser, Debug, Clone)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), "-", env!("GIT_HASH")))]
pub struct OrderflowIngressArgs {
    /// Listen socket address for receiving user flow.
    #[clap(long, env = "USER_LISTEN_ADDR", id = "USER_LISTEN_ADDR")]
    pub user_listen_addr: SocketAddr,

    /// Listen socket address for receiving HTTP system flow.
    #[clap(long, env = "SYSTEM_LISTEN_ADDR", id = "SYSTEM_LISTEN_ADDR")]
    pub system_listen_addr_http: SocketAddr,

    /// Listen socket address for receiving TPC-only system flow.
    #[clap(long, env = "SYSTEM_LISTEN_ADDR_TCP", id = "SYSTEM_LISTEN_ADDR_TCP")]
    pub system_listen_addr_tcp: SocketAddr,

    /// Private key PEM file for client authentication (mTLS)
    #[clap(long, env = "PRIVATE_KEY_PEM_FILE", id = "PRIVATE_KEY_PEM_FILE")]
    pub private_key_pem_file: PathBuf,

    /// Certificate PEM file for client authentication (mTLS)
    #[clap(long, env = "CERTIFICATE_PEM_FILE", id = "CERTIFICATE_PEM_FILE")]
    pub certificate_pem_file: PathBuf,

    /// Listen URL for receiving builder stats.
    #[clap(long, env = "BUILDER_LISTEN_ADDR", id = "BUILDER_LISTEN_ADDR")]
    pub builder_listen_addr: Option<SocketAddr>,

    /// The URL of the local builder. This should be set in production.
    #[clap(long, value_hint = ValueHint::Url, env = "BUILDER_ENDPOINT", id = "BUILDER_ENDPOINT")]
    pub builder_url: Option<String>,

    /// The endpoint to check if the local builder is ready.
    #[clap(long, value_hint = ValueHint::Url, env = "BUILDER_READY_ENDPOINT", id = "BUILDER_READY_ENDPOINT", default_value = "http://127.0.0.1:6070")]
    pub builder_ready_endpoint: Option<String>,

    /// The name of the local builder. For consistency with BuilderHub data, dashes will be
    /// replaced with underscores.
    #[clap(long, env = "BUILDERNET_NODE_NAME", id = "BUILDERNET_NODE_NAME", value_parser = replace_dashes_with_underscores)]
    pub builder_name: String,

    /// The URL of BuilderHub.
    #[clap(long, value_hint = ValueHint::Url, env = "BUILDERHUB_ENDPOINT", id = "BUILDERHUB_ENDPOINT")]
    pub builder_hub_url: Option<String>,

    /// Enable Prometheus metrics.
    /// The metrics will be served at the given interface and port.
    #[arg(long, env = "METRICS_ADDR", id = "METRICS_ADDR")]
    pub metrics: Option<String>,

    /// The orderflow signer of this proxy.
    #[clap(
        long,
        env = "FLASHBOTS_ORDERFLOW_SIGNER",
        id = "FLASHBOTS_ORDERFLOW_SIGNER",
        hide_env_values = true
    )]
    pub orderflow_signer: Option<PrivateKeySigner>,

    /// The flashbots signer of this proxy.
    #[clap(
        long,
        env = "FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS",
        id = "FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS"
    )]
    pub flashbots_signer: Option<Address>,

    /// The maximum request size in bytes.
    #[clap(long, default_value_t = MAX_REQUEST_SIZE_BYTES)]
    pub max_request_size: usize,

    /// The maximum number of raw transactions per bundle.
    #[clap(long, default_value_t = SystemBundleDecoder::DEFAULT_MAX_TXS_PER_BUNDLE)]
    pub max_txs_per_bundle: usize,

    /// Enable rate limiting.
    #[clap(long, default_value_t = false)]
    pub enable_rate_limiting: bool,

    /// Number of seconds to look back for ratelimit computation.
    #[clap(long, default_value_t = 1)]
    pub rate_limit_lookback_s: u64,

    /// Max number of requests sent per rolling `--ratelimit-lookback-s` window, per IP.
    #[clap(long, default_value_t = 500)]
    pub rate_limit_count: u64,

    /// Number of seconds to look back for score computation.
    #[clap(long, default_value_t = 60)]
    pub score_lookback_s: u64,

    /// The number of seconds in one scoring bucket.
    #[clap(long, default_value_t = 4)]
    pub score_bucket_s: u64,

    /// Disable forwarding to peers (useful for testing).
    #[clap(long, default_value_t = false)]
    pub disable_forwarding: bool,

    /// Outputs logs in JSON format if enabled.
    #[clap(long = "log.json", default_value_t = false, env = "LOG_JSON", id = "LOG_JSON")]
    pub log_json: bool,

    /// Flag indicating whether GZIP support is enabled.
    #[clap(long = "http.enable-gzip", default_value_t = false)]
    pub gzip_enabled: bool,

    /// For each peer, the size of the HTTP client pool used to forward requests.
    #[clap(
        long = "http.client-pool-size",
        default_value_t = NonZero::new(8).expect("non-zero"),
        env = "CLIENT_POOL_SIZE",
        id = "CLIENT_POOL_SIZE"
    )]
    pub client_pool_size: NonZero<usize>,

    /// The number of IO worker threads used in Tokio.
    #[clap(long, default_value_t = 4, env = "IO_THREADS", id = "IO_THREADS")]
    pub io_threads: usize,

    /// The number of threads in the compute threadpool.
    #[clap(long, default_value_t = 4, env = "COMPUTE_THREADS", id = "COMPUTE_THREADS")]
    pub compute_threads: usize,

    #[command(flatten)]
    pub cache: CacheArgs,

    #[command(flatten)]
    pub indexing: IndexerArgs,
}

impl Default for OrderflowIngressArgs {
    fn default() -> Self {
        Self {
            user_listen_addr: SocketAddr::from_str("127.0.0.1:0").unwrap(),
            system_listen_addr_http: SocketAddr::from_str("127.0.0.1:0").unwrap(),
            system_listen_addr_tcp: SocketAddr::from_str("127.0.0.1:0").unwrap(),
            builder_listen_addr: SocketAddr::from_str("127.0.0.1:0").unwrap().into(),
            private_key_pem_file: "./".parse().unwrap(),
            certificate_pem_file: "./".parse().unwrap(),
            builder_url: None,
            builder_ready_endpoint: None,
            builder_name: String::from("buildernet"),
            builder_hub_url: None,
            flashbots_signer: None,
            max_txs_per_bundle: 100,
            enable_rate_limiting: false,
            metrics: None,
            orderflow_signer: None,
            max_request_size: MAX_REQUEST_SIZE_BYTES,
            disable_forwarding: false,
            rate_limit_lookback_s: 1,
            rate_limit_count: 500,
            score_lookback_s: 60,
            score_bucket_s: 4,
            log_json: false,
            gzip_enabled: false,
            client_pool_size: NonZero::new(8).unwrap(),
            io_threads: 4,
            compute_threads: 4,
            cache: CacheArgs {
                order_cache_ttl: 12,
                order_cache_size: 4096,
                signer_cache_ttl: 12,
                signer_cache_size: 4096,
            },
            indexing: IndexerArgs { clickhouse: None, parquet: None },
        }
    }
}

impl OrderflowIngressArgs {
    /// Set max request size.
    pub fn max_request_size(mut self, max: usize) -> Self {
        self.max_request_size = max;
        self
    }

    /// Set rate limit lookback seconds.
    pub fn rate_limit_lookback_s(mut self, lookback_s: u64) -> Self {
        self.rate_limit_lookback_s = lookback_s;
        self
    }

    /// Set rate limit count.
    pub fn rate_limit_count(mut self, count: u64) -> Self {
        self.rate_limit_count = count;
        self
    }

    /// Set the score lookback seconds.
    pub fn score_lookback_s(mut self, lookback_s: u64) -> Self {
        self.score_lookback_s = lookback_s;
        self
    }

    /// Set score bucket seconds.
    pub fn score_bucket_s(mut self, bucket_s: u64) -> Self {
        self.score_bucket_s = bucket_s;
        self
    }

    /// Enable support for gzip encoded requests.
    pub fn gzip_enabled(mut self) -> Self {
        self.gzip_enabled = true;
        self
    }

    /// Disable the builder hub.
    pub fn disable_builder_hub(mut self) -> Self {
        self.builder_hub_url = None;
        self
    }

    /// Disable forwarding to peers.
    pub fn disable_forwarding(mut self) -> Self {
        self.disable_forwarding = true;
        self
    }
}

/// Replace dashes with underscores in a string. Returns a `Result` so that it can be used
/// as a `clap` value parser.
fn replace_dashes_with_underscores(s: &str) -> Result<String, Infallible> {
    Ok(s.replace('-', "_"))
}

/// Test that optional indexing args are validated correctly and match expected usage.
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::cli::OrderflowIngressArgs;

    #[test]
    fn cli_indexing_args_optional_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-addr",
            "0.0.0.0:9754",
            "--system-listen-addr-http",
            "0.0.0.0:9755",
            "--system-listen-addr-tcp",
            "0.0.0.0:9756",
            "--private-key-pem-file",
            "./",
            "--certificate-pem-file",
            "./",
            "--builder-listen-addr",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--builder-name",
            "buildernet",
        ];

        let args = OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("optional indexing arg: {e}"));

        assert!(args.indexing.clickhouse.is_none(), "clickhouse args should not be set");
        assert!(args.indexing.parquet.is_none(), "parquet args should not be set");
    }

    #[test]
    fn cli_indexing_args_partial_fail() {
        let args = vec![
            "test", // binary name
            "--user-listen-addr",
            "0.0.0.0:9754",
            "--system-listen-addr-http",
            "0.0.0.0:9755",
            "--system-listen-addr-tcp",
            "0.0.0.0:9756",
            "--private-key-pem-file",
            "./",
            "--certificate-pem-file",
            "./",
            "--builder-listen-addr",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--builder-name",
            "buildernet",
            "--indexer.clickhouse.host",
            "http://127.0.0.1:12345",
        ];

        let err = OrderflowIngressArgs::try_parse_from(args).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("arguments were not provided"),
            "Unexpected error: {err}"
        );
        assert!(err.to_string().to_lowercase().contains("clickhouse"), "Unexpected error: {err}");
    }

    #[test]
    fn cli_indexing_args_clickhouse_provided_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-addr",
            "0.0.0.0:9754",
            "--system-listen-addr-http",
            "0.0.0.0:9755",
            "--system-listen-addr-tcp",
            "0.0.0.0:9756",
            "--private-key-pem-file",
            "./",
            "--certificate-pem-file",
            "./",
            "--builder-listen-addr",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--builder-name",
            "buildernet",
            "--indexer.clickhouse.host",
            "http://127.0.0.1:12345",
            "--indexer.clickhouse.database",
            "pronto",
            "--indexer.clickhouse.password",
            "pronto",
            "--indexer.clickhouse.username",
            "pronto",
            "--indexer.clickhouse.backup.memory-max-size-bytes",
            "512",
        ];

        let args = OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("clickhouse indexing args are provided: {e}"));

        let Some(clickhouse) = args.indexing.clickhouse else {
            panic!("clickhouse args should be set");
        };

        assert_eq!(clickhouse.host, Some(String::from("http://127.0.0.1:12345")));
        assert_eq!(clickhouse.database, Some(String::from("pronto")));
        assert_eq!(clickhouse.password, Some(String::from("pronto")));
        assert_eq!(clickhouse.username, Some(String::from("pronto")));
        assert_eq!(clickhouse.backup_memory_max_size_bytes, 512);
    }

    #[test]
    fn cli_indexing_args_parquet_provided_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-addr",
            "0.0.0.0:9754",
            "--system-listen-addr-http",
            "0.0.0.0:9755",
            "--system-listen-addr-tcp",
            "0.0.0.0:9756",
            "--private-key-pem-file",
            "./",
            "--certificate-pem-file",
            "./",
            "--builder-listen-addr",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--builder-name",
            "buildernet",
            "--indexer.parquet.bundle-receipts-file-path",
            "pronto.parquet",
        ];

        let args = OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("parquet indexing args are provided: {e}"));

        let Some(parquet) = args.indexing.parquet else {
            panic!("parquet args should be set");
        };

        assert_eq!(parquet.bundle_receipts_file_path, Some(PathBuf::from("pronto.parquet")));
    }

    #[test]
    fn cli_indexing_args_provided_both_clickhouse_parquet_fails() {
        let args = vec![
            "test", // binary name
            "--user-listen-addr",
            "0.0.0.0:9754",
            "--system-listen-addr-http",
            "0.0.0.0:9755",
            "--system-listen-addr-tcp",
            "0.0.0.0:9756",
            "--private-key-pem-file",
            "./",
            "--certificate-pem-file",
            "./",
            "--builder-listen-addr",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--builder-name",
            "buildernet",
            "--indexer.parquet.bundle-receipts-file-path",
            "pronto.parquet",
            "--indexer.clickhouse.host",
            "http://127.0.0.1:12345",
            "--indexer.clickhouse.database",
            "pronto",
            "--indexer.clickhouse.password",
            "pronto",
            "--indexer.clickhouse.username",
            "pronto",
            "--indexer.clickhouse.backup.memory-max-size-bytes",
            "512",
        ];

        let err = OrderflowIngressArgs::try_parse_from(args).unwrap_err();
        assert!(err
            .to_string()
            .contains("the argument '--indexer.parquet.bundle-receipts-file-path <PARQUET_BUNDLE_RECEIPTS_FILE_PATH>' cannot be used with"), "Unexpected error: {err}");
    }
}

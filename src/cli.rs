use std::path::PathBuf;

use alloy_signer_local::PrivateKeySigner;
use clap::{Args, Parser, ValueHint};

/// The maximum request size in bytes (10 MiB).
const MAX_REQUEST_SIZE_BYTES: usize = 10 * 1024 * 1024;

/// Arguments required to create a clickhouse client.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
#[group(id = "clickhouse", requires_all = ["host", "username", "password", "database"])]
pub struct ClickhouseArgs {
    #[arg(long = "indexer.clickhouse.host", env = "CLICKHOUSE_HOST")]
    pub host: Option<String>,

    #[arg(long = "indexer.clickhouse.username", env = "CLICKHOUSE_USERNAME")]
    pub username: Option<String>,

    #[arg(long = "indexer.clickhouse.password", env = "CLICKHOUSE_PASSWORD")]
    pub password: Option<String>,

    #[arg(long = "indexer.clickhouse.database", env = "CLICKHOUSE_DATABASE")]
    pub database: Option<String>,
}

/// Arguments required to setup file-based parquet indexing.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
#[group(id = "parquet", conflicts_with = "clickhouse")]
pub struct ParquetArgs {
    /// The file path to store bundle receipts data.
    #[arg(
        long = "indexer.parquet.bundle-receipts-file-path",
        env = "PARQUET_BUNDLE_RECEIPTS_FILE_PATH",
        value_hint = ValueHint::FilePath,
        default_value = "bundle_receipts.parquet"
    )]
    pub bundle_receipts_file_path: PathBuf,
}

/// Arguments required to setup indexing.
#[derive(PartialEq, Eq, Clone, Debug, Args)]
pub struct IndexerArgs {
    #[command(flatten)]
    pub clickhouse: Option<ClickhouseArgs>,
    #[command(flatten)]
    pub parquet: Option<ParquetArgs>,
}

#[derive(Parser, Debug)]
pub struct OrderflowIngressArgs {
    /// Listen URL for receiving user flow.
    #[clap(long, value_hint = ValueHint::Url)]
    pub user_listen_url: String,

    /// Listen URL for receiving system flow.
    #[clap(long, value_hint = ValueHint::Url)]
    pub system_listen_url: String,

    /// Listen URL for receiving builder stats.
    #[clap(long, value_hint = ValueHint::Url)]
    pub builder_listen_url: String,

    /// The URL of the local builder. This should be set in production.
    #[clap(long, value_hint = ValueHint::Url)]
    pub builder_url: Option<String>,

    /// The name of the local builder.
    #[clap(long, default_value_t = String::from("buildernet"))]
    pub builder_name: String,

    /// The URL of BuilderHub.
    #[clap(long, value_hint = ValueHint::Url)]
    pub builder_hub_url: Option<String>,

    /// Enable Prometheus metrics.
    /// The metrics will be served at the given interface and port.
    #[arg(long)]
    pub metrics: Option<String>,

    /// The orderflow signer of this proxy.
    #[clap(long)]
    pub orderflow_signer: Option<PrivateKeySigner>,

    /// The maximum request size in bytes.
    #[clap(long, default_value_t = MAX_REQUEST_SIZE_BYTES)]
    pub max_request_size: usize,

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

    /// Outputs logs in JSON format if enabled.
    #[clap(long = "log.json")]
    pub log_json: bool,

    /// Flag indicating whether GZIP support is enabled.
    #[clap(long = "http.enable-gzip")]
    pub gzip_enabled: bool,

    /// The order cache TTL in seconds.
    #[clap(long = "cache.ttl", default_value_t = 12)]
    pub cache_ttl: u64,

    /// The order cache size.
    #[clap(long = "cache.size", default_value_t = 4096)]
    pub cache_size: u64,

    #[command(flatten)]
    pub indexing: Option<IndexerArgs>,
}

impl Default for OrderflowIngressArgs {
    fn default() -> Self {
        Self {
            user_listen_url: String::from("127.0.0.1:0"),
            system_listen_url: String::from("127.0.0.1:0"),
            builder_listen_url: String::from("127.0.0.1:0"),
            builder_url: None,
            builder_name: String::from("buildernet"),
            builder_hub_url: None,
            metrics: None,
            orderflow_signer: None,
            max_request_size: MAX_REQUEST_SIZE_BYTES,
            rate_limit_lookback_s: 1,
            rate_limit_count: 500,
            score_lookback_s: 60,
            score_bucket_s: 4,
            log_json: false,
            gzip_enabled: false,
            cache_ttl: 60,
            cache_size: 4096,

            indexing: None,
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

    pub fn disable_builder_hub(mut self) -> Self {
        self.builder_hub_url = None;
        self
    }
}

/// Test that optional indexing args are validated correctly and match expected usage.
#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::OrderflowIngressArgs;

    #[test]
    fn indexing_args_optional_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-url",
            "0.0.0.0:9754",
            "--system-listen-url",
            "0.0.0.0:9755",
            "--builder-listen-url",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
        ];

        OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("optional indexing arg: {e}"));
    }

    #[test]
    fn indexing_args_partial_fail() {
        let args = vec![
            "test", // binary name
            "--user-listen-url",
            "0.0.0.0:9754",
            "--system-listen-url",
            "0.0.0.0:9755",
            "--builder-listen-url",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
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
    fn indexing_args_clickhouse_provided_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-url",
            "0.0.0.0:9754",
            "--system-listen-url",
            "0.0.0.0:9755",
            "--builder-listen-url",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--indexer.clickhouse.host",
            "http://127.0.0.1:12345",
            "--indexer.clickhouse.database",
            "pronto",
            "--indexer.clickhouse.password",
            "pronto",
            "--indexer.clickhouse.username",
            "pronto",
        ];

        OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("clickhouse indexing args are provided: {e}"));
    }

    #[test]
    fn indexing_args_parquet_provided_succeds() {
        let args = vec![
            "test", // binary name
            "--user-listen-url",
            "0.0.0.0:9754",
            "--system-listen-url",
            "0.0.0.0:9755",
            "--builder-listen-url",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
            "--indexer.parquet.bundle-receipts-file-path",
            "pronto.parquet",
        ];

        OrderflowIngressArgs::try_parse_from(args)
            .unwrap_or_else(|e| panic!("parquet indexing args are provided: {e}"));
    }

    #[test]
    fn indexing_args_provided_both_clickhouse_parquet_fails() {
        let args = vec![
            "test", // binary name
            "--user-listen-url",
            "0.0.0.0:9754",
            "--system-listen-url",
            "0.0.0.0:9755",
            "--builder-listen-url",
            "0.0.0.0:8756",
            "--builder-url",
            "http://0.0.0.0:2020",
            "--builder-hub-url",
            "http://localhost:3000",
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
        ];

        let err = OrderflowIngressArgs::try_parse_from(args).unwrap_err();
        assert!(err
            .to_string()
            .contains("the argument '--indexer.parquet.bundle-receipts-file-path <BUNDLE_RECEIPTS_FILE_PATH>' cannot be used with"), "Unexpected error: {err}");
    }
}

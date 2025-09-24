use alloy_signer_local::PrivateKeySigner;
use clap::{Parser, ValueHint};

/// The maximum request size in bytes (10 MiB).
const MAX_REQUEST_SIZE_BYTES: usize = 10 * 1024 * 1024;

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

    /// The URL of the local builder.
    #[clap(long, value_hint = ValueHint::Url)]
    pub builder_url: String,

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
}

impl Default for OrderflowIngressArgs {
    fn default() -> Self {
        Self {
            user_listen_url: String::from("127.0.0.1:0"),
            system_listen_url: String::from("127.0.0.1:0"),
            builder_listen_url: String::from("127.0.0.1:0"),
            builder_url: String::from("http://127.0.0.1:8545"),
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

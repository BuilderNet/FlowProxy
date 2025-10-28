//! The [`init_tracing`] function sets up tracing for the application.
//! [`init_otel_provider`] is also interesting :)
//!
//! Adapated from https://github.com/init4tech/teaching-tracing/blob/main/src/trace.rs.

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_tracing(log_json: bool) {
    let registry = tracing_subscriber::registry().with(
        EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy(),
    );
    if log_json {
        let _ = registry.with(tracing_subscriber::fmt::layer().json()).try_init();
    } else {
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();
    }
}

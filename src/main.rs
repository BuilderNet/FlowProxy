use clap::Parser;
use flowproxy::{
    cli::OrderflowIngressArgs,
    runner::{CliContext, CliRunner},
    trace::init_tracing,
};

#[cfg(all(feature = "jemalloc", unix))]
type AllocatorInner = tikv_jemallocator::Jemalloc;
#[cfg(not(all(feature = "jemalloc", unix)))]
type AllocatorInner = std::alloc::System;

/// Custom allocator.
pub(crate) type Allocator = AllocatorInner;

/// Creates a new [custom allocator][Allocator].
pub(crate) const fn new_allocator() -> Allocator {
    AllocatorInner {}
}

#[global_allocator]
static ALLOC: Allocator = new_allocator();

use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::LazyLock;

static OTEL_PROVIDER: LazyLock<SdkTracerProvider> = LazyLock::new(init_tracing);

fn main() {
    dotenvy::dotenv().ok();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create runtime");

    let runner = CliRunner::from_runtime(tokio_runtime);

    let command = |ctx: CliContext| flowproxy::run(OrderflowIngressArgs::parse(), ctx);

    if let Err(e) = runner.run_command_until_exit(command) {
        eprintln!("Orderflow proxy terminated with error: {e}");
    }
    let _ = OTEL_PROVIDER.force_flush();
}

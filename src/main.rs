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

fn main() {
    dotenvy::dotenv().ok();
    let args = OrderflowIngressArgs::parse();
    init_tracing(args.log_json);

    // TODO: Configure based on the number of CPU cores on the machine, CLI overrides.
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        // Limit to 4 I/O worker threads. Defaults to the number of CPU cores on the machine.
        .worker_threads(4)
        // Limit to 128 blocking threads. Defaults to 512, which seems high?
        // When blocking operations are requested, Tokio will spawn up to this many threads to
        // execute them.
        .max_blocking_threads(128)
        .enable_all()
        .build()
        .expect("failed to create runtime");

    let runner = CliRunner::from_runtime(tokio_runtime);

    let command = |ctx: CliContext| flowproxy::run(args, ctx);

    if let Err(e) = runner.run_command_until_exit(command) {
        eprintln!("Orderflow proxy terminated with error: {e}");
    }
}

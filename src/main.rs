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

    // Configure the Tokio runtime.
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        // Defaults to the number of CPU cores on the machine.
        .worker_threads(args.io_threads)
        .enable_all()
        .build()
        .expect("failed to create runtime");

    let runner = CliRunner::from_runtime(tokio_runtime);

    let command = |ctx: CliContext| flowproxy::run(args, ctx);

    if let Err(e) = runner.run_command_until_exit(command) {
        eprintln!("Orderflow proxy terminated with error: {e}");
    }
}

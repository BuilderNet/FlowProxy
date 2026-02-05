use std::time::Duration;

use clap::Parser;
use flowproxy::{
    cli::OrderflowIngressArgs,
    indexer::Indexer,
    trace::init_tracing,
    utils::{SHUTDOWN_TIMEOUT, wait_for_critical_tasks},
};
use rbuilder_utils::tasks::{PanickedTaskError, TaskManager};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

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
    let task_manager = TaskManager::new(tokio_runtime.handle().clone());
    info!("Main task started");

    // Executes the main task command until it finished or ctrl-c was fired.
    // IMPORTANT: flowproxy::run has no nice cancellation and will be stopped being polled abruptly
    // so it must not contain any critical tasks that need proper shutdown.
    tokio_runtime.block_on(run_with_shutdown(args, task_manager));

    info!("Main task finished. Shutting down tokio runtime");

    if let Err(error) = wait_tokio_runtime_shutdown(tokio_runtime, Duration::from_secs(5)) {
        error!(?error, "Flow proxy terminated with error");
    }
}

async fn run_with_shutdown(args: OrderflowIngressArgs, mut task_manager: TaskManager) {
    let task_executor = task_manager.executor();
    let (indexer_handle, indexer_join_handles) =
        Indexer::run(args.indexing.clone(), args.builder_name.clone(), task_executor.clone());
    let cancellation_token = CancellationToken::new();
    let main_task = flowproxy::run(args, task_executor, indexer_handle, cancellation_token.clone());
    match run_to_completion_or_panic(&mut task_manager, run_until_ctrl_c(main_task)).await {
        Ok(()) => {
            tracing::warn!(target = "cli", "shutting down gracefully");
        }
        Err(err) => {
            tracing::error!(?err, target = "cli", "shutting down due to error");
        }
    }
    // This kills some tasks launched by flowproxy::run that release the last references to the
    // indexer_handle.
    cancellation_token.cancel();
    // At this point all the rpc was abruptly dropped which dropped the indexer_handle and that will
    // allow the indexer core to process all pending data and start shutting down.
    wait_for_critical_tasks(indexer_join_handles, SHUTDOWN_TIMEOUT).await;
    // We already have a chance to critical tasks to finish by themselves, so we can now call the
    // graceful shutdown.
    task_manager.graceful_shutdown_with_timeout(SHUTDOWN_TIMEOUT);
}

fn wait_tokio_runtime_shutdown(
    tokio_runtime: tokio::runtime::Runtime,
    timeout: Duration,
) -> Result<(), std::sync::mpsc::RecvTimeoutError> {
    // `drop(tokio_runtime)` would block the current thread until its pools
    // (including blocking pool) are shutdown. Since we want to exit as soon as possible, drop
    // it on a separate thread and wait for up to 5 seconds for this operation to
    // complete.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("tokio-runtime-shutdown".to_string())
        .spawn(move || {
            drop(tokio_runtime);
            let _ = tx.send(());
        })
        .unwrap();

    rx.recv_timeout(timeout).inspect_err(|err| {
        tracing::debug!(target: "reth::cli", %err, "tokio runtime shutdown timed out");
    })
}

/// Runs the future to completion or until:
/// - `ctrl-c` is received.
/// - `SIGTERM` is received (unix only).
async fn run_until_ctrl_c<F, E>(fut: F) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
    E: Send + Sync + 'static + From<std::io::Error>,
{
    let ctrl_c = tokio::signal::ctrl_c();

    let mut stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let sigterm = stream.recv();
    let sigterm = Box::pin(sigterm);
    let ctrl_c = Box::pin(ctrl_c);
    let fut = Box::pin(fut);

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received ctrl-c");
        },
        _ = sigterm => {
            tracing::info!("Received SIGTERM");
        },
        res = fut => res?,
    }

    Ok(())
}

/// Runs the given future to completion or until a critical task panicked.
///
/// Returns the error if a task panicked, or the given future returned an error.
async fn run_to_completion_or_panic<F, E>(tasks: &mut TaskManager, fut: F) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
    E: Send + Sync + From<PanickedTaskError> + 'static,
{
    {
        let fut = Box::pin(fut);
        tokio::select! {
            task_manager_result = tasks => {
                if let Err(panicked_error) = task_manager_result {
                    return Err(panicked_error.into());
                }
            },
            res = fut => res?,
        }
    }
    Ok(())
}

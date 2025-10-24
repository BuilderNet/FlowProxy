//! Asynchronous task runner utilities.

use std::{future::Future, time::Duration};

use rbuilder_utils::tasks::{PanickedTaskError, TaskExecutor, TaskManager};

#[derive(Debug, Clone)]
pub struct CliContext {
    pub task_executor: TaskExecutor,
}

/// Executes CLI commands.
///
/// Provides utilities for running a cli command to completion.
#[derive(Debug)]
#[non_exhaustive]
pub struct CliRunner {
    tokio_runtime: tokio::runtime::Runtime,
}

impl CliRunner {
    /// Create a new [`CliRunner`] from a provided tokio [`Runtime`](tokio::runtime::Runtime).
    pub const fn from_runtime(tokio_runtime: tokio::runtime::Runtime) -> Self {
        Self { tokio_runtime }
    }
}

// === impl CliRunner ===

impl CliRunner {
    /// Executes the given _async_ command on the tokio runtime until the command future resolves or
    /// until the process receives a `SIGINT` or `SIGTERM` signal.
    ///
    /// Tasks spawned by the command via the [`TaskExecutor`] are shut down and an attempt is made
    /// to drive their shutdown to completion after the command has finished.
    pub fn run_command_until_exit<F, E>(
        self,
        command: impl FnOnce(CliContext) -> F,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<(), E>>,
        E: Send + Sync + From<std::io::Error> + From<PanickedTaskError> + 'static,
    {
        let tokio_runtime = self.tokio_runtime;
        let mut task_manager = TaskManager::new(tokio_runtime.handle().clone());
        let task_executor = task_manager.executor();
        let context = CliContext { task_executor };

        // Executes the command until it finished or ctrl-c was fired
        let command_res = tokio_runtime.block_on(run_to_completion_or_panic(
            &mut task_manager,
            run_until_ctrl_c(command(context)),
        ));

        if command_res.is_err() {
            tracing::error!(target: "cli", "shutting down due to error");
        } else {
            tracing::debug!(target: "cli", "shutting down gracefully");
            // after the command has finished or exit signal was received we shutdown the task
            // manager which fires the shutdown signal to all tasks spawned via the task
            // executor and awaiting on tasks spawned with graceful shutdown
            task_manager.graceful_shutdown_with_timeout(Duration::from_secs(5));
        }

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

        let _ = rx.recv_timeout(Duration::from_secs(5)).inspect_err(|err| {
            tracing::debug!(target: "reth::cli", %err, "tokio runtime shutdown timed out");
        });

        command_res
    }
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

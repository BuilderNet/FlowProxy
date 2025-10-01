use std::{ops::DerefMut, time::Duration};

use buildernet_orderflow_proxy::{
    cli::OrderflowIngressArgs,
    statics::{SHUTDOWN_TOKEN, TASKS},
};
use clap::Parser;
use futures::future::join_all;
use tokio::signal::unix::SignalKind;

#[tokio::main]
async fn main() {
    // When a task crashes, we stop the whole program.
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);

        SHUTDOWN_TOKEN.cancel();
    }));

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to install signal handler");

    tokio::select! {
        Err(error) = buildernet_orderflow_proxy::run(OrderflowIngressArgs::parse()) => {
            eprintln!("Error: {error:?}");
        }
        _ = sigterm.recv() => {
            tracing::warn!("Received SIGTERM, shutting down all tasks...");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("Received SIGINT, shutting down all tasks...");
        }
    }
    SHUTDOWN_TOKEN.cancel();

    cleanup().await;
}

/// Cleans up all long-lived tasks by awaiting their join handles.
async fn cleanup() {
    SHUTDOWN_TOKEN.cancelled().await;

    let tasks = std::mem::take(TASKS.write().expect("not poisoned").deref_mut());

    if tasks.is_empty() {
        return;
    }

    let Ok(results) = tokio::time::timeout(Duration::from_secs(5), join_all(tasks)).await else {
        eprintln!("Timed out waiting for tasks to shut down");
        return;
    };

    for result in results {
        if let Err(e) = result {
            eprintln!("Error while cleaning up task {}: {}", e.name, e.err);
        }
    }
}

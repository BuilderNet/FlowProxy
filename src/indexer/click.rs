//! Indexing functionality powered by Clickhouse.

use std::{fmt::Debug, time::Duration};

use alloy_primitives::B256;
use clickhouse::{
    inserter::{Inserter, Quantities},
    Client as ClickhouseClient, Row, RowWrite,
};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::info;

use crate::{
    cli::ClickhouseArgs,
    indexer::{
        models::{BundleRow, PrivateTxRow},
        BuilderName, OrderReceivers, BUNDLE_TABLE_NAME, TRACING_TARGET,
    },
    tasks::TaskExecutor,
    types::{SystemBundle, SystemTransaction},
};

/// An high-level order type that can be indexed in clickhouse.
pub(crate) trait ClickhouseIndexableOrder: Sized {
    /// The associated inner row type that can be serialized into Clickhouse data.
    type ClickhouseRowType: Row + RowWrite + Serialize + From<(Self, BuilderName)>;

    /// The type of such order, e.g. "bundles" or "transactions". For informational purposes.
    const ORDER_TYPE: &'static str;

    /// An identifier of such order.
    fn hash(&self) -> B256;

    /// Internal function that takes the inner row types and extracts the reference needed for
    /// Clickhouse inserter functions like `Inserter::write`. While a default implementation is not
    /// provided, it should suffice to simply return `row`.
    fn to_row_ref(row: &Self::ClickhouseRowType) -> &<Self::ClickhouseRowType as Row>::Value<'_>;
}

impl ClickhouseIndexableOrder for SystemBundle {
    type ClickhouseRowType = BundleRow;

    const ORDER_TYPE: &'static str = "bundle";

    fn hash(&self) -> B256 {
        self.bundle_hash
    }

    fn to_row_ref(row: &Self::ClickhouseRowType) -> &<Self::ClickhouseRowType as Row>::Value<'_> {
        row
    }
}

impl ClickhouseIndexableOrder for SystemTransaction {
    type ClickhouseRowType = PrivateTxRow;

    const ORDER_TYPE: &'static str = "transaction";

    fn hash(&self) -> B256 {
        self.tx_hash()
    }

    fn to_row_ref(row: &Self::ClickhouseRowType) -> &<Self::ClickhouseRowType as Row>::Value<'_> {
        row
    }
}

/// A namespace struct to spawn a Clickhouse indexer.
#[derive(Debug, Clone)]
pub(crate) struct ClickhouseIndexer;

impl ClickhouseIndexer {
    /// Create and spawn new Clickhouse indexer tasks, returning their indexer handle.
    pub(crate) fn run(
        args: ClickhouseArgs,
        builder_name: BuilderName,
        receivers: OrderReceivers,
        task_executor: TaskExecutor,
    ) {
        let (host, database, username, password) = (
            args.host.expect("host is set"),
            args.database.expect("database is set"),
            args.username.expect("username is set"),
            args.password.expect("password is set"),
        );

        info!(%host, "Running with clickhouse indexer");

        let OrderReceivers { bundle_rx, mut bundle_receipt_rx, transaction_rx } = receivers;

        let client = ClickhouseClient::default()
            .with_url(host)
            .with_database(database)
            .with_user(username)
            .with_password(password)
            // NOTE: Validation is disabled for performance reasons, and because validation doesn't
            // support Uint256 data types.
            .with_validation(false);

        let bundle_inserter = client
            .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
            .with_period(Some(Duration::from_secs(4))) // Dump every 4s
            .with_period_bias(0.1) // 4±(0.1*4)
            .with_max_bytes(128 * 1024 * 1024) // 128MiB
            .with_max_rows(65_536);
        let mut bundle_inserter_runner =
            InserterRunner::new(bundle_rx, bundle_inserter, builder_name.clone());

        let transaction_inserter = client
            .inserter::<PrivateTxRow>(BUNDLE_TABLE_NAME)
            .with_period(Some(Duration::from_secs(3))) // Dump every 3s
            .with_period_bias(0.1)
            .with_max_bytes(128 * 1024 * 1024) // 128MiB
            .with_max_rows(65_536);
        let mut transaction_inserter_runner =
            InserterRunner::new(transaction_rx, transaction_inserter, builder_name);

        task_executor.spawn_with_graceful_shutdown_signal(|shutdown| async move {
            let mut shutdown_guard = None;
            tokio::select! {
                _ = bundle_inserter_runner.run_loop() => {
                    info!(target: TRACING_TARGET, "clickhouse bundle indexer channel closed");
                }
                guard = shutdown => {
                    info!(target: TRACING_TARGET, "Received shutdown, performing cleanup");
                    shutdown_guard = Some(guard);
                },
            }
            if let Err(e) = bundle_inserter_runner.inserter.end().await {
                tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion of bundles to indexer");
            }
            drop(shutdown_guard);
        });

        task_executor.spawn_with_graceful_shutdown_signal(|shutdown| async move {
            let mut shutdown_guard = None;
            tokio::select! {
                _ = transaction_inserter_runner.run_loop() => {
                    info!(target: TRACING_TARGET, "clickhouse transaction indexer channel closed");
                }
                guard = shutdown => {
                    info!(target: TRACING_TARGET, "Received shutdown, performing cleanup");
                    shutdown_guard = Some(guard);
                },
            }
            if let Err(e) = transaction_inserter_runner.inserter.end().await {
                tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion of transactions to indexer");
            }
            drop(shutdown_guard);
        });

        // TODO: support bundle receipts in Clickhouse.
        task_executor.spawn(async move { while let Some(_b) = bundle_receipt_rx.recv().await {} });
    }
}

// fn spawn_indexer_task<T: ClickhouseIndexableOrder>(
//     mut inserter_runner: InserterRunner<T>,
//     task_executor: &TaskExecutor,
// ) {
//     task_executor.spawn_with_graceful_shutdown_signal(move |shutdown| async move {
//             let mut shutdown_guard = None;
//             tokio::select! {
//                 _ = inserter_runner.run_loop() => {
//                     info!(target: TRACING_TARGET, "clickhouse bundle indexer channel closed");
//                 }
//                 guard = shutdown => {
//                     info!(target: TRACING_TARGET, "Received shutdown, performing cleanup");
//                     shutdown_guard = Some(guard);
//                 },
//             }
//             if let Err(e) = inserter_runner.inserter.end().await {
//                 tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion of bundles to indexer");
//             }
//             drop(shutdown_guard);
//     });
// }

struct InserterRunner<T: ClickhouseIndexableOrder> {
    rx: mpsc::Receiver<T>,
    inserter: Inserter<T::ClickhouseRowType>,
    builder_name: String,
}

impl<T: ClickhouseIndexableOrder> InserterRunner<T> {
    fn new(
        rx: mpsc::Receiver<T>,
        inserter: Inserter<T::ClickhouseRowType>,
        builder_name: BuilderName,
    ) -> Self {
        Self { rx, inserter, builder_name }
    }

    async fn run_loop(&mut self) {
        while let Some(order) = self.rx.recv().await {
            tracing::trace!(target: TRACING_TARGET, hash = %order.hash(), "received {} to index", T::ORDER_TYPE);

            let hash = order.hash();
            let order_row: T::ClickhouseRowType = (order, self.builder_name.clone()).into();
            let value_ref = T::to_row_ref(&order_row);

            if let Err(e) = self.inserter.write(value_ref).await {
                tracing::error!(target: TRACING_TARGET,
                    ?e,
                    %hash,
                    "failed to write {} to clickhouse inserter", T::ORDER_TYPE
                )
            }

            // TODO(thedevbirb): current clickhouse code doesn't let me know if this calls
            // `force_commit` or not. It kinda sucks. I should fork it and make a PR
            // eventually.
            //
            // TODO(thedevbirb): implement a file-based backup in case this call fails due to
            // connection timeouts or whatever.
            match self.inserter.commit().await {
                Ok(quantities) => {
                    if quantities == Quantities::ZERO {
                        tracing::trace!(target: TRACING_TARGET, %hash, "committed {} to inserter", T::ORDER_TYPE);
                    } else {
                        tracing::debug!(target: TRACING_TARGET, ?quantities, "inserted batch of {}s to clickhouse", T::ORDER_TYPE)
                    }
                }
                Err(e) => {
                    tracing::error!(target: TRACING_TARGET, ?e, "failed to commit bundle of {}s to clickhouse", T::ORDER_TYPE)
                }
            }
        }
        tracing::error!(target: TRACING_TARGET, "{} tx channel closed, indexer will stop running", T::ORDER_TYPE);
    }
}

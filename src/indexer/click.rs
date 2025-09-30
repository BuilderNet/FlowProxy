//! Indexing functionality powered by Clickhouse.

use std::{fmt::Debug, time::Duration};

use alloy_primitives::B256;
use clickhouse::{
    inserter::{Inserter, Quantities},
    Client as ClickhouseClient, Row, RowWrite,
};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    cli::ClickhouseArgs,
    indexer::{
        models::{BundleRow, PrivateTxRow},
        BuilderName, OrderIndexerTasks, OrderReceivers, BUNDLE_TABLE_NAME, TRACING_TARGET,
    },
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
    pub(crate) fn spawn(
        args: ClickhouseArgs,
        builder_name: BuilderName,
        receivers: OrderReceivers,
        token: CancellationToken,
    ) -> OrderIndexerTasks {
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

        let transaction_inserter = client
            .inserter::<PrivateTxRow>(BUNDLE_TABLE_NAME)
            .with_period(Some(Duration::from_secs(3))) // Dump every 3s
            .with_period_bias(0.1)
            .with_max_bytes(128 * 1024 * 1024) // 128MiB
            .with_max_rows(65_536);

        let bundle_indexer_task = tokio::spawn(run_indexer(
            bundle_rx,
            bundle_inserter,
            builder_name.clone(),
            token.child_token(),
        ));
        // TODO: support bundle receipts in Clickhouse.
        let bundle_receipt_indexer_task =
            tokio::task::spawn(
                async move { while let Some(_b) = bundle_receipt_rx.recv().await {} },
            );
        let transaction_indexer_task = tokio::spawn(run_indexer(
            transaction_rx,
            transaction_inserter,
            builder_name.clone(),
            token,
        ));

        OrderIndexerTasks {
            bundle_indexer_task,
            bundle_receipt_indexer_task,
            transaction_indexer_task,
        }
    }
}

/// Run the indexer of the specified type until the receiving channel is closed.
async fn run_indexer<T: ClickhouseIndexableOrder>(
    mut rx: mpsc::Receiver<T>,
    mut inserter: Inserter<T::ClickhouseRowType>,
    builder_name: BuilderName,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            maybe_order = rx.recv() => {
                let Some(order) = maybe_order else {
                    tracing::error!(target: TRACING_TARGET, "{} tx channel closed, indexer will stop running", T::ORDER_TYPE);
                    break;
                };
                tracing::trace!(target: TRACING_TARGET, hash = %order.hash(), "received {} to index", T::ORDER_TYPE);

                let hash = order.hash();
                let order_row: T::ClickhouseRowType = (order, builder_name.clone()).into();
                let value_ref = T::to_row_ref(&order_row);

                if let Err(e) = inserter.write(value_ref).await {
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
                match inserter.commit().await {
                    Ok(quantities) => {
                        if quantities == Quantities::ZERO {
                            tracing::trace!(target: TRACING_TARGET, %hash, "committed {} to inserter", T::ORDER_TYPE);
                        } else {
                            tracing::info!(target: TRACING_TARGET, ?quantities, "inserted batch of {}s to clickhouse", T::ORDER_TYPE)
                        }
                    }
                    Err(e) => {
                        tracing::error!(target: TRACING_TARGET, ?e, "failed to commit bundle of {}s to clickhouse", T::ORDER_TYPE)
                    }
                }
            },
            _ = token.cancelled() => {
                tracing::info!(target: TRACING_TARGET, "Received shutdown message");
                break;
            }
        }
    }

    if let Err(e) = inserter.end().await {
        tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion of {}s to indexer", T::ORDER_TYPE);
    }
}

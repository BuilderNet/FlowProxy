//! Crate which contains structures and logic for indexing bundles and other types of data in to
//! Clickhouse.

use std::fmt::Debug;

use tokio::sync::mpsc;

use crate::{
    cli::IndexerArgs,
    indexer::{click::ClickhouseIndexer, parq::ParquetIndexer},
    tasks::TaskExecutor,
    types::{BundleReceipt, SystemBundle, SystemTransaction},
};

mod click;
mod models;
mod parq;
mod ser;

/// The size of the channel buffer for the bundle indexer.
pub const BUNDLE_INDEXER_BUFFER_SIZE: usize = 4096;
/// The size of the channel buffer for the bundle receipt indexer.
pub const BUNDLE_RECEIPT_INDEXER_BUFFER_SIZE: usize = 8192;
/// The size of the channel buffer for the bundle indexer.
pub const TRANSACTION_INDEXER_BUFFER_SIZE: usize = 4096;
/// The name of the Clickhouse table to store bundles in.
pub const BUNDLE_TABLE_NAME: &str = "bundles";
/// The name of the Clickhouse table to store transactions in.
pub const TRANSACTIONS_TABLE_NAME: &str = "transactions";
/// The tracing target for this indexer crate.
const TRACING_TARGET: &str = "indexer";

/// A simple alias to refer to a builder name.
pub(crate) type BuilderName = String;

/// Trait for adding order indexing functionality.
pub trait OrderIndexer: Sync + Send {
    fn index_bundle(&self, system_bundle: SystemBundle);
    fn index_bundle_receipt(&self, bundle_receipt: BundleReceipt);
    fn index_transaction(&self, system_transaction: SystemTransaction);
}

/// The collection of channel senders to send data to be indexed.
#[derive(Debug, Clone)]
pub(crate) struct OrderSenders {
    bundle_tx: mpsc::Sender<SystemBundle>,
    bundle_receipt_tx: mpsc::Sender<BundleReceipt>,
    transaction_tx: mpsc::Sender<SystemTransaction>,
}

/// The collection of channel receivers to receive data to be indexed.
#[derive(Debug)]
pub(crate) struct OrderReceivers {
    bundle_rx: mpsc::Receiver<SystemBundle>,
    bundle_receipt_rx: mpsc::Receiver<BundleReceipt>,
    transaction_rx: mpsc::Receiver<SystemTransaction>,
}

impl OrderSenders {
    /// Creates a new set of order indexer channel senders and receivers.
    pub(crate) fn new() -> (Self, OrderReceivers) {
        let (bundle_tx, bundle_rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let (bundle_receipt_tx, bundle_receipt_rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let (transaction_tx, transaction_rx) = mpsc::channel(TRANSACTION_INDEXER_BUFFER_SIZE);
        let senders = Self { bundle_tx, bundle_receipt_tx, transaction_tx };
        let receivers = OrderReceivers { bundle_rx, bundle_receipt_rx, transaction_rx };
        (senders, receivers)
    }
}

/// A namespace struct for spawning an indexer.
#[derive(Debug, Clone)]
pub struct Indexer;

impl Indexer {
    pub fn run(
        args: IndexerArgs,
        builder_name: BuilderName,
        task_executor: TaskExecutor,
    ) -> IndexerHandle {
        let (senders, receivers) = OrderSenders::new();

        match (args.clickhouse, args.parquet) {
            (None, None) => {
                MockIndexer.run(receivers, task_executor);
                IndexerHandle { senders }
            }
            (Some(clickhouse), None) => {
                let validation = false;
                ClickhouseIndexer::run(
                    clickhouse,
                    builder_name,
                    receivers,
                    task_executor,
                    validation,
                );
                IndexerHandle { senders }
            }
            (None, Some(parquet)) => {
                ParquetIndexer::run(parquet, builder_name, receivers, task_executor)
                    .expect("failed to start parquet indexer");
                IndexerHandle { senders }
            }
            (Some(_), Some(_)) => {
                unreachable!("Cannot specify both clickhouse and parquet indexer");
            }
        }
    }
}

/// An handle to the indexer, which mainly consists of channel senders to send data to be indexed.
#[derive(Debug)]
pub struct IndexerHandle {
    senders: OrderSenders,
}

impl OrderIndexer for IndexerHandle {
    fn index_bundle(&self, system_bundle: SystemBundle) {
        if let Err(e) = self.senders.bundle_tx.try_send(system_bundle) {
            tracing::error!(?e, "failed to send bundle to index");
        }
    }
    fn index_bundle_receipt(&self, bundle_receipt: BundleReceipt) {
        if let Err(e) = self.senders.bundle_receipt_tx.try_send(bundle_receipt) {
            tracing::error!(?e, "failed to send bundle receipt to index");
        }
    }
    fn index_transaction(&self, system_transaction: SystemTransaction) {
        if let Err(e) = self.senders.transaction_tx.try_send(system_transaction) {
            tracing::error!(?e, "failed to send transaction to index");
        }
    }
}

/// A mock indexer that simply drains the channels.
struct MockIndexer;

impl MockIndexer {
    fn run(self, receivers: OrderReceivers, task_executor: TaskExecutor) {
        tracing::info!(target: TRACING_TARGET, "Running with mocked indexer");

        let OrderReceivers { mut bundle_rx, mut bundle_receipt_rx, mut transaction_rx } = receivers;

        task_executor.spawn(async move { while let Some(_b) = bundle_rx.recv().await {} });
        task_executor.spawn(async move { while let Some(_b) = bundle_receipt_rx.recv().await {} });
        task_executor.spawn(async move { while let Some(_t) = transaction_rx.recv().await {} });
    }
}
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use alloy_consensus::transaction::SignerRecoverable;
    use rbuilder_primitives::serialize::RawBundle;
    use time::UtcDateTime;

    use crate::types::{decode_transaction, EthereumTransaction, SystemBundle, SystemTransaction};

    /// An example raw bundle in JSON format to use for testing. The transactions are from a real
    /// bundle, along with the block number set to zero. The rest is to mainly populate some
    /// fields.
    const TEST_BUNDLE: &str = r#"{
    "txs": [
        "0x02f89201820132850826299e00850826299e0082a1d694f82300c34f0d11b0420ac3ce85f0ebe4e3e0544280a40d2959800000000000000000000000000000000000000000000000000000000000000001c001a0030c9637d6d442bd2f9a43f69ec09dbaedb12e08ef1fe38ae5ce855bbcfc36ada064101cb4c00d6c21e792a2959c896992c9bbf8e2d84ce7647d561bfbcf59365a",
        "0x02f9019901458405f5e10085084f73d22e8304d3819480a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e870aa87bee538000b90124d1ef924900000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000038d7ea4c6800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006670d0d6c4985c3948000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000f82300c34f0d11b0420ac3ce85f0ebe4e3e05442c080a0b881ab734863b49369bfbf2e28c035785276423783dbbb8e74ec813fd3a95614a0499f781a62cdf3d9a235b83e050f6e8f74df15fcad09822c78a84ace5a44a59b",
        "0x02f9019901058405f5e10085084f73d22e8304d4269480a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e87071afd498d0000b90124d1ef924900000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000038d7ea4c6800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006670d0d6c4985c394800000000000000000000000000000000000000000000000100f8061414d648d750000000000000000000000005c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000f82300c34f0d11b0420ac3ce85f0ebe4e3e05442c080a0b9272dad54e7f289385ba64368db5de0a96bd9364734d2edf989307c93cd3982a01b076c83c0bb7d2c644568255d9878ce66c7552175df58025d670a9cec310546"
    ],
    "blockNumber": "0x0",
    "droppingTxHashes": ["0x0000000000000000000000000000000000000000000000000000000000000000"],
    "minTimestamp": 0,
    "maxTimestamp": 0,
    "replacementNonce": 0,
    "refundPercent": 0,
    "refundRecipient": "0x0000000000000000000000000000000000000000",
    "refundTxHashes": ["0x0000000000000000000000000000000000000000000000000000000000000000"]
}"#;

    /// An example replacement bundle with no transactions, a.k.a. a cancel bundle.
    ///
    /// NOTE: we populate refndTxHashes with an empty hash to satisfy the schema, which doesn't
    /// accept `Nullable(Array(String))`.
    const TEST_CANCEL_BUNDLE: &str = r#"{
    "txs": [],
    "replacementUuid": "bcf24b5c-a8f8-4174-a1ad-f7521f3bd70c",
    "replacementNonce": 1,
    "signingAddress": "0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c",
    "refundTxHashes": []
}"#;

    /// An example system bundle to use for testing.
    pub(crate) fn system_bundle_example() -> SystemBundle {
        let bundle = serde_json::from_str::<RawBundle>(TEST_BUNDLE).unwrap();
        let signer = alloy_primitives::address!("0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c");
        let received_at = UtcDateTime::now();
        SystemBundle::try_from_bundle_and_signer(bundle, signer, received_at).unwrap()
    }

    /// An example cancel bundle to use for testing.
    pub(crate) fn system_cancel_bundle_example() -> SystemBundle {
        let bundle = serde_json::from_str::<RawBundle>(TEST_CANCEL_BUNDLE).unwrap();
        let signer = alloy_primitives::address!("0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c");
        let received_at = UtcDateTime::now();
        SystemBundle::try_from_bundle_and_signer(bundle, signer, received_at).unwrap()
    }

    pub(crate) fn system_transaction_example() -> SystemTransaction {
        let bytes = alloy_primitives::Bytes::from(alloy_primitives::hex!("02f89201820132850826299e00850826299e0082a1d694f82300c34f0d11b0420ac3ce85f0ebe4e3e0544280a40d2959800000000000000000000000000000000000000000000000000000000000000001c001a0030c9637d6d442bd2f9a43f69ec09dbaedb12e08ef1fe38ae5ce855bbcfc36ada064101cb4c00d6c21e792a2959c896992c9bbf8e2d84ce7647d561bfbcf59365a"));
        let decoded = decode_transaction(&bytes).unwrap();
        let transaction = Arc::new(EthereumTransaction::new(decoded, bytes));
        let signer = transaction.recover_signer().unwrap();
        let received_at = UtcDateTime::now();
        SystemTransaction { transaction, signer, received_at }
    }
}

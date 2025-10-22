//! Crate which contains structures and logic for indexing bundles and other types of data in to
//! Clickhouse.

use std::fmt::Debug;

use tokio::sync::mpsc;

use crate::{
    cli::IndexerArgs,
    indexer::{click::ClickhouseIndexer, parq::ParquetIndexer},
    metrics::IndexerMetrics,
    primitives::{BundleReceipt, SystemBundle},
    tasks::TaskExecutor,
};

pub(crate) mod click;
mod parq;
mod ser;

/// The size of the channel buffer for the bundle indexer.
pub const BUNDLE_INDEXER_BUFFER_SIZE: usize = 12384;

/// The size of the channel buffer for the bundle receipt indexer.
pub const BUNDLE_RECEIPT_INDEXER_BUFFER_SIZE: usize = 8192;

/// The size of the channel buffer for the bundle indexer.
pub const TRANSACTION_INDEXER_BUFFER_SIZE: usize = 4096;

/// The name of the Clickhouse table to store bundles in.
pub const BUNDLE_TABLE_NAME: &str = "bundles";

/// The name of the Clickhouse table to store bundle receipts in.
pub const BUNDLE_RECEIPTS_TABLE_NAME: &str = "bundle_receipts";

/// The name of the Clickhouse table to store transactions in.
pub const TRANSACTIONS_TABLE_NAME: &str = "transactions";

/// The path of the backup database for storing failed Clickhouse batch insertions
pub const BACKUP_DATABASE_PATH: &str = "/var/lib/buildernet-of-proxy/clickhouse-backup.db";

/// The tracing target for this indexer crate.
const TARGET: &str = "indexer";

/// Trait for adding order indexing functionality.
pub trait OrderIndexer: Sync + Send {
    fn index_bundle(&self, system_bundle: SystemBundle);
    fn index_bundle_receipt(&self, bundle_receipt: BundleReceipt);
}

/// The collection of channel senders to send data to be indexed.
#[derive(Debug, Clone)]
pub(crate) struct OrderSenders {
    bundle_tx: mpsc::Sender<SystemBundle>,
    bundle_receipt_tx: mpsc::Sender<BundleReceipt>,
}

/// The collection of channel receivers to receive data to be indexed.
#[derive(Debug)]
pub(crate) struct OrderReceivers {
    bundle_rx: mpsc::Receiver<SystemBundle>,
    bundle_receipt_rx: mpsc::Receiver<BundleReceipt>,
}

impl OrderSenders {
    /// Creates a new set of order indexer channel senders and receivers.
    pub(crate) fn new() -> (Self, OrderReceivers) {
        let (bundle_tx, bundle_rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let (bundle_receipt_tx, bundle_receipt_rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let senders = Self { bundle_tx, bundle_receipt_tx };
        let receivers = OrderReceivers { bundle_rx, bundle_receipt_rx };
        (senders, receivers)
    }
}

/// A namespace struct for spawning an indexer.
#[derive(Debug, Clone)]
pub struct Indexer;

impl Indexer {
    pub fn run(
        args: IndexerArgs,
        builder_name: String,
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
            match e {
                mpsc::error::TrySendError::Full(bundle) => {
                    tracing::error!(target: TARGET, bundle_hash = ?bundle.bundle_hash(), "CRITICAL: Failed to send bundle to index, channel is full");
                    IndexerMetrics::increment_bundle_indexing_failures("Full");
                }
                mpsc::error::TrySendError::Closed(_) => {
                    tracing::error!(target: TARGET, "CRITICAL: Failed to send bundle to index, indexer task is closed");
                    IndexerMetrics::increment_bundle_indexing_failures("Closed");
                }
            }
        }
    }

    fn index_bundle_receipt(&self, bundle_receipt: BundleReceipt) {
        if let Err(e) = self.senders.bundle_receipt_tx.try_send(bundle_receipt) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    tracing::error!(target: TARGET,  "Failed to send bundle receipt to index, channel is full");
                    IndexerMetrics::increment_bundle_receipt_indexing_failures("Full");
                }
                mpsc::error::TrySendError::Closed(_) => {
                    tracing::error!(target: TARGET, "CRITICAL: Failed to send bundle receipt to index, indexer task is closed");
                    IndexerMetrics::increment_bundle_receipt_indexing_failures("Closed");
                }
            }
        }
    }
}

/// A mock indexer that simply drains the channels.
struct MockIndexer;

impl MockIndexer {
    fn run(self, receivers: OrderReceivers, task_executor: TaskExecutor) {
        tracing::info!(target: TARGET, "Running with mocked indexer");

        let OrderReceivers { mut bundle_rx, mut bundle_receipt_rx } = receivers;
        task_executor.spawn(async move { while let Some(_b) = bundle_rx.recv().await {} });
        task_executor.spawn(async move { while let Some(_b) = bundle_receipt_rx.recv().await {} });
    }
}
#[cfg(test)]
pub(crate) mod tests {
    use rbuilder_primitives::serialize::RawBundle;
    use revm_primitives::B256;
    use time::UtcDateTime;

    use crate::{
        primitives::{
            BundleReceipt, SystemBundle, SystemBundleDecoder, SystemBundleMetadata, UtcInstant,
        },
        priority::Priority,
    };

    trait UtcDateTimeExt {
        fn now_ms() -> Self;
    }

    impl UtcDateTimeExt for UtcDateTime {
        fn now_ms() -> Self {
            let now = UtcDateTime::now();
            now.replace_millisecond(now.millisecond()).unwrap()
        }
    }

    /// An example raw bundle in JSON format to use for testing. The transactions are from a real
    /// bundle, along with the block number set to zero. The rest is to mainly populate some
    /// fields.
    const TEST_BUNDLE: &str = r#"{
    "version": "v2",
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
    "refundTxHashes": ["0x0000000000000000000000000000000000000000000000000000000000000000"],
    "refund_identity": "0x0000000000000000000000000000000000000000",
    "delayed_refund": "true"
}"#;

    /// An example replacement bundle with no transactions, a.k.a. a cancel bundle.
    ///
    /// NOTE: we populate refndTxHashes with an empty hash to satisfy the schema, which doesn't
    /// accept `Nullable(Array(String))`.
    ///
    /// NOTE: adding block number just to satisfy round trip conversion logic, since on DB we
    /// always add the block number.
    const TEST_CANCEL_BUNDLE: &str = r#"{
    "version": "v2",
    "txs": [],
    "replacementUuid": "bcf24b5c-a8f8-4174-a1ad-f7521f3bd70c",
    "replacementNonce": 1,
    "signingAddress": "0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c",
    "refundTxHashes": [],
    "blockNumber": "0x0"
}"#;

    /// An example system bundle to use for testing.
    pub(crate) fn system_bundle_example() -> SystemBundle {
        let bundle = serde_json::from_str::<RawBundle>(TEST_BUNDLE).unwrap();
        let signer = alloy_primitives::address!("0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c");
        let received_at = UtcInstant::now();

        let metadata = SystemBundleMetadata { signer, received_at, priority: Priority::Medium };

        let decoder = SystemBundleDecoder::default();
        decoder.try_decode(bundle, metadata).unwrap()
    }

    /// An example cancel bundle to use for testing.
    pub(crate) fn system_cancel_bundle_example() -> SystemBundle {
        let bundle = serde_json::from_str::<RawBundle>(TEST_CANCEL_BUNDLE).unwrap();
        let signer = alloy_primitives::address!("0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c");
        let received_at = UtcInstant::now();

        let metadata = SystemBundleMetadata { signer, received_at, priority: Priority::Medium };
        let decoder = SystemBundleDecoder::default();
        decoder.try_decode(bundle, metadata).unwrap()
    }

    pub(crate) fn bundle_receipt_example() -> BundleReceipt {
        BundleReceipt {
            bundle_hash: B256::random(),
            sent_at: Some(UtcDateTime::now_ms()),
            received_at: UtcDateTime::now_ms(),
            src_builder_name: "buildernet".to_string(),
            payload_size: 256,
            priority: Priority::Medium,
        }
    }
}

//! Crate which contains structures and logic for indexing bundles and other types of data in to
//! Clickhouse.

use std::{fmt::Debug, time::Duration};

use clickhouse::{
    inserter::{Inserter, Quantities},
    Client as ClickhouseClient,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::info;

use crate::{cli::ClickhouseArgs, indexer::models::BundleRow, types::SystemBundle};

mod models;
mod serde;

/// The size of the channel buffer for the bundle indexer.
pub const BUNDLE_INDEXER_BUFFER_SIZE: usize = 4096;
/// The name of the Clickhouse table to store bundles in.
pub const BUNDLE_TABLE_NAME: &str = "bundles";
/// The tracing target for this indexer crate.
const TRACING_TARGET: &str = "indexer";

/// Trait for adding bundle indexing functionality.
pub trait BundleIndexer: Sync + Send {
    fn index_bundle(&self, system_bundle: SystemBundle);
}

/// An handle to the indexer, which mainly consists of channel senders to send data to be indexed.
#[derive(Debug)]
pub struct IndexerHandle {
    bundle_tx: mpsc::Sender<SystemBundle>,
}

/// The Clickhouse indexer implementation.
pub struct ClickhouseIndexer {
    /// The underlying Clickhouse client, used to create the Clickhouse `Inserter`.
    #[allow(dead_code)]
    pub(crate) client: ClickhouseClient,
    /// A Clickhouse inserter which supports batching and periodic commits upon configurable
    /// conditions.
    pub(crate) bundle_inserter: Inserter<BundleRow>,
    /// The receiver half of the channel to receive bundles to index.
    pub(crate) bundle_rx: mpsc::Receiver<SystemBundle>,
    /// The name of the local builder, to store in the `builder_name` column.
    pub(crate) builder_name: String,
}

impl Debug for ClickhouseIndexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseIndexer")
            .field("client", &"ClickhouseClient")
            .field("bundle_rx", &self.bundle_rx)
            .field("bundle_inserter", &"Inserter")
            .field("builder_name", &self.builder_name)
            .finish()
    }
}

impl ClickhouseIndexer {
    /// Create and spawn a new Clickhouse indexer task, returning the indexer handle and its
    /// task handle.
    pub fn spawn(
        args: Option<ClickhouseArgs>,
        builder_name: String,
    ) -> (IndexerHandle, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let handle = IndexerHandle { bundle_tx: tx };

        let Some(args) = args else {
            info!("Running with mocked indexer");
            let mut indexer = MockIndexer { bundle_rx: rx };
            let task_handle = tokio::task::spawn(async move {
                while let Some(_b) = indexer.bundle_rx.recv().await {}
            });
            return (handle, task_handle);
        };

        info!(host = %args.host, "Running with clickhouse indexer");

        let client = ClickhouseClient::default()
            .with_url(args.host)
            .with_database(args.database)
            .with_user(args.username)
            .with_password(args.password)
            .with_validation(true);

        let bundle_inserter = client
            .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
            .with_period(Some(Duration::from_secs(4))) // Dump every 4s
            .with_period_bias(0.1) // 4±(0.1*4)
            .with_max_bytes(128 * 1024 * 1024) // 128MiB
            .with_max_rows(65_536);

        let indexer = ClickhouseIndexer { bundle_rx: rx, client, bundle_inserter, builder_name };
        let task_handle = tokio::spawn(indexer.run());

        (handle, task_handle)
    }

    /// Run the indexer until the receiving channel is closed.
    async fn run(mut self) {
        while let Some(system_bundle) = self.bundle_rx.recv().await {
            tracing::trace!(target: TRACING_TARGET, hash = %system_bundle.bundle_hash, "received bundle to index");

            let bundle_row = (system_bundle, self.builder_name.clone()).into();

            if let Err(e) = self.bundle_inserter.write(&bundle_row).await {
                tracing::error!(target: TRACING_TARGET,
                    ?e,
                    bundle_hash = bundle_row.hash,
                    "failed to write bundle to clickhouse inserter"
                )
            }

            // TODO(thedevbirb): current clickhouse code doesn't let me know if this calls
            // `force_commit` or not. It kinda sucks. I should fork it and make a PR
            // eventually.
            //
            // TODO(thedevbirb): implement a file-based backup in case this call fails due to
            // connection timeouts or whatever.
            match self.bundle_inserter.commit().await {
                Ok(quantities) => {
                    if quantities == Quantities::ZERO {
                        tracing::trace!(target: TRACING_TARGET, hash = %bundle_row.hash, "committed bundle to inserter");
                    } else {
                        tracing::info!(target: TRACING_TARGET, ?quantities, "inserted batch to clickhouse")
                    }
                }
                Err(e) => {
                    tracing::error!(target: TRACING_TARGET, ?e, "failed to commit bundle to clickhouse")
                }
            }
        }

        tracing::error!(target: TRACING_TARGET, "bundle tx channel closed, indexer will stop running");
        if let Err(e) = self.bundle_inserter.end().await {
            tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion to indexer");
        }
    }
}

impl BundleIndexer for IndexerHandle {
    fn index_bundle(&self, system_bundle: SystemBundle) {
        if let Err(e) = self.bundle_tx.try_send(system_bundle) {
            tracing::error!(?e, "failed to send bundle to index");
        }
    }
}

/// A mock indexer which just receives bundles and does nothing with them. Useful for testing or
/// for running without an indexer.
#[derive(Debug)]
pub struct MockIndexer {
    pub bundle_rx: mpsc::Receiver<SystemBundle>,
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs;

    use clickhouse::{
        error::Result as ClickhouseResult, test::handlers::RecordDdlControl,
        Client as ClickhouseClient,
    };
    use derive_more::{Deref, DerefMut, From, Into};
    use rbuilder_primitives::serialize::RawBundle;
    use time::UtcDateTime;
    use tokio::sync::mpsc;

    use crate::{
        indexer::{
            models::BundleRow, BundleIndexer, ClickhouseIndexer, IndexerHandle,
            BUNDLE_INDEXER_BUFFER_SIZE, BUNDLE_TABLE_NAME,
        },
        types::SystemBundle,
    };

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

    #[derive(From, Into, Deref, DerefMut)]
    struct MockClickhouseClient {
        #[deref]
        #[deref_mut]
        inner: ClickhouseClient,
        mock: clickhouse::test::Mock,
    }

    impl MockClickhouseClient {
        fn new() -> Self {
            let mock = clickhouse::test::Mock::new();
            let inner = ClickhouseClient::default().with_mock(&mock).with_validation(true);
            Self { inner, mock }
        }

        async fn create_bundles_table(&self) -> ClickhouseResult<RecordDdlControl> {
            let recording = self.mock.add(clickhouse::test::handlers::record_ddl());
            let create_bundles_table_ddl =
                fs::read_to_string("./fixtures/create_bundles_table.sql")
                    .expect("could not read create_bundles_table.sql");
            self.query(&create_bundles_table_ddl).execute().await?;
            Ok(recording)
        }
    }

    #[allow(dead_code)]
    impl ClickhouseIndexer {
        async fn receive_one(&mut self) {
            let Some(system_bundle) = self.bundle_rx.recv().await else {
                panic!("Expected to receive a bundle")
            };
            let bundle_row = (system_bundle, self.builder_name.clone()).into();

            self.bundle_inserter.write(&bundle_row).await.unwrap();
            let _ = self.bundle_inserter.commit().await;
        }
    }

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

    // NOTE: when working with the mocked clickhouse client, every request must have a
    // corresponding handler before it's executed, otherwise it will panic.
    //
    // This is an example of how such handlers are created and used: <https://github.com/ClickHouse/clickhouse-rs/blob/main/examples/mock.rs>

    #[tokio::test]
    async fn clickhouse_bundles_table_create_table_succeds() {
        let client = MockClickhouseClient::new();
        let recording = client.create_bundles_table().await.unwrap();
        assert!(recording.query().await.contains("CREATE TABLE"));
    }

    /// Adapated from <https://github.com/ClickHouse/clickhouse-rs/blob/v0.13.3/examples/mock.rs>
    #[tokio::test]
    async fn clickhouse_bundles_insert_single_row_succeeds() {
        // Scaffolding

        let client = MockClickhouseClient::new();
        client.create_bundles_table().await.unwrap();

        let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let handle = IndexerHandle { bundle_tx: tx };
        let bundle_inserter = client.inserter::<BundleRow>(BUNDLE_TABLE_NAME).with_max_rows(0); // force commit immediately

        let indexer = ClickhouseIndexer {
            bundle_rx: rx,
            client: client.inner.clone(),
            bundle_inserter,
            builder_name: "buildernet".to_string(),
        };

        let system_bundle = system_bundle_example();

        let expected_bundle_rows =
            vec![BundleRow::from((system_bundle.clone(), indexer.builder_name.clone()))];

        // Test logic

        // Add handler for the next INSERT request.
        let recording = client.mock.add(clickhouse::test::handlers::record());

        tokio::spawn(indexer.run());
        handle.index_bundle(system_bundle);

        let recording_rows: Vec<BundleRow> = recording.collect().await;
        assert_eq!(expected_bundle_rows, recording_rows);
    }

    /// Adapated from <https://github.com/ClickHouse/clickhouse-rs/blob/v0.13.3/examples/mock.rs>
    #[tokio::test]
    async fn clickhouse_bundles_insert_single_cancel_bundle_row_succeeds() {
        // Scaffolding

        let client = MockClickhouseClient::new();
        client.create_bundles_table().await.unwrap();

        let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let handle = IndexerHandle { bundle_tx: tx };
        let bundle_inserter = client.inserter::<BundleRow>(BUNDLE_TABLE_NAME).with_max_rows(0); // force commit immediately

        let indexer = ClickhouseIndexer {
            bundle_rx: rx,
            client: client.inner.clone(),
            bundle_inserter,
            builder_name: "buildernet".to_string(),
        };

        let system_bundle = system_cancel_bundle_example();

        let expected_bundle_rows =
            vec![BundleRow::from((system_bundle.clone(), indexer.builder_name.clone()))];

        // Test logic

        // Add handler for the next INSERT request.
        let recording = client.mock.add(clickhouse::test::handlers::record());

        tokio::spawn(indexer.run());
        handle.index_bundle(system_bundle);

        let recording_rows: Vec<BundleRow> = recording.collect().await;
        assert_eq!(expected_bundle_rows, recording_rows);
    }
}

//! Clickhouse indexer of bundles.

use std::time::Duration;

use clickhouse::{inserter::Inserter, Client as ClickhouseClient};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::info;

use crate::{cli::ClickhouseArgs, indexer::models::BundleRow, types::IndexableSystemBundle};

mod models;

pub const BUNDLE_INDEXER_BUFFER_SIZE: usize = 4096;
pub const BUNDLE_TABLE_NAME: &str = "bundles";
const TRACING_TARGET: &str = "indexer";

pub trait BundleIndexer: Sync + Send {
    fn index_bundle(&self, indexable_bundle: IndexableSystemBundle);
}

#[derive(Debug)]
pub struct ClickhouseIndexerHandle {
    bundle_tx: mpsc::Sender<IndexableSystemBundle>,
}

struct ClickhouseIndexer {
    #[allow(dead_code)]
    pub client: ClickhouseClient,
    pub bundle_rx: mpsc::Receiver<IndexableSystemBundle>,
    pub bundle_inserter: Inserter<BundleRow>,
}

impl ClickhouseIndexer {
    async fn run(mut self) {
        while let Some(indexable_bundle) = self.bundle_rx.recv().await {
            let bundle_row = indexable_bundle.into();

            if let Err(e) = self.bundle_inserter.write(&bundle_row) {
                tracing::error!(target: TRACING_TARGET,
                    ?e,
                    bundle_hash = bundle_row.hash,
                    "failed to write bundle to clickhouse inserter"
                )
            }

            // TODO(thedevbirb): current clickhouse code doesn't let me know if this calls `force_commit` or
            // not. It kinda sucks. I should fork it and make a PR eventually.
            //
            // TODO(thedevbirb): implement a file-based backup in case this call fails due to connection
            // timeouts or whatever.
            let _ = self.bundle_inserter.commit().await;
        }

        tracing::error!(target: TRACING_TARGET, "bundle tx channel closed, indexer will stop running");
        if let Err(e) = self.bundle_inserter.end().await {
            tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion to indexer");
        }
    }
}

impl BundleIndexer for ClickhouseIndexerHandle {
    fn index_bundle(&self, indexable_bundle: IndexableSystemBundle) {
        if let Err(e) = self.bundle_tx.try_send(indexable_bundle) {
            tracing::error!(?e, "failed to send bundle to index");
        }
    }
}

#[derive(Debug)]
pub struct MockIndexer {
    pub bundle_rx: mpsc::Receiver<IndexableSystemBundle>,
}

pub fn spawn_indexer(args: Option<ClickhouseArgs>) -> (ClickhouseIndexerHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
    let handle = ClickhouseIndexerHandle { bundle_tx: tx };

    let Some(args) = args else {
        info!("Running with mocked indexer");
        let mut indexer = MockIndexer { bundle_rx: rx };
        let task_handle =
            tokio::task::spawn(
                async move { while let Some(_b) = indexer.bundle_rx.recv().await {} },
            );
        return (handle, task_handle);
    };

    info!(host = %args.host, "Running with clickhouse indexer");

    let client = ClickhouseClient::default()
        .with_url(args.host)
        .with_database(args.database)
        .with_user(args.username)
        .with_password(args.password);

    let bundle_inserter = client
        .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
        .expect("in 0.13.3, this never returns Err")
        .with_period(Some(Duration::from_secs(4))) // Dump every 4s
        .with_period_bias(0.1) // 4±(0.1*4)
        .with_max_bytes(128 * 1024 * 1024) // 128MiB
        .with_max_rows(65_536);

    let indexer = ClickhouseIndexer { bundle_rx: rx, client, bundle_inserter };
    let task_handle = tokio::spawn(indexer.run());

    (handle, task_handle)
}

#[cfg(test)]
pub(crate) mod tests {
    use clickhouse::Client as ClickhouseClient;
    use clickhouse::{error::Result as ClickhouseResult, test::handlers::RecordDdlControl};
    use derive_more::{Deref, DerefMut, From, Into};
    use rbuilder_primitives::serialize::RawBundle;
    use time::UtcDateTime;
    use tokio::sync::mpsc;

    use crate::types::IndexableSystemBundle;
    use crate::{
        indexer::{
            models::BundleRow, BundleIndexer, ClickhouseIndexer, ClickhouseIndexerHandle,
            BUNDLE_INDEXER_BUFFER_SIZE, BUNDLE_TABLE_NAME,
        },
        types::SystemBundle,
    };

    const CREATE_BUNDLES_TABLE_DDL: &str = r#"CREATE TABLE bundles
(
    `time` DateTime64(6),
    `transactions.hash` Array(FixedString(66)),
    `transactions.from` Array(FixedString(42)),
    `transactions.nonce` Array(UInt64),
    `transactions.r` Array(UInt256),
    `transactions.s` Array(UInt256),
    `transactions.v` Array(UInt256),
    `transactions.to` Array(Nullable(FixedString(42))),
    `transactions.gas` Array(UInt128),
    `transactions.type` Array(UInt64),
    `transactions.input` Array(String),
    `transactions.value` Array(UInt256),
    `transactions.gasPrice` Array(Nullable(UInt128)),
    `transactions.maxFeePerGas` Array(Nullable(UInt128)),
    `transactions.maxPriorityFeePerGas` Array(Nullable(UInt128)),
    `transactions.accessList` Array(Nullable(String)),
    `transactions.authorizationList` Array(Nullable(String)),

    `block_number` Nullable(UInt64),
    `min_timestamp` Nullable(UInt64),
    `max_timestamp` Nullable(UInt64),

    `reverting_tx_hashes` Array(FixedString(66)),
    `dropping_tx_hashes` Array(FixedString(66)),
    `refund_tx_hashes` Nullable(Array(FixedString(66))),

    `uuid` Nullable(String),
    `replacement_nonce` Nullable(Uint64),
    `refund_percent` Nullable(UInt8),
    `refund_recipient` Nullable(FixedString(42)),
    `signer_address` Nullable(FixedString(42)),
    `refund_identity` Nullable(FixedString(42)),

    `builder_name` LowCardinality(String) COMMENT 'name of the endpoint, or IP if missing',

    `hash` FixedString(66),
    `timestamp` DateTime64(6) ALIAS time,

    INDEX from_bloom_filter `transactions.from` TYPE bloom_filter GRANULARITY 10,
    INDEX transactions_hash_bloom_filter `transactions.hash` TYPE bloom_filter GRANULARITY 10,
    INDEX hash_bloom_filter hash TYPE bloom_filter GRANULARITY 10,
    INDEX uuid_bloom_filter uuid TYPE bloom_filter GRANULARITY 10,

    CONSTRAINT valid_transactions_hashes CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.hash`),
    CONSTRAINT valid_transactions_from CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.from`),
    CONSTRAINT valid_transactions_input CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.input`),
    CONSTRAINT valid_refund_percent CHECK (refund_percent IS NULL) OR (refund_percent <= 100)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(time)
PRIMARY KEY (block_number, time)
ORDER BY (block_number, time)
TTL toDateTime(time) + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192;"#;

    const TEST_BUNDLE: &str = r#"{
    "txs": [
        "0x02f89201820132850826299e00850826299e0082a1d694f82300c34f0d11b0420ac3ce85f0ebe4e3e0544280a40d2959800000000000000000000000000000000000000000000000000000000000000001c001a0030c9637d6d442bd2f9a43f69ec09dbaedb12e08ef1fe38ae5ce855bbcfc36ada064101cb4c00d6c21e792a2959c896992c9bbf8e2d84ce7647d561bfbcf59365a",
        "0x02f9019901458405f5e10085084f73d22e8304d3819480a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e870aa87bee538000b90124d1ef924900000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000038d7ea4c6800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006670d0d6c4985c3948000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000f82300c34f0d11b0420ac3ce85f0ebe4e3e05442c080a0b881ab734863b49369bfbf2e28c035785276423783dbbb8e74ec813fd3a95614a0499f781a62cdf3d9a235b83e050f6e8f74df15fcad09822c78a84ace5a44a59b",
        "0x02f9019901058405f5e10085084f73d22e8304d4269480a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e87071afd498d0000b90124d1ef924900000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000038d7ea4c6800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006670d0d6c4985c394800000000000000000000000000000000000000000000000100f8061414d648d750000000000000000000000005c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000f82300c34f0d11b0420ac3ce85f0ebe4e3e05442c080a0b9272dad54e7f289385ba64368db5de0a96bd9364734d2edf989307c93cd3982a01b076c83c0bb7d2c644568255d9878ce66c7552175df58025d670a9cec310546"
    ],
    "blockNumber": "0x0"
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
            let inner = ClickhouseClient::default().with_url(mock.url());
            Self { inner, mock }
        }

        async fn create_bundles_table(&self) -> ClickhouseResult<RecordDdlControl> {
            let recording = self.mock.add(clickhouse::test::handlers::record_ddl());
            self.query(CREATE_BUNDLES_TABLE_DDL).execute().await?;
            Ok(recording)
        }
    }

    #[allow(dead_code)]
    impl ClickhouseIndexer {
        async fn receive_one(&mut self) {
            let Some(indexable_bundle) = self.bundle_rx.recv().await else {
                panic!("Expected to receive a bundle")
            };
            let bundle_row = indexable_bundle.into();

            self.bundle_inserter.write(&bundle_row).unwrap();
            let _ = self.bundle_inserter.commit().await;
        }
    }

    /// An example system bundle to use for testing.
    pub(crate) fn system_bundle_example() -> SystemBundle {
        let bundle = serde_json::from_str::<RawBundle>(TEST_BUNDLE).unwrap();
        let signer = alloy_primitives::address!("0xff31f52c4363b1dacb25d9de07dff862bf1d0e1c");
        SystemBundle::try_from_bundle_and_signer(bundle, signer).unwrap()
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
        let handle = ClickhouseIndexerHandle { bundle_tx: tx };
        let bundle_inserter = client
            .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
            .expect("in 0.13.3, this never returns Err")
            .with_max_rows(0); // force commit immediately

        let indexer =
            ClickhouseIndexer { bundle_rx: rx, client: client.inner.clone(), bundle_inserter };

        let indexable_bundle = IndexableSystemBundle {
            system_bundle: system_bundle_example(),
            timestamp: UtcDateTime::now(),
            builder_name: String::from("buildernet"),
        };

        let expected_bundle_rows = vec![BundleRow::from(indexable_bundle.clone())];

        // Test logic

        // Add handler for the next INSERT request.
        let recording = client.mock.add(clickhouse::test::handlers::record());

        tokio::spawn(indexer.run());
        handle.index_bundle(indexable_bundle);

        let recording_rows: Vec<BundleRow> = recording.collect().await;
        assert_eq!(expected_bundle_rows, recording_rows);
    }
}

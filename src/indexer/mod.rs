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
    use std::{borrow::Cow, collections::BTreeMap, fs};

    use clickhouse::{error::Result as ClickhouseResult, Client as ClickhouseClient};
    use rbuilder_primitives::serialize::RawBundle;
    use testcontainers::{
        core::{
            error::Result as TestcontainersResult, wait::HttpWaitStrategy, ContainerPort, WaitFor,
        },
        runners::AsyncRunner as _,
        ContainerAsync, Image,
    };
    use time::UtcDateTime;
    use tokio::sync::mpsc;

    use crate::{
        indexer::{
            models::BundleRow, ClickhouseIndexer, IndexerHandle, BUNDLE_INDEXER_BUFFER_SIZE,
            BUNDLE_TABLE_NAME,
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

    /// The default clickhouse image name to use for testcontainers testing.
    const CLICKHOUSE_DEFAULT_IMAGE_NAME: &str = "clickhouse/clickhouse-server";
    /// The default clickhouse image tag to use for testcontainers testing.
    const CLICKHOUSE_DEFAULT_IMAGE_TAG: &str = "25.6.2.5";
    /// Port that the [`ClickHouse`] container has internally, for testcontainers testing.
    /// Can be rebound externally via [`testcontainers::core::ImageExt::with_mapped_port`].
    const CLICKHOUSE_PORT: ContainerPort = ContainerPort::Tcp(8123);

    /// A clickhouse image that can be spawn up using testcontainers.
    ///
    /// # Example
    /// ```
    /// use testcontainers_modules::{clickhouse, testcontainers::runners::SyncRunner};
    ///
    /// let clickhouse = clickhouse::ClickHouse::default().start().unwrap();
    /// let http_port = clickhouse.get_host_port_ipv4(8123).unwrap();
    ///
    /// // do something with the started clickhouse instance..
    /// ```
    ///
    /// [`ClickHouse`]: https://clickhouse.com/
    /// [`Clickhouse docker image`]: https://hub.docker.com/r/clickhouse/clickhouse-server
    #[derive(Debug, Clone)]
    struct ClickhouseImage {
        env_vars: BTreeMap<String, String>,
    }

    impl Image for ClickhouseImage {
        fn name(&self) -> &str {
            CLICKHOUSE_DEFAULT_IMAGE_NAME
        }

        fn tag(&self) -> &str {
            CLICKHOUSE_DEFAULT_IMAGE_TAG
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            vec![WaitFor::http(HttpWaitStrategy::new("/").with_expected_status_code(200_u16))]
        }

        fn env_vars(
            &self,
        ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
            &self.env_vars
        }

        fn expose_ports(&self) -> &[ContainerPort] {
            &[CLICKHOUSE_PORT]
        }
    }

    impl Default for ClickhouseImage {
        fn default() -> Self {
            let mut env_vars = BTreeMap::default();
            env_vars.insert("CLICKHOUSE_DB".to_string(), "default".to_string());
            env_vars.insert("CLICKHOUSE_USER".to_string(), "default".to_string());
            env_vars.insert("CLICKHOUSE_PASSWORD".to_string(), "password".to_string());
            env_vars.insert("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT".to_string(), "1".to_string());
            Self { env_vars }
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

    /// Create a test clickhouse client using testcontainers. Returns both the image and the
    /// client.
    ///
    /// IMPORTANT: the image must be manually `drop`ped at the end of the test, otherwise the
    /// container is cancelled prematurely.
    async fn create_test_clickhouse_client(
    ) -> TestcontainersResult<(ContainerAsync<ClickhouseImage>, ClickhouseClient)> {
        // Start a Docker client (testcontainers manages lifecycle)
        let clickhouse = ClickhouseImage::default().start().await?;
        let port = clickhouse.get_host_port_ipv4(8123).await?;
        let host = clickhouse.get_host().await?;
        let url = format!("http://{}:{}", host, port);

        Ok((
            clickhouse,
            ClickhouseClient::default()
                .with_url(url)
                .with_user("default")
                .with_password("password")
                .with_validation(true),
        ))
    }

    /// Creates the bundles table from the DDL present inside the `fixtures` folder.
    async fn create_clickhouse_bundles_table(client: &ClickhouseClient) -> ClickhouseResult<()> {
        let create_bundles_table_ddl = fs::read_to_string("./fixtures/create_bundles_table.sql")
            .expect("could not read create_bundles_table.sql")
            // NOTE: for local instances, ReplicatedMergeTree isn't supported.
            .replace(
                "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
                "ENGINE = MergeTree()",
            );
        client.query(&create_bundles_table_ddl).execute().await
    }

    #[tokio::test]
    async fn clickhouse_bundles_table_create_table_succeds() {
        let (image, client) = create_test_clickhouse_client().await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();
        drop(image);
    }

    #[tokio::test]
    async fn clickhouse_bundles_insert_single_row_succeeds() {
        let (image, client) = create_test_clickhouse_client().await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();

        let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
        let _ = IndexerHandle { bundle_tx: tx };
        let bundle_inserter = client.inserter::<BundleRow>(BUNDLE_TABLE_NAME).with_max_rows(0); // force commit immediately

        let mut indexer = ClickhouseIndexer {
            bundle_rx: rx,
            client: client.clone(),
            bundle_inserter,
            builder_name: "buildernet".to_string(),
        };

        let system_bundle = system_bundle_example();

        indexer
            .bundle_inserter
            .write(&(system_bundle.clone(), indexer.builder_name.clone()).into())
            .await
            .unwrap();

        indexer.bundle_inserter.force_commit().await.unwrap();

        let system_bundle = system_cancel_bundle_example();

        indexer
            .bundle_inserter
            .write(&(system_bundle.clone(), indexer.builder_name.clone()).into())
            .await
            .unwrap();

        indexer.bundle_inserter.force_commit().await.unwrap();
        drop(image);
    }
}

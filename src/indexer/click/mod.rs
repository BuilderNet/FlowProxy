//! Indexing functionality powered by Clickhouse.

use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use clickhouse::{
    error::Result as ClickhouseResult,
    inserter::{Inserter, Quantities},
    Client as ClickhouseClient, Row,
};
use tokio::sync::mpsc;

use crate::{
    cli::ClickhouseArgs,
    indexer::{
        click::{
            backup::{FailedCommit, MemoryBackup, MAX_BACKUP_SIZE_BYTES},
            models::{BundleReceiptRow, BundleRow},
            primitives::{BuilderName, ClickhouseIndexableOrder},
        },
        OrderReceivers, BUNDLE_RECEIPTS_TABLE_NAME, BUNDLE_TABLE_NAME, TARGET,
    },
    metrics::IndexerMetrics,
    primitives::{BundleReceipt, Sampler, SystemBundle},
    spawn_clickhouse_backup, spawn_clickhouse_inserter,
    tasks::TaskExecutor,
};

mod backup;
mod macros;
mod models;
pub(crate) mod primitives;

/// An clickhouse inserter with some sane defaults.
fn default_inserter<T: Row>(client: &ClickhouseClient, table_name: &str) -> Inserter<T> {
    // TODO: make this configurable.
    let send_timeout = Duration::from_secs(2);
    let end_timeout = Duration::from_secs(4);

    client
        .inserter::<T>(table_name)
        .with_period(Some(Duration::from_secs(4))) // Dump every 4s
        .with_period_bias(0.1) // 4±(0.1*4)
        .with_max_bytes(128 * 1024 * 1024) // 128MiB
        .with_max_rows(65_536)
        .with_timeouts(Some(send_timeout), Some(end_timeout))
}

/// A wrapper over a Clickhouse [`Inserter`] that supports a backup mechanism.
struct ClickhouseInserter<T: ClickhouseIndexableOrder> {
    /// The inner Clickhouse inserter client.
    inner: Inserter<T::ClickhouseRowType>,
    /// A small in-memory backup of the current data we're trying to commit. In case this fails to
    /// be inserted into Clickhouse, it is sent to the backup actor.
    rows_backup: Vec<T::ClickhouseRowType>,
    /// The channel where to send data to be backed up.
    backup_tx: mpsc::Sender<FailedCommit<T>>,
    /// The name of the local operator to use when adding data to clickhouse.
    builder_name: String,
}

impl<T: ClickhouseIndexableOrder> ClickhouseInserter<T> {
    fn new(
        inner: Inserter<T::ClickhouseRowType>,
        backup_tx: mpsc::Sender<FailedCommit<T>>,
        builder_name: String,
    ) -> Self {
        let rows_backup = Vec::new();
        Self { inner, rows_backup, backup_tx, builder_name }
    }

    /// Writes the provided order into the inner Clickhouse writer buffer.
    async fn write(&mut self, order: T) {
        let hash = order.hash();
        let order_row: T::ClickhouseRowType = (order, self.builder_name.clone()).into();
        let value_ref = T::to_row_ref(&order_row);

        if let Err(e) = self.inner.write(value_ref).await {
            IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
            tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, %hash, "failed to write to clickhouse inserter");
            return;
        }

        // NOTE: we don't backup if writing failes. The reason is that if this fails, then the same
        // writing to the backup inserter should fail.
        self.rows_backup.push(order_row);
    }

    /// Tries to commit to Clickhouse if the conditions are met. In case of failures, data is sent
    /// to the backup actor for retries.
    async fn commit(&mut self) {
        let pending = self.inner.pending().clone(); // This is cheap to clone.

        let start = Instant::now();
        match self.inner.commit().await {
            Ok(quantities) => {
                if quantities == Quantities::ZERO {
                    tracing::trace!(target: TARGET, order = T::ORDER_TYPE, "committed to inserter");
                } else {
                    tracing::debug!(target: TARGET, order = T::ORDER_TYPE, ?quantities, "inserted batch to clickhouse");
                    IndexerMetrics::process_clickhouse_quantities(&quantities);
                    IndexerMetrics::record_clickhouse_batch_commit_time(start.elapsed());
                    // Clear the backup rows.
                    self.rows_backup.clear();
                }
            }
            Err(e) => {
                IndexerMetrics::increment_clickhouse_commit_failures(e.to_string());
                tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, "failed to commit bundle to clickhouse");

                let rows = std::mem::take(&mut self.rows_backup);
                let failed_commit = FailedCommit::new(rows, pending);

                if let Err(e) = self.backup_tx.try_send(failed_commit) {
                    tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, "failed to send rows backup");
                }
            }
        }
    }

    /// Ends the current `INSERT` and whole `Inserter` unconditionally.
    async fn end(self) -> ClickhouseResult<Quantities> {
        self.inner.end().await
    }
}

impl<T: ClickhouseIndexableOrder> std::fmt::Debug for ClickhouseInserter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseInserter")
            .field("inserter", &T::ORDER_TYPE.to_string())
            .field("rows_backup_len", &self.rows_backup.len())
            .field("builder_name", &self.builder_name)
            .finish()
    }
}

/// A long-lived actor to run a [`ClickhouseIndexer`] until it possible to receive new order to
/// index.
struct InserterRunner<T: ClickhouseIndexableOrder> {
    /// The channel from which we can receive new orders to index.
    rx: mpsc::Receiver<T>,
    /// The underlying Clickhouse inserter.
    inserter: ClickhouseInserter<T>,
}

impl<T: ClickhouseIndexableOrder> InserterRunner<T> {
    fn new(rx: mpsc::Receiver<T>, inserter: ClickhouseInserter<T>) -> Self {
        Self { rx, inserter }
    }

    /// Run the inserter until it is possible to receive new orders.
    async fn run_loop(&mut self) {
        let mut sampler = Sampler::default()
            .with_sample_size(self.rx.capacity() / 2)
            .with_interval(Duration::from_secs(4));

        while let Some(order) = self.rx.recv().await {
            tracing::trace!(target: TARGET, order = T::ORDER_TYPE, hash = %order.hash(), "received data to index");
            sampler.sample(|| {
                IndexerMetrics::set_clickhouse_queue_size(self.rx.len(), T::ORDER_TYPE);
            });

            self.inserter.write(order).await;
            self.inserter.commit().await;
        }
        tracing::error!(target: TARGET, order = T::ORDER_TYPE, "tx channel closed, indexer will stop running");
    }
}

/// A namespace struct to spawn a Clickhouse indexer.
#[derive(Debug, Clone)]
pub(crate) struct ClickhouseIndexer;

impl ClickhouseIndexer {
    /// Create and spawn new Clickhouse indexer tasks, returning their indexer handle.
    ///
    /// NOTE: validation should be set to false for for performance reasons, and because validation
    /// doesn't support Uint256 data types.
    pub(crate) fn run(
        args: ClickhouseArgs,
        builder_name: BuilderName,
        receivers: OrderReceivers,
        task_executor: TaskExecutor,
        validation: bool,
    ) {
        let (host, database, username, password, bundles_table_name, bundle_receipts_table_name) = (
            args.host.expect("host is set"),
            args.database.expect("database is set"),
            args.username.expect("username is set"),
            args.password.expect("password is set"),
            args.bundles_table_name.unwrap_or(BUNDLE_TABLE_NAME.to_string()),
            args.bundle_receipts_table_name.unwrap_or(BUNDLE_RECEIPTS_TABLE_NAME.to_string()),
        );

        tracing::info!(%host, "Running with clickhouse indexer");

        let OrderReceivers { bundle_rx, bundle_receipt_rx } = receivers;

        let client = ClickhouseClient::default()
            .with_url(host)
            .with_database(database)
            .with_user(username)
            .with_password(password)
            .with_validation(validation);

        let bundle_inserter = default_inserter::<BundleRow>(&client, &bundles_table_name);
        let (tx, rx) = mpsc::channel::<FailedCommit<SystemBundle>>(128);
        let bundle_inserter = ClickhouseInserter::new(bundle_inserter, tx, builder_name.clone());
        let mut bundle_inserter_runner = InserterRunner::new(bundle_rx, bundle_inserter);
        let mut bundle_backup = MemoryBackup::new(
            rx,
            client
                .inserter::<BundleRow>(&bundles_table_name)
                .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(12))),
        )
        .with_max_size_bytes(args.max_backup_size_bytes.unwrap_or(MAX_BACKUP_SIZE_BYTES));

        let bundle_receipt_inserter =
            default_inserter::<BundleReceiptRow>(&client, &bundle_receipts_table_name);
        let (tx, rx) = mpsc::channel::<FailedCommit<BundleReceipt>>(128);
        let bundle_receipt_inserter =
            ClickhouseInserter::new(bundle_receipt_inserter, tx, builder_name);
        let mut bundle_receipt_inserter_runner =
            InserterRunner::new(bundle_receipt_rx, bundle_receipt_inserter);
        let mut bundle_receipt_backup = MemoryBackup::new(
            rx,
            client
                .inserter::<BundleReceiptRow>(&bundle_receipts_table_name)
                .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(12))),
        )
        .with_max_size_bytes(args.max_backup_size_bytes.unwrap_or(MAX_BACKUP_SIZE_BYTES));

        spawn_clickhouse_inserter!(task_executor, bundle_inserter_runner, "bundles");
        spawn_clickhouse_backup!(task_executor, bundle_backup, "bundles");
        spawn_clickhouse_inserter!(
            task_executor,
            bundle_receipt_inserter_runner,
            "bundle receipts"
        );
        spawn_clickhouse_backup!(task_executor, bundle_receipt_backup, "bundles receipts");
    }
}

impl<T: ClickhouseIndexableOrder> std::fmt::Debug for InserterRunner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InserterRunner")
            .field("inserter", &T::ORDER_TYPE.to_string())
            .field("rx", &self.rx)
            .finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{borrow::Cow, collections::BTreeMap, fs, time::Duration};

    use crate::{
        cli::ClickhouseArgs,
        indexer::{
            click::{
                models::{BundleReceiptRow, BundleRow},
                ClickhouseIndexer,
            },
            tests::{bundle_receipt_example, system_bundle_example},
            OrderSenders, BUNDLE_RECEIPTS_TABLE_NAME, BUNDLE_TABLE_NAME,
        },
        tasks::TaskManager,
    };
    use clickhouse::{error::Result as ClickhouseResult, Client as ClickhouseClient};
    use testcontainers::{
        core::{
            error::Result as TestcontainersResult, wait::HttpWaitStrategy, ContainerPort, WaitFor,
        },
        runners::AsyncRunner as _,
        ContainerAsync, Image,
    };
    use tokio::runtime::Handle;

    // Uncomment to enable logging during tests.
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

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

    /// The configuration used in a [`ClickhouseClient`] for testing.
    #[derive(Debug, Clone)]
    struct ClickhouseConfig {
        url: String,
        user: String,
        password: String,
        validation: bool,
    }

    impl From<ClickhouseConfig> for ClickhouseArgs {
        fn from(config: ClickhouseConfig) -> Self {
            Self {
                host: Some(config.url),
                database: Some("default".to_string()),
                username: Some(config.user),
                password: Some(config.password),
                bundles_table_name: Some(BUNDLE_TABLE_NAME.to_string()),
                bundle_receipts_table_name: Some(BUNDLE_RECEIPTS_TABLE_NAME.to_string()),
                max_backup_size_bytes: None,
            }
        }
    }

    /// Create a test clickhouse client using testcontainers. Returns both the image and the
    /// client.
    ///
    /// IMPORTANT: the image must be manually `drop`ped at the end of the test, otherwise the
    /// container is cancelled prematurely.
    async fn create_test_clickhouse_client(
        validation: bool,
    ) -> TestcontainersResult<(ContainerAsync<ClickhouseImage>, ClickhouseClient, ClickhouseConfig)>
    {
        // Start a Docker client (testcontainers manages lifecycle)
        let clickhouse = ClickhouseImage::default().start().await?;
        let port = clickhouse.get_host_port_ipv4(8123).await?;
        let host = clickhouse.get_host().await?;
        let url = format!("http://{host}:{port}");

        let config = ClickhouseConfig {
            url,
            user: "default".to_string(),
            password: "password".to_string(),
            validation,
        };

        Ok((
            clickhouse,
            ClickhouseClient::default()
                .with_url(config.url.clone())
                .with_user(config.user.clone())
                .with_password(config.password.clone())
                .with_validation(config.validation),
            config,
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

    /// Creates the bundle receipts table from the DDL present inside the `fixtures` folder.
    async fn create_clickhouse_bundle_receipts_table(
        client: &ClickhouseClient,
    ) -> ClickhouseResult<()> {
        let create_bundle_receipts_table_ddl =
            fs::read_to_string("./fixtures/create_bundle_receipts_table.sql")
                .expect("could not read create_bundle_receipts_table.sql")
                .replace("ENGINE = SharedMergeTree()", "ENGINE = MergeTree()");
        client.query(&create_bundle_receipts_table_ddl).execute().await
    }

    #[tokio::test]
    async fn clickhouse_bundles_table_create_table_succeds() {
        let (image, client, _) = create_test_clickhouse_client(true).await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();
        drop(image);
    }

    #[tokio::test]
    async fn clickhouse_bundles_insert_single_row_succeeds() {
        let (image, client, _) = create_test_clickhouse_client(false).await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();

        let mut bundle_inserter = client.inserter::<BundleRow>(BUNDLE_TABLE_NAME).with_max_rows(0); // force commit immediately
        let builder_name = "buildernet".to_string();

        // Insert system bundle and system cancel bundle

        let system_bundle = system_bundle_example();
        let system_bundle_row = (system_bundle.clone(), builder_name.clone()).into();

        bundle_inserter.write(&system_bundle_row).await.unwrap();
        bundle_inserter.commit().await.unwrap();

        // Now select then, and verify they match with original input.

        let select_row = client
            .query(&format!("SELECT * FROM {BUNDLE_TABLE_NAME} LIMIT 1"))
            .fetch_one::<BundleRow>()
            .await
            .unwrap();

        assert_eq!(select_row, system_bundle_row);

        drop(image);
    }

    /// E2E where we spin up the whole indexer and we shut down the application.
    #[tokio::test(flavor = "multi_thread")]
    async fn clickhouse_bundles_insert_single_row_e2e_succeds() {
        // Uncomment to toggle logs.
        let registry = tracing_subscriber::registry().with(
            EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).from_env_lossy(),
        );
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        // 1. Spin up Clickhouse
        let (image, client, config) = create_test_clickhouse_client(false).await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();

        // 2. Spin up task executor and indexer
        let task_manager = TaskManager::new(Handle::current());
        let task_executor = task_manager.executor();
        let builder_name = "buildernet".to_string();
        let (senders, receivers) = OrderSenders::new();

        let validation = false;
        ClickhouseIndexer::run(
            config.into(),
            builder_name.clone(),
            receivers,
            task_executor,
            validation,
        );

        // 3. Send a bundle
        let system_bundle = system_bundle_example();
        let system_bundle_row = (system_bundle.clone(), builder_name.clone()).into();
        senders.bundle_tx.send(system_bundle.clone()).await.unwrap();

        // Wait a bit for bundle to be actually processed before shutting down.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 4. Shutdown and check results.
        assert!(
            task_manager.graceful_shutdown_with_timeout(Duration::from_secs(5)),
            "shutdown timeout"
        );
        let select_row = client
            .query(&format!("SELECT * FROM {BUNDLE_TABLE_NAME} LIMIT 1"))
            .fetch_one::<BundleRow>()
            .await
            .unwrap();

        assert_eq!(select_row, system_bundle_row);

        drop(image);
    }

    /// E2E where we spin up the whole indexer and we shut down the application.
    #[tokio::test(flavor = "multi_thread")]
    async fn clickhouse_bundle_receipts_insert_single_row_e2e_succeds() {
        // Uncomment to toggle logs.
        let registry = tracing_subscriber::registry().with(
            EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).from_env_lossy(),
        );
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        // 1. Spin up Clickhouse
        let (image, client, config) = create_test_clickhouse_client(false).await.unwrap();
        create_clickhouse_bundle_receipts_table(&client).await.unwrap();

        // 2. Spin up task executor and indexer
        let task_manager = TaskManager::new(Handle::current());
        let task_executor = task_manager.executor();
        let builder_name = "buildernet".to_string();
        let (senders, receivers) = OrderSenders::new();

        let validation = false;
        ClickhouseIndexer::run(
            config.into(),
            builder_name.clone(),
            receivers,
            task_executor,
            validation,
        );

        let mut bundle_receipts = Vec::new();
        let mut bundle_receipts_rows = Vec::new();
        for _ in 0..16 {
            bundle_receipts.push(bundle_receipt_example());
            // So we get a different `received_at` field
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for bundle_receipt in bundle_receipts {
            let bundle_receipt_row = (bundle_receipt.clone(), builder_name.clone()).into();
            bundle_receipts_rows.push(bundle_receipt_row);
            senders.bundle_receipt_tx.send(bundle_receipt.clone()).await.unwrap();
        }

        // Wait a bit for bundle to be actually processed before shutting down.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 4. Shutdown and check results.
        assert!(
            task_manager.graceful_shutdown_with_timeout(Duration::from_secs(5)),
            "shutdown timeout"
        );
        let select_row = client
            .query(&format!("SELECT * FROM {BUNDLE_RECEIPTS_TABLE_NAME}"))
            .fetch_all::<BundleReceiptRow>()
            .await
            .unwrap();

        assert_eq!(select_row.len(), bundle_receipts_rows.len());

        for (row, expected) in select_row.iter().zip(bundle_receipts_rows.iter()) {
            assert_eq!(row, expected);
        }

        drop(image);
    }
}

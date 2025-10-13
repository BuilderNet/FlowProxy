//! Indexing functionality powered by Clickhouse.
//!
//! TODO: update metrics

use std::{collections::VecDeque, fmt::Debug, time::Duration};

use alloy_primitives::B256;
use clickhouse::{
    error::Result as ClickhouseResult,
    inserter::{Inserter, Quantities},
    Client as ClickhouseClient, Row, RowWrite,
};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::info;

use crate::{
    cli::ClickhouseArgs,
    indexer::{
        models::BundleRow,
        time::{BackoffInterval, ExponentialBackoff},
        BuilderName, OrderReceivers, BUNDLE_TABLE_NAME, TRACING_TARGET,
    },
    metrics::IndexerMetrics,
    tasks::TaskExecutor,
    types::{Sampler, SystemBundle},
};

const MAX_BACKUP_SIZE_BYTES: u64 = 512 * 1024 * 1024; // 512 MiB

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

#[derive(Debug)]
struct FailedCommit<T: ClickhouseIndexableOrder> {
    rows: Vec<T::ClickhouseRowType>,
    quantities: Quantities,
    attempts: u8,
}

impl<T: ClickhouseIndexableOrder> FailedCommit<T> {
    fn new(rows: Vec<T::ClickhouseRowType>, quantities: Quantities) -> Self {
        Self { rows, quantities, attempts: 0 }
    }
}

// Rationale for sending multiple rows instead of sending rows: the backup abstraction must
// periodically block to write data to the inserter and try to commit it to clickhouse. Each
// attempt results in doing the previous step. This could clog the channel which will receive
// individual rows, leading to potential row losses.
//
// By sending backup data less often, we give time gaps for these operation to be performed.

struct InMemoryBackup<T: ClickhouseIndexableOrder> {
    rx: mpsc::Receiver<FailedCommit<T>>,
    failed_commits: VecDeque<FailedCommit<T>>,
    inserter: Inserter<T::ClickhouseRowType>,
    interval: BackoffInterval,
    max_attempts: u8,
    max_size_bytes: u64,
    apply_pressure: bool,
}

impl<T: ClickhouseIndexableOrder> InMemoryBackup<T> {
    fn new(
        rx: mpsc::Receiver<FailedCommit<T>>,
        inserter: Inserter<T::ClickhouseRowType>,
        interval: Option<BackoffInterval>,
        max_attempts: u8,
        max_size_bytes: u64,
    ) -> Self {
        Self {
            rx,
            max_attempts,
            inserter,
            interval: interval.unwrap_or_default(),
            failed_commits: VecDeque::new(),
            max_size_bytes,
            apply_pressure: false,
        }
    }

    #[tracing::instrument(target = TRACING_TARGET, fields(order = %T::ORDER_TYPE))]
    async fn run(&mut self) {
        loop {
            tokio::select! {
                maybe_failed_commit = self.rx.recv() => {
                    let Some(failed_commit) = maybe_failed_commit else {
                        tracing::error!("backup channel closed");
                        break;
                    };
                    tracing::debug!(quantities = ?failed_commit.quantities, "received failed commit to backup");

                    self.failed_commits.push_back(failed_commit);
                    let total_size_bytes = self.failed_commits.iter().map(|c| c.quantities.bytes).sum::<u64>();

                    if total_size_bytes > self.max_size_bytes {
                        self.apply_pressure = true;
                        // TODO: maybe MiB would be a better unit here?
                        tracing::warn!(total_size_bytes, max_size_bytes = self.max_size_bytes, "failed commits exceeded max size, applying backpressure");
                    }
                }
                _ = self.interval.tick() => {
                        let Some(mut oldest) = self.failed_commits.pop_back() else {
                            continue // Nothing to do!
                        };

                        if self.apply_pressure {
                            oldest.attempts += 1;
                        }

                        if oldest.attempts > self.max_attempts {
                            tracing::error!(max_attempts = self.max_attempts, "dropping failed commit, too many attempts");
                            continue;
                        }

                        for row in &oldest.rows {
                            let value_ref = T::to_row_ref(row);

                            if let Err(e) = self.inserter.write(value_ref).await {
                                IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
                                tracing::error!(?e, "failed to write to backup inserter");
                                continue;
                            }
                        }

                        if let Err(e) = self.inserter.force_commit().await {
                            tracing::error!(target: TRACING_TARGET, ?e, "failed to commit backup");
                            self.failed_commits.push_front(oldest);
                        } else {
                            tracing::info!(target: TRACING_TARGET, "successfully backed up");
                            self.interval.reset();
                        }

                }
            }
        }
    }

    #[tracing::instrument(target = TRACING_TARGET, fields(order = %T::ORDER_TYPE))]
    async fn end(mut self) {
        for failed_commit in self.failed_commits.drain(..) {
            for row in &failed_commit.rows {
                let value_ref = T::to_row_ref(row);

                if let Err(e) = self.inserter.write(value_ref).await {
                    IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
                    tracing::error!(?e, "failed to write to backup inserter during shutdown");
                    continue;
                }
            }
            if let Err(e) = self.inserter.force_commit().await {
                tracing::error!(target: TRACING_TARGET, ?e, "failed to commit backup during shutdown");
            }
        }
    }
}

impl<T: ClickhouseIndexableOrder> std::fmt::Debug for InMemoryBackup<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBackup")
            .field("rx", &self.rx)
            .field("inserter", &T::ORDER_TYPE.to_string())
            .field("failed_commits", &self.failed_commits.len())
            .field("max_attempts", &self.max_attempts)
            .field("max_size_bytes", &self.max_size_bytes)
            .finish()
    }
}

struct ClickhouseInserter<T: ClickhouseIndexableOrder> {
    inner: Inserter<T::ClickhouseRowType>,
    rows_backup: Vec<T::ClickhouseRowType>,
    backup_tx: mpsc::Sender<FailedCommit<T>>,
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
    #[tracing::instrument(target = TRACING_TARGET, fields(order = %T::ORDER_TYPE))]
    async fn write(&mut self, order: T) {
        let hash = order.hash();
        let order_row: T::ClickhouseRowType = (order, self.builder_name.clone()).into();
        let value_ref = T::to_row_ref(&order_row);

        if let Err(e) = self.inner.write(value_ref).await {
            IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
            tracing::error!(?e, %hash, "failed to write to clickhouse inserter"
            )
        }

        // NOTE: we don't backup if writing failes. The reason is that if this fails, then the same
        // writing to the backup inserter will fail.
        self.rows_backup.push(order_row);
    }

    /// Tries to commit to Clickhouse if the conditions are met. In case of failures, data is sent
    /// to the backup actor for retries.
    #[tracing::instrument(target = TRACING_TARGET, fields(order = %T::ORDER_TYPE))]
    async fn commit(&mut self) {
        let pending = self.inner.pending().clone(); // This is cheap to clone.

        match self.inner.commit().await {
            Ok(quantities) => {
                if quantities == Quantities::ZERO {
                    tracing::trace!("committed to inserter");
                } else {
                    tracing::debug!(?quantities, "inserted batch to clickhouse");
                    IndexerMetrics::process_clickhouse_quantities(&quantities);
                }
            }
            Err(e) => {
                IndexerMetrics::increment_clickhouse_commit_failures(e.to_string());
                tracing::error!(?e, "failed to commit bundle to clickhouse");

                let rows = std::mem::take(&mut self.rows_backup);
                let failed_commit = FailedCommit::new(rows, pending);

                if let Err(e) = self.backup_tx.try_send(failed_commit) {
                    tracing::error!(?e, "failed to send rows backup");
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

struct InserterRunner<T: ClickhouseIndexableOrder> {
    rx: mpsc::Receiver<T>,
    inserter: ClickhouseInserter<T>,
}

impl<T: ClickhouseIndexableOrder> InserterRunner<T> {
    fn new(rx: mpsc::Receiver<T>, inserter: ClickhouseInserter<T>) -> Self {
        Self { rx, inserter }
    }

    #[tracing::instrument(target = TRACING_TARGET, fields(order = %T::ORDER_TYPE))]
    async fn run_loop(&mut self) {
        let mut sampler = Sampler::default()
            .with_sample_size(self.rx.capacity() / 2)
            .with_interval(Duration::from_secs(4));

        while let Some(order) = self.rx.recv().await {
            tracing::trace!(hash = %order.hash(), "received data to index");
            sampler.sample(|| {
                IndexerMetrics::set_clickhouse_queue_size(self.rx.len(), T::ORDER_TYPE);
            });

            self.inserter.write(order).await;
            self.inserter.commit().await;
        }
        tracing::error!(target: TRACING_TARGET, "{} tx channel closed, indexer will stop running", T::ORDER_TYPE);
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
        let (host, database, username, password, bundles_table_name) = (
            args.host.expect("host is set"),
            args.database.expect("database is set"),
            args.username.expect("username is set"),
            args.password.expect("password is set"),
            args.bundles_table_name.unwrap_or(BUNDLE_TABLE_NAME.to_string()),
        );

        info!(%host, "Running with clickhouse indexer");

        let OrderReceivers { bundle_rx, mut bundle_receipt_rx } = receivers;

        let client = ClickhouseClient::default()
            .with_url(host)
            .with_database(database)
            .with_user(username)
            .with_password(password)
            .with_validation(validation);

        let bundle_inserter = client
            .inserter::<BundleRow>(bundles_table_name.as_str())
            .with_period(Some(Duration::from_secs(4))) // Dump every 4s
            .with_period_bias(0.1) // 4±(0.1*4)
            .with_max_bytes(128 * 1024 * 1024) // 128MiB
            .with_max_rows(65_536);

        // TODO: cleanup

        let (tx, rx) = mpsc::channel::<FailedCommit<SystemBundle>>(8); // TODO: use this and spin up backup
        let bundle_inserter = ClickhouseInserter::new(bundle_inserter, tx, builder_name);

        let interval = BackoffInterval::new(ExponentialBackoff::from_millis(100)).with_jitter();

        let mut backup_runner = InMemoryBackup::new(
            rx,
            client.inserter::<BundleRow>(bundles_table_name.as_str()),
            Some(interval),
            4,
            MAX_BACKUP_SIZE_BYTES,
        );

        let mut bundle_inserter_runner = InserterRunner::new(bundle_rx, bundle_inserter);

        // TODO: Make this generic over order types. Requires some trait bounds.
        task_executor.spawn_with_graceful_shutdown_signal(|shutdown| async move {
            let mut shutdown_guard = None;
            tokio::select! {
                _ = bundle_inserter_runner.run_loop() => {
                    info!(target: TRACING_TARGET, "clickhouse bundle indexer channel closed");
                }
                guard = shutdown => {
                    info!(target: TRACING_TARGET, "Received shutdown for bundle indexer, performing cleanup");
                    shutdown_guard = Some(guard);
                },
            }

            match  bundle_inserter_runner.inserter.end().await {
                Ok(quantities) => {
                    info!(target: TRACING_TARGET, ?quantities, "finalized clickhouse bundle inserter");
                }
                Err(e) => {
                    tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion of bundles to indexer");
                }
            }
            drop(shutdown_guard);
        });

        task_executor.spawn_with_graceful_shutdown_signal(|shutdown| async move {
            let mut shutdown_guard = None;
            tokio::select! {
                _ = backup_runner.run() => {
                    info!(target: TRACING_TARGET, "clickhouse bundle indexer channel closed");
                }
                guard = shutdown => {
                    info!(target: TRACING_TARGET, "Received shutdown for bundle indexer, performing cleanup");
                    shutdown_guard = Some(guard);
                },
            }

            backup_runner.end().await;
            drop(shutdown_guard);
        });

        // TODO: support bundle receipts in Clickhouse.
        task_executor.spawn(async move { while let Some(_b) = bundle_receipt_rx.recv().await {} });
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
            click::ClickhouseIndexer, models::BundleRow, tests::system_bundle_example,
            OrderSenders, BUNDLE_TABLE_NAME,
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
}

//! Indexing functionality powered by Clickhouse.

use std::{fmt::Debug, time::Duration};

use rbuilder_utils::{
    clickhouse::{
        backup::{Backup, DiskBackup, DiskBackupConfig, MemoryBackupConfig},
        indexer::{default_inserter, ClickhouseClientConfig, ClickhouseInserter, InserterRunner},
        Quantities,
    },
    spawn_clickhouse_backup, spawn_clickhouse_inserter,
    tasks::TaskExecutor,
};
use tokio::sync::mpsc;

use crate::{
    cli::ClickhouseArgs,
    indexer::{
        click::models::{BundleReceiptRow, BundleRow},
        OrderReceivers, TARGET,
    },
    metrics::IndexerMetrics,
};

mod models;

fn config_from_clickhouse_args(args: &ClickhouseArgs, validation: bool) -> ClickhouseClientConfig {
    ClickhouseClientConfig {
        host: args.host.clone().expect("host is set"),
        database: args.database.clone().expect("database is set"),
        username: args.username.clone().expect("username is set"),
        password: args.password.clone().expect("password is set"),
        validation,
    }
}

struct Metrics {}

impl rbuilder_utils::clickhouse::backup::metrics::Metrics for Metrics {
    fn increment_write_failures(err: String) {
        IndexerMetrics::increment_clickhouse_write_failures(err);
    }

    fn process_quantities(quantities: &Quantities) {
        IndexerMetrics::process_clickhouse_quantities(quantities);
    }

    fn record_batch_commit_time(duration: Duration) {
        IndexerMetrics::record_clickhouse_batch_commit_time(duration);
    }

    fn increment_commit_failures(err: String) {
        IndexerMetrics::increment_clickhouse_commit_failures(err);
    }

    fn set_queue_size(size: usize, order: &'static str) {
        IndexerMetrics::set_clickhouse_queue_size(size, order);
    }

    fn set_disk_backup_size(size_bytes: u64, batches: usize, order: &'static str) {
        IndexerMetrics::set_clickhouse_disk_backup_size(size_bytes, batches, order);
    }

    fn increment_backup_disk_errors(order: &'static str, error: &str) {
        IndexerMetrics::increment_clickhouse_backup_disk_errors(order, error);
    }

    fn set_memory_backup_size(size_bytes: u64, batches: usize, order: &'static str) {
        IndexerMetrics::set_clickhouse_memory_backup_size(size_bytes, batches, order);
    }

    fn process_backup_data_lost_quantities(quantities: &Quantities) {
        IndexerMetrics::process_clickhouse_backup_data_lost_quantities(quantities);
    }

    fn process_backup_data_quantities(quantities: &Quantities) {
        IndexerMetrics::process_clickhouse_backup_data_quantities(quantities);
    }

    fn set_backup_empty_size(order: &'static str) {
        IndexerMetrics::set_clickhouse_backup_empty_size(order);
    }
}

/// A namespace struct to spawn a Clickhouse indexer.
#[derive(Debug, Clone)]
pub(crate) struct ClickhouseIndexer;

impl ClickhouseIndexer {
    /// Create and spawn new Clickhouse indexer tasks, returning their indexer handle.
    ///
    /// NOTE: In non-testing setting, validation should be set to false for for performance
    /// reasons, and because validation doesn't support UInt256 data types.
    pub(crate) fn run(
        args: ClickhouseArgs,
        builder_name: String,
        receivers: OrderReceivers,
        task_executor: TaskExecutor,
        validation: bool,
    ) {
        let client = config_from_clickhouse_args(&args, validation).into();
        tracing::info!("Running with clickhouse indexer");

        let (bundles_table_name, bundle_receipts_table_name) =
            (args.bundles_table_name, args.bundle_receipts_table_name);
        let memory_backup_max_size_bytes = args.backup_memory_max_size_bytes;

        let OrderReceivers { bundle_rx, bundle_receipt_rx } = receivers;

        let disk_backup = DiskBackup::<BundleReceiptRow>::new(
            DiskBackupConfig::new()
                .with_path(args.backup_disk_database_path.into())
                .with_max_size_bytes(args.backup_disk_max_size_bytes.into()),
            &task_executor,
        )
        .expect("could not create disk backup");

        let (tx, rx) = mpsc::channel(128);
        let bundle_inserter = default_inserter(&client, &bundles_table_name);
        let bundle_inserter = ClickhouseInserter::<_, Metrics>::new(bundle_inserter, tx);
        let mut bundle_inserter_runner =
            InserterRunner::new(bundle_rx, bundle_inserter, builder_name.clone());

        let mut bundle_backup = Backup::<BundleRow, Metrics>::new(
            rx,
            client
                .inserter(&bundles_table_name)
                .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(4))),
            disk_backup.clone_to(),
        )
        .with_memory_backup_config(MemoryBackupConfig::new(memory_backup_max_size_bytes));

        let (tx, rx) = mpsc::channel(128);
        let bundle_receipt_inserter = default_inserter(&client, &bundle_receipts_table_name);
        let bundle_receipt_inserter =
            ClickhouseInserter::<_, Metrics>::new(bundle_receipt_inserter, tx);
        let mut bundle_receipt_inserter_runner =
            InserterRunner::new(bundle_receipt_rx, bundle_receipt_inserter, builder_name);
        let mut bundle_receipt_backup = Backup::<BundleReceiptRow, Metrics>::new(
            rx,
            client
                .inserter(&bundle_receipts_table_name)
                .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(4))),
            disk_backup,
        )
        .with_memory_backup_config(MemoryBackupConfig::new(memory_backup_max_size_bytes));

        spawn_clickhouse_inserter!(task_executor, bundle_inserter_runner, "bundles", TARGET);
        spawn_clickhouse_backup!(task_executor, bundle_backup, "bundles", TARGET);
        spawn_clickhouse_inserter!(
            task_executor,
            bundle_receipt_inserter_runner,
            "bundle receipts",
            TARGET
        );
        spawn_clickhouse_backup!(task_executor, bundle_receipt_backup, "bundle receipts", TARGET);
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
                ClickhouseClientConfig, ClickhouseIndexer,
            },
            tests::{bundle_receipt_example, system_bundle_example},
            OrderSenders, BUNDLE_RECEIPTS_TABLE_NAME, BUNDLE_TABLE_NAME, TARGET,
        },
    };
    use clickhouse::{error::Result as ClickhouseResult, Client as ClickhouseClient};
    use rbuilder_utils::{
        clickhouse::{
            backup::{metrics::NullMetrics, Backup, DiskBackup, DiskBackupConfig, FailedCommit},
            indexer::default_disk_backup_database_path,
            Quantities,
        },
        spawn_clickhouse_backup,
        tasks::TaskManager,
    };
    use testcontainers::{
        core::{
            error::Result as TestcontainersResult, wait::HttpWaitStrategy, ContainerPort, WaitFor,
        },
        runners::AsyncRunner as _,
        ContainerAsync, Image,
    };
    use tokio::{runtime::Handle, sync::mpsc};

    // Uncomment to enable logging during tests.
    // use tracing::level_filters::LevelFilter;
    // use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

    /// The default clickhouse image name to use for testcontainers testing.
    pub(crate) const CLICKHOUSE_DEFAULT_IMAGE_NAME: &str = "clickhouse/clickhouse-server";
    /// The default clickhouse image tag to use for testcontainers testing.
    pub(crate) const CLICKHOUSE_DEFAULT_IMAGE_TAG: &str = "25.6.2.5";
    /// Port that the [`ClickHouse`] container has internally, for testcontainers testing.
    /// Can be rebound externally via [`testcontainers::core::ImageExt::with_mapped_port`].
    pub(crate) const CLICKHOUSE_PORT: ContainerPort = ContainerPort::Tcp(8123);

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
    pub(crate) struct ClickhouseImage {
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

    impl From<ClickhouseClientConfig> for ClickhouseArgs {
        fn from(config: ClickhouseClientConfig) -> Self {
            Self {
                host: Some(config.host),
                database: Some("default".to_string()),
                username: Some(config.username),
                password: Some(config.password),
                bundles_table_name: BUNDLE_TABLE_NAME.to_string(),
                bundle_receipts_table_name: BUNDLE_RECEIPTS_TABLE_NAME.to_string(),
                backup_memory_max_size_bytes: 1024 * 1024 * 10, // 10MiB
                backup_disk_database_path: default_disk_backup_database_path(),
                backup_disk_max_size_bytes: 1024 * 1024 * 100, // 100MiB
            }
        }
    }

    /// Create a test clickhouse client using testcontainers. Returns both the image and the
    /// client.
    ///
    /// IMPORTANT: the image must be manually `drop`ped at the end of the test, otherwise the
    /// container is cancelled prematurely.
    pub(crate) async fn create_test_clickhouse_client(
        validation: bool,
    ) -> TestcontainersResult<(
        ContainerAsync<ClickhouseImage>,
        ClickhouseClient,
        ClickhouseClientConfig,
    )> {
        // Start a Docker client (testcontainers manages lifecycle)
        let clickhouse = ClickhouseImage::default().start().await?;
        let port = clickhouse.get_host_port_ipv4(8123).await?;
        let host = clickhouse.get_host().await?;
        let url = format!("http://{host}:{port}");

        let config = ClickhouseClientConfig {
            host: url,
            database: "default".to_string(),
            username: "default".to_string(),
            password: "password".to_string(),
            validation,
        };

        Ok((
            clickhouse,
            ClickhouseClient::default()
                .with_url(config.host.clone())
                .with_user(config.username.clone())
                .with_password(config.password.clone())
                .with_validation(config.validation),
            config,
        ))
    }

    /// Creates the bundles table from the DDL present inside the `fixtures` folder.
    pub(crate) async fn create_clickhouse_bundles_table(
        client: &ClickhouseClient,
    ) -> ClickhouseResult<()> {
        let create_bundles_table_ddl = fs::read_to_string("./fixtures/create_bundles_table.sql")
            .expect("could not read create_bundles_table.sql");
        client.query(&create_bundles_table_ddl).execute().await
    }

    /// Creates the bundle receipts table from the DDL present inside the `fixtures` folder.
    pub(crate) async fn create_clickhouse_bundle_receipts_table(
        client: &ClickhouseClient,
    ) -> ClickhouseResult<()> {
        let create_bundle_receipts_table_ddl =
            fs::read_to_string("./fixtures/create_bundle_receipts_table.sql")
                .expect("could not read create_bundle_receipts_table.sql");
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
        // let registry = tracing_subscriber::registry().with(
        //     EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).
        // from_env_lossy(), );
        // let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

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
    async fn clickhouse_bundle_receipts_rows_e2e_succeds() {
        // Uncomment to toggle logs.
        // let registry = tracing_subscriber::registry().with(
        //     EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).
        // from_env_lossy(), );
        // let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        // 1. Spin up Clickhouse
        let (image, client, config) = create_test_clickhouse_client(true).await.unwrap();
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

    // Uncomment to enable logging during tests.
    // use tracing::level_filters::LevelFilter;
    // use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

    #[tokio::test(flavor = "multi_thread")]
    async fn backup_e2e_works() {
        // Uncomment to toggle logs.
        // let registry = tracing_subscriber::registry().with(
        //     EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).
        // from_env_lossy(), );
        // let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        let memory_backup_only = [false, true];

        let task_manager = TaskManager::new(tokio::runtime::Handle::current());
        let task_executor = task_manager.executor();

        for use_memory_only in memory_backup_only {
            println!(
                "---- Running backup_memory_e2e_works with use_memory_only = {use_memory_only} ----"
            );

            // 1. Spin up Clickhouse. No validation because we're testing both receipts and bundles,
            // and validation on U256 is not supported.
            let (image, client, _) = create_test_clickhouse_client(false).await.unwrap();
            create_clickhouse_bundles_table(&client).await.unwrap();

            let tempfile = tempfile::NamedTempFile::new().unwrap();

            let disk_backup = DiskBackup::new(
                DiskBackupConfig::new().with_path(tempfile.path().to_path_buf().into()),
                &task_executor,
            )
            .expect("could not create disk backup");

            let (tx, rx) = mpsc::channel(128);
            let mut bundle_backup = Backup::<BundleRow, NullMetrics>::new_test(
                rx,
                client
                    .inserter(BUNDLE_TABLE_NAME)
                    .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(12))),
                disk_backup,
                use_memory_only,
            );

            spawn_clickhouse_backup!(task_executor, bundle_backup, "bundles", TARGET);

            let quantities = Quantities { bytes: 512, rows: 1, transactions: 1 }; // approximated
            let bundle_row: BundleRow = (system_bundle_example(), "buildernet".to_string()).into();
            let bundle_rows = Vec::from([bundle_row]);
            let failed_commit = FailedCommit::<BundleRow>::new(bundle_rows.clone(), quantities);

            tx.send(failed_commit).await.unwrap();
            // Wait some time to let the backup process it
            tokio::time::sleep(Duration::from_millis(100)).await;

            let results = client
                .query(&format!("select * from {BUNDLE_TABLE_NAME}"))
                .fetch_all::<BundleRow>()
                .await
                .unwrap();

            assert_eq!(results.len(), 1);
            assert_eq!(bundle_rows, results, "expected, got");

            drop(image);
        }
    }
}

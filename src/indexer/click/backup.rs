#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::indexer::{
        click::{
            models::BundleRow,
            tests::{create_clickhouse_bundles_table, create_test_clickhouse_client},
        },
        tests::system_bundle_example,
        BUNDLE_TABLE_NAME, TARGET,
    };
    use rbuilder_utils::{
        clickhouse::{
            backup::{metrics::NullMetrics, Backup, DiskBackup, DiskBackupConfig, FailedCommit},
            Quantities,
        },
        spawn_clickhouse_backup,
        tasks::TaskManager,
    };
    use tokio::sync::mpsc;

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

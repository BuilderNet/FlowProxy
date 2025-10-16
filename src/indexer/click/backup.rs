use std::{
    collections::VecDeque,
    marker::PhantomData,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use clickhouse::inserter::{Inserter, Quantities};
use derive_more::{Deref, DerefMut};
use redb::TableDefinition;
use tokio::sync::mpsc;

use crate::{
    indexer::click::ClickhouseIndexableOrder, metrics::IndexerMetrics,
    primitives::backoff::BackoffInterval,
};

/// A default maximum size in bytes for the in-memory backup of failed commits.
pub(crate) const MAX_MEMORY_BACKUP_SIZE_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

/// The default path where the backup database is stored.
pub(crate) const DISK_BACKUP_DATABASE_PATH: &str =
    "/var/lib/buildernet-orderflow-proxy/clickhouse_backup.db";

/// Tracing target for the backup actor.
const TARGET: &str = "indexer::backup";

fn unix_now() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_millis()
}

/// Represents data we failed to commit to clickhouse, including the rows and some information
/// about the size of such data.
pub(crate) struct FailedCommit<T: ClickhouseIndexableOrder> {
    /// The actual rows we were trying to commit.
    rows: Vec<T::ClickhouseRowType>,
    /// The quantities related to such commit, like the total size in bytes.
    quantities: Quantities,
}

impl<T: ClickhouseIndexableOrder> FailedCommit<T> {
    pub(crate) fn new(rows: Vec<T::ClickhouseRowType>, quantities: Quantities) -> Self {
        Self { rows, quantities }
    }
}

/// A wrapper over a [`VecDeque`] of [`FailedCommit`] with added functionality.
///
/// Newly failed commits are pushed to the front of the queue, so the oldest are at the back.
struct FailedCommits<T: ClickhouseIndexableOrder> {
    inner: VecDeque<FailedCommit<T>>,
    /// Aggregated quantities of all the failed commits.
    total_quantities: Quantities,
}

impl<T: ClickhouseIndexableOrder> FailedCommits<T> {
    /// Push a new failed commit to the front of the queue, updating the aggregated quantities.
    fn push_front(&mut self, value: FailedCommit<T>) -> (Quantities, usize) {
        self.inner.push_front(value);
        self.update_quantities()
    }

    /// Push back the oldest failed commit to the back of the queue, updating the aggregated
    /// quantities.
    fn push_back(&mut self, value: FailedCommit<T>) -> (Quantities, usize) {
        self.inner.push_back(value);
        self.update_quantities()
    }

    /// Get the oldest failed commit from the back of the queue, updating the aggregated quantities.
    fn pop_back(&mut self) -> Option<FailedCommit<T>> {
        let res = self.inner.pop_back();
        self.update_quantities();
        res
    }

    /// Drain all the failed commits from the queue, updating the aggregated quantities.
    fn drain(&mut self, range: std::ops::RangeFull) -> impl Iterator<Item = FailedCommit<T>> + '_ {
        self.zeroize_quantities();
        let res = self.inner.drain(range);
        res
    }

    /// Get the number of failed commits currently in the queue.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Triggering a recalculation of the aggregated quantities.
    fn update_quantities(&mut self) -> (Quantities, usize) {
        let total_size_bytes = self.inner.iter().map(|c| c.quantities.bytes).sum::<u64>();
        let total_rows = self.inner.iter().map(|c| c.quantities.rows).sum::<u64>();
        let total_transactions = self.inner.iter().map(|c| c.quantities.transactions).sum::<u64>();

        self.total_quantities = Quantities {
            bytes: total_size_bytes,
            rows: total_rows,
            transactions: total_transactions,
        };

        self.quantities()
    }

    /// Zeroizing the aggregated quantities.
    fn zeroize_quantities(&mut self) {
        self.total_quantities = Quantities::ZERO;
    }

    /// Get the aggregated quantities and the number of failed commits.
    fn quantities(&self) -> (Quantities, usize) {
        (self.total_quantities.clone(), self.inner.len())
    }
}

impl<T: ClickhouseIndexableOrder> Default for FailedCommits<T> {
    fn default() -> Self {
        Self { inner: VecDeque::default(), total_quantities: Quantities::ZERO }
    }
}

/// Configuration for the [`DiskBackup`] of failed commits.
#[derive(Debug, Clone)]
pub(crate) struct DiskBackupConfig {
    /// The path where the backup database is stored.
    path: PathBuf,
    /// The name of the table to store data into.
    table_name: String,
}

impl DiskBackupConfig {
    pub fn new(path: PathBuf, table_name: String) -> Self {
        Self { path, table_name }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MemoryBackupConfig {
    /// The maximum size in bytes for holding past failed commits in-memory. Once we go over this threshold,
    /// pressure is applied and old commits are dropped.
    pub memory_max_size_bytes: u64,
}

impl MemoryBackupConfig {
    pub fn new(memory_max_size_bytes: u64) -> Self {
        Self { memory_max_size_bytes }
    }
}

impl Default for MemoryBackupConfig {
    fn default() -> Self {
        Self { memory_max_size_bytes: MAX_MEMORY_BACKUP_SIZE_BYTES }
    }
}

/// Configuration for the [`Backup`] actor.
#[derive(Debug)]
pub(crate) struct BackupConfig {
    /// The interval at which we try to backup data.
    pub interval: BackoffInterval,
    /// The configuration for the disk backup.
    pub disk_backup_config: DiskBackupConfig,
    /// The configuration for the in-memory backup.
    pub memory_backup_config: MemoryBackupConfig,
}

impl BackupConfig {
    pub fn new(
        interval: BackoffInterval,
        disk_backup_config: DiskBackupConfig,
        memory_backup_config: MemoryBackupConfig,
    ) -> Self {
        Self { interval, disk_backup_config, memory_backup_config }
    }
}

/// A disk backup for failed commits. This handle to a database allows to write only to one table
/// for scoped access. If you want to write to another table, clone it using [`Self::clone_with_table`].
#[derive(Debug, Clone)]
pub(crate) struct DiskBackup<T> {
    db: Arc<RwLock<redb::Database>>,
    config: DiskBackupConfig,

    _marker: PhantomData<T>,
}

impl<T> DiskBackup<T> {
    pub fn new(config: DiskBackupConfig) -> Result<Self, redb::DatabaseError> {
        let db = redb::Database::create(&config.path)?;

        Ok(Self { db: Arc::new(RwLock::new(db)), config, _marker: Default::default() })
    }

    /// Like [`Clone`], but allows to change table name.
    pub fn clone_change_table<U: ClickhouseIndexableOrder>(
        &self,
        table_name: String,
    ) -> DiskBackup<U> {
        DiskBackup {
            db: self.db.clone(),
            config: DiskBackupConfig { path: self.config.path.clone(), table_name },
            _marker: Default::default(),
        }
    }
}

impl<T: ClickhouseIndexableOrder> DiskBackup<T> {
    pub fn write(&mut self, data: FailedCommit<T>) -> Result<(), redb::Error> {
        let table_def = TableDefinition::<u128, FailedCommit<T>>::new(&self.config.table_name);

        let writer = self.db.write().expect("not poisoned").begin_write()?;
        {
            let mut table = writer.open_table(table_def)?;
            table.insert(unix_now(), data);
        }

        Ok(())
    }
}

#[derive(Deref, DerefMut)]
struct MemoryBackup<T: ClickhouseIndexableOrder> {
    /// The in-memory cache of failed commits.
    #[deref]
    #[deref_mut]
    failed_commits: FailedCommits<T>,

    /// The configuration for the in-memory backup.
    config: MemoryBackupConfig,
}

impl<T: ClickhouseIndexableOrder> MemoryBackup<T> {
    fn new(config: MemoryBackupConfig) -> Self {
        Self { failed_commits: FailedCommits::default(), config }
    }
}

// Needed otherwise requires T: Default
impl<T: ClickhouseIndexableOrder> Default for MemoryBackup<T> {
    fn default() -> Self {
        Self { failed_commits: FailedCommits::default(), config: MemoryBackupConfig::default() }
    }
}

// Rationale for sending multiple rows instead of sending rows: the backup abstraction must
// periodically block to write data to the inserter and try to commit it to clickhouse. Each
// attempt results in doing the previous step. This could clog the channel which will receive
// individual rows, leading to potential row losses.
//
// By sending backup data less often, we give time gaps for these operation to be performed.

/// An in-memory backup actor for Clickhouse data. This actor receives [`FailedCommit`]s and keeps
/// them in memory, and periodically tries to commit them back again to Clickhouse. Since memory
/// is finite, there is an upper bound on how much memory this data structure holds. Once this has
/// been hit, pressure applies, meaning that we try again a certain failed commit for a finite
/// number of times, and then we discard it to accomdate new data.
pub(crate) struct Backup<T: ClickhouseIndexableOrder> {
    /// The receiver of failed commit attempts.
    rx: mpsc::Receiver<FailedCommit<T>>,
    /// The disk cache of failed commits.
    disk_backup: DiskBackup<T>,
    /// The in-memory cache of failed commits.
    memory_backup: MemoryBackup<T>,
    /// A clickhouse inserter for committing again the data.
    inserter: Inserter<T::ClickhouseRowType>,
    /// The interval at which we try to backup data.
    interval: BackoffInterval,
}

impl<T: ClickhouseIndexableOrder> Backup<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<FailedCommit<T>>,
        inserter: Inserter<T::ClickhouseRowType>,
        disk_backup: DiskBackup<T>,
    ) -> Self {
        let backup = Self {
            rx,
            inserter,
            interval: Default::default(),
            memory_backup: MemoryBackup::default(),
            disk_backup,
        };

        backup
    }

    #[allow(dead_code)]
    pub(crate) fn with_memory_backup_config(mut self, config: MemoryBackupConfig) -> Self {
        self.memory_backup.config = config;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_disk_backup_config(mut self, config: DiskBackupConfig) -> Self {
        self.disk_backup.config = config;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_interval(mut self, interval: BackoffInterval) -> Self {
        self.interval = interval;
        self
    }

    /// Run the backup actor until it is possible to receive messages.
    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                maybe_failed_commit = self.rx.recv() => {
                    let Some(failed_commit) = maybe_failed_commit else {
                        tracing::error!(target: TARGET, order = T::ORDER_TYPE, "backup channel closed");
                        break;
                    };

                    let quantities = failed_commit.quantities.clone();
                    let (Quantities { bytes: total_size_bytes, .. }, new_len) = self.failed_commits.push_front(failed_commit);
                    IndexerMetrics::set_clickhouse_backup_size(total_size_bytes, new_len, T::ORDER_TYPE);

                    tracing::debug!(target: TARGET, order = T::ORDER_TYPE,
                        bytes = ?quantities.bytes, rows = ?quantities.rows, total_size_bytes, total_batches = self.failed_commits.len(),
                        "received failed commit to backup"
                    );

                    if total_size_bytes > self.max_size_bytes && self.failed_commits.len() > 1 {
                        tracing::warn!(target: TARGET, order = T::ORDER_TYPE,
                            total_size_bytes, max_size_bytes = self.max_size_bytes, "failed commits exceeded max size, dropping oldest failed commit");
                        let oldest = self.failed_commits.pop_back().expect("length checked above");
                        IndexerMetrics::process_clickhouse_backup_data_lost_quantities(&oldest.quantities);
                    }
                }
                _ = self.interval.tick() => {
                    let Some(oldest) = self.failed_commits.pop_back() else {
                        self.interval.reset();
                        IndexerMetrics::set_clickhouse_backup_size(0, 0, T::ORDER_TYPE);
                        continue // Nothing to do!
                    };

                    for row in &oldest.rows {
                        let value_ref = T::to_row_ref(row);

                        if let Err(e) = self.inserter.write(value_ref).await {
                            IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
                            tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, "failed to write to backup inserter");
                            continue;
                        }
                    }

                    let start = Instant::now();
                    match self.inserter.force_commit().await {
                        Ok(quantities) => {
                            tracing::info!(target: TARGET, order = T::ORDER_TYPE, ?quantities, "successfully backed up");
                            IndexerMetrics::process_clickhouse_backup_data_quantities(&quantities);
                            IndexerMetrics::record_clickhouse_batch_commit_time(start.elapsed());
                            self.interval.reset();
                        }
                        Err(e) => {
                            tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, quantities = ?oldest.quantities, "failed to commit bundle to clickhouse from backup");
                            IndexerMetrics::increment_clickhouse_commit_failures(e.to_string());
                            self.failed_commits.push_back(oldest);
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// To call on shutdown, tries make a last-resort attempt to backup all the data.
    pub(crate) async fn end(mut self) {
        for failed_commit in self.failed_commits.drain(..) {
            for row in &failed_commit.rows {
                let value_ref = T::to_row_ref(row);

                if let Err(e) = self.inserter.write(value_ref).await {
                    tracing::error!( target: TARGET, order = T::ORDER_TYPE, ?e, "failed to write to backup inserter during shutdown");
                    IndexerMetrics::increment_clickhouse_write_failures(e.to_string());
                    continue;
                }
            }
            if let Err(e) = self.inserter.force_commit().await {
                tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, "failed to commit backup during shutdown");
                IndexerMetrics::increment_clickhouse_commit_failures(e.to_string());
            }
        }

        if let Err(e) = self.inserter.end().await {
            tracing::error!(target: TARGET, order = T::ORDER_TYPE, ?e, "failed to end backup inserter during shutdown");
        }
    }
}

impl<T: ClickhouseIndexableOrder> std::fmt::Debug for Backup<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBackup")
            .field("rx", &self.rx)
            .field("inserter", &T::ORDER_TYPE.to_string())
            .field("failed_commits", &self.failed_commits.len())
            .field("max_size_bytes", &self.max_size_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use crate::{
        indexer::{
            click::{
                models::BundleRow,
                tests::{create_clickhouse_bundles_table, create_test_clickhouse_client},
            },
            tests::system_bundle_example,
            BUNDLE_TABLE_NAME,
        },
        primitives::SystemBundle,
        spawn_clickhouse_backup,
        tasks::TaskManager,
    };

    // Uncomment to enable logging during tests.
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

    #[tokio::test(flavor = "multi_thread")]
    async fn backup_memory_e2e_works() {
        // Uncomment to toggle logs.
        let registry = tracing_subscriber::registry().with(
            EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).from_env_lossy(),
        );
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        let task_manager = TaskManager::new(tokio::runtime::Handle::current());
        let task_executor = task_manager.executor();

        // 1. Spin up Clickhouse. No validation because we're testing both receipts and bundles,
        // and validation on U256 is not supported.
        let (image, client, _) = create_test_clickhouse_client(false).await.unwrap();
        create_clickhouse_bundles_table(&client).await.unwrap();

        let (tx, rx) = mpsc::channel::<FailedCommit<SystemBundle>>(128);
        let mut bundle_backup = Backup::new(
            rx,
            client
                .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
                .with_timeouts(Some(Duration::from_secs(2)), Some(Duration::from_secs(12))),
        )
        .with_memory_max_size_bytes(MAX_MEMORY_BACKUP_SIZE_BYTES);

        spawn_clickhouse_backup!(task_executor, bundle_backup, "bundles");

        let quantities = Quantities { bytes: 512, rows: 1, transactions: 1 }; // approximated
        let bundle_row: BundleRow = (system_bundle_example(), "buildernet".to_string()).into();
        let bundle_rows = Vec::from([bundle_row]);
        let failed_commit = FailedCommit::<SystemBundle>::new(bundle_rows.clone(), quantities);

        tx.send(failed_commit).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await; // Wait some time to let the backup process it.

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

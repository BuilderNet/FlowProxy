use std::{collections::VecDeque, time::Instant};

use clickhouse::inserter::{Inserter, Quantities};
use tokio::sync::mpsc;

use crate::{
    indexer::click::ClickhouseIndexableOrder, metrics::IndexerMetrics,
    primitives::backoff::BackoffInterval,
};

/// A default maximum size in bytes for the in-memory backup of failed commits.
pub(crate) const MAX_BACKUP_SIZE_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

/// Tracing target for the backup actor.
const TARGET: &str = "indexer::backup";

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
pub(crate) struct MemoryBackup<T: ClickhouseIndexableOrder> {
    /// The receiver of failed commit attempts.
    rx: mpsc::Receiver<FailedCommit<T>>,
    /// The in-memory cache of failed commits.
    failed_commits: FailedCommits<T>,
    /// A clickhouse inserter for committing again the data.
    inserter: Inserter<T::ClickhouseRowType>,
    /// The interval at which we try to backup data.
    interval: BackoffInterval,
    /// The maximum size in bytes for holding past failed commits. Once we go over this threshold,
    /// pressure is applied.
    max_size_bytes: u64,
}

impl<T: ClickhouseIndexableOrder> MemoryBackup<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<FailedCommit<T>>,
        inserter: Inserter<T::ClickhouseRowType>,
    ) -> Self {
        Self {
            rx,
            inserter,
            interval: Default::default(),
            failed_commits: Default::default(),
            max_size_bytes: MAX_BACKUP_SIZE_BYTES,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_size_bytes(mut self, max_size_bytes: u64) -> Self {
        self.max_size_bytes = max_size_bytes;
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
                    IndexerMetrics::set_clickhouse_backup_size_bytes(total_size_bytes, T::ORDER_TYPE);
                    IndexerMetrics::set_clickhouse_backup_size_batches(new_len, T::ORDER_TYPE);

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
                        IndexerMetrics::set_clickhouse_backup_size_bytes(0, T::ORDER_TYPE);
                        IndexerMetrics::set_clickhouse_backup_size_batches(0, T::ORDER_TYPE);
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

impl<T: ClickhouseIndexableOrder> std::fmt::Debug for MemoryBackup<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBackup")
            .field("rx", &self.rx)
            .field("inserter", &T::ORDER_TYPE.to_string())
            .field("failed_commits", &self.failed_commits.len())
            .field("max_size_bytes", &self.max_size_bytes)
            .finish()
    }
}

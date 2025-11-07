use crate::metrics::WorkerMetrics;

use super::Priority;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::{sync::Arc, time::Instant};
use tokio::sync::{oneshot, Semaphore};

const DEFAULT_HIGH_QUEUE_SIZE: usize = Semaphore::MAX_PERMITS;
const DEFAULT_MEDIUM_QUEUE_SIZE: usize = 50_000;
const DEFAULT_LOW_QUEUE_SIZE: usize = 5_000;

/// Priority level permits for processing and validating user requests.
/// See [`Priority`] for details.
#[derive(Debug, Clone)]
pub struct PriorityWorkers {
    /// Permits for [`Priority::High`].
    high: Arc<Semaphore>,
    high_size: usize,
    /// Permits for [`Priority::Medium`].
    medium: Arc<Semaphore>,
    medium_size: usize,
    /// Permits for [`Priority::Low`].
    low: Arc<Semaphore>,
    low_size: usize,
    /// Metrics for the priority workers.
    metrics: WorkerMetrics,
    /// Thread pool for the worker tasks.
    pool: Arc<ThreadPool>,
}

impl PriorityWorkers {
    /// Create new priority queues with configured sizes.
    pub fn new(high: usize, medium: usize, low: usize, worker_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(worker_threads)
            .thread_name(|index| format!("priority-worker-{}", index))
            .build()
            .expect("failed to create Rayon thread pool");

        tracing::info!(threads = worker_threads, "created Rayon thread pool");

        Self {
            high: Arc::new(Semaphore::new(high)),
            high_size: high,
            medium: Arc::new(Semaphore::new(medium)),
            medium_size: medium,
            low: Arc::new(Semaphore::new(low)),
            low_size: low,
            metrics: WorkerMetrics::default(),
            pool: Arc::new(pool),
        }
    }

    pub fn new_with_threads(worker_threads: usize) -> Self {
        Self::new(
            DEFAULT_HIGH_QUEUE_SIZE,
            DEFAULT_MEDIUM_QUEUE_SIZE,
            DEFAULT_LOW_QUEUE_SIZE,
            worker_threads,
        )
    }

    /// Return priority queue for the given priority.
    fn semaphore_for(&self, priority: Priority) -> &Semaphore {
        match priority {
            Priority::High => &self.high,
            Priority::Medium => &self.medium,
            Priority::Low => &self.low,
        }
    }

    /// Return the available permits for the given priority.
    pub fn available_permits_for(&self, priority: Priority) -> usize {
        self.semaphore_for(priority).available_permits()
    }

    /// Return the total permits for the given priority.
    pub fn total_permits_for(&self, priority: Priority) -> usize {
        match priority {
            Priority::High => self.high_size,
            Priority::Medium => self.medium_size,
            Priority::Low => self.low_size,
        }
    }

    /// Return the active permits for the given priority.
    pub fn active_permits_for(&self, priority: Priority) -> usize {
        self.total_permits_for(priority) - self.available_permits_for(priority)
    }

    /// Spawn the task with given priority. These tasks are reserved for computationally expensive
    /// operations, and will be executed on a thread from the Rayon thread pool.
    pub async fn spawn_with_priority<R, F>(&self, priority: Priority, f: F) -> R
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let start = Instant::now();
        let semaphore = self.semaphore_for(priority);
        let (tx, rx) = oneshot::channel();
        let _permit = semaphore.acquire().await;

        // NOTE: Normal `[ThreadPool::spawn]` executes tasks in a LIFO manner, potentially starving
        // earlier tasks. They claim this is for better cache locality, but this is less
        // important here, so we use `spawn_fifo` instead to ensure fairness.
        self.pool.spawn_fifo(|| {
            let result = f();
            let _ = tx.send(result);
        });

        let result = rx.await.expect("failed to receive result from worker task");

        let elapsed = start.elapsed();
        self.metrics.task_durations(priority.as_str()).observe(elapsed.as_secs_f64());

        result
    }
}

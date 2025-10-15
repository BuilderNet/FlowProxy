use super::Priority;
use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};

const DEFAULT_HIGH_QUEUE_SIZE: usize = Semaphore::MAX_PERMITS;
const DEFAULT_MEDIUM_QUEUE_SIZE: usize = 50_000;
const DEFAULT_LOW_QUEUE_SIZE: usize = 5_000;

/// Priority level permits for processing and validating user requests.
/// See [`Priority`] for details.
#[derive(Debug)]
pub struct PriorityQueues {
    /// Permits for [`Priority::High`].
    high: Arc<Semaphore>,
    high_size: usize,
    /// Permits for [`Priority::Medium`].
    medium: Arc<Semaphore>,
    medium_size: usize,
    /// Permits for [`Priority::Low`].
    low: Arc<Semaphore>,
    low_size: usize,
}

/// Opinionated priority queues defaults.
impl Default for PriorityQueues {
    fn default() -> Self {
        Self {
            high: Arc::new(Semaphore::new(DEFAULT_HIGH_QUEUE_SIZE)),
            high_size: DEFAULT_HIGH_QUEUE_SIZE,
            medium: Arc::new(Semaphore::new(DEFAULT_MEDIUM_QUEUE_SIZE)),
            medium_size: DEFAULT_MEDIUM_QUEUE_SIZE,
            low: Arc::new(Semaphore::new(DEFAULT_LOW_QUEUE_SIZE)),
            low_size: DEFAULT_LOW_QUEUE_SIZE,
        }
    }
}

impl PriorityQueues {
    /// Create new priority queues with configured sizes.
    pub fn new(high: usize, medium: usize, low: usize) -> Self {
        Self {
            high: Arc::new(Semaphore::new(high)),
            high_size: high,
            medium: Arc::new(Semaphore::new(medium)),
            medium_size: medium,
            low: Arc::new(Semaphore::new(low)),
            low_size: low,
        }
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

    /// Spawn the task with given priority.
    pub async fn spawn_with_priority<R, F>(&self, priority: Priority, f: F) -> R
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let semaphore = self.semaphore_for(priority);
        let _permit = semaphore.acquire().await;
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = tx.send(f());
        });
        rx.await.unwrap()
    }
}

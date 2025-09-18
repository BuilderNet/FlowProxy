use super::Priority;
use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};

/// Priority level permits for processing and validating user requests.
/// See [`Priority`] for details.
#[derive(Debug)]
pub struct PriorityQueues {
    /// Permits for [`Priority::High`].
    high: Arc<Semaphore>,
    /// Permits for [`Priority::Medium`].
    medium: Arc<Semaphore>,
    /// Permits for [`Priority::Low`].
    low: Arc<Semaphore>,
}

/// Opinionated priority queues defaults.
impl Default for PriorityQueues {
    fn default() -> Self {
        Self {
            high: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            medium: Arc::new(Semaphore::new(10_000)),
            low: Arc::new(Semaphore::new(1_000)),
        }
    }
}

impl PriorityQueues {
    /// Create new priority queues with configured sizes.
    pub fn new(high: usize, medium: usize, low: usize) -> Self {
        Self {
            high: Arc::new(Semaphore::new(high)),
            medium: Arc::new(Semaphore::new(medium)),
            low: Arc::new(Semaphore::new(low)),
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

use std::time::Duration;

use mini_moka::sync::Cache;
use uuid::Uuid;

/// A thread-safe, in-memory cache for deduplicating order IDs.
#[derive(Debug, Clone)]
pub struct OrderCache {
    /// The inner cache.
    cache: Cache<Uuid, ()>,
}

impl OrderCache {
    /// Create a new order cache with the given TTL and size.
    pub fn new(cache_ttl: u64, cache_size: u64) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl))
                .max_capacity(cache_size)
                .build(),
        }
    }

    /// Insert an order ID into the cache.
    pub fn insert(&self, id: Uuid) {
        self.cache.insert(id, ());
    }

    /// Check if an order ID is in the cache.
    pub fn contains(&self, id: Uuid) -> bool {
        self.cache.contains_key(&id)
    }
}

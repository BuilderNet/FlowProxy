use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use alloy_primitives::{Address, B256};
use mini_moka::sync::Cache;

/// A thread-safe, in-memory cache for deduplicating order IDs.
#[derive(Debug, Clone)]
pub struct OrderCache {
    /// The inner cache.
    cache: Cache<B256, ()>,
    /// The number of hits.
    hits: Arc<AtomicUsize>,
    /// The number of misses.
    misses: Arc<AtomicUsize>,
}

impl OrderCache {
    /// Create a new order cache with the given TTL and size.
    /// Includes metrics for hits and misses that are ONLY updated when [`Self::contains`] is
    /// called.
    pub fn new(cache_ttl: u64, cache_size: u64) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl))
                .max_capacity(cache_size)
                .build(),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the hit ratio of the cache.
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        hits as f64 / (hits + misses) as f64
    }

    /// Insert an order ID into the cache.
    pub fn insert(&self, key: B256) {
        self.cache.insert(key, ());
    }

    /// Check if an order ID is in the cache.
    /// Updates the metrics for hits and misses.
    pub fn contains(&self, id: &B256) -> bool {
        if self.cache.contains_key(id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

/// A thread-safe, in-memory LRU cache for mapping transactions hashes to recovered signers.
#[derive(Debug, Clone)]
pub struct SignerCache {
    /// The inner cache.
    cache: Cache<B256, Address>,
    /// The number of hits.
    hits: Arc<AtomicUsize>,
    /// The number of misses.
    misses: Arc<AtomicUsize>,
}

impl SignerCache {
    /// Create a new signer cache with the given TTL and size.
    /// Includes metrics for hits and misses that are ONLY updated when [`Self::get`] is called.
    pub fn new(cache_ttl: u64, cache_size: u64) -> Self {
        Self {
            // NOTE: adding a max capacity with `cache_size` makes this a LRU cache.
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl))
                .max_capacity(cache_size)
                .build(),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the hit ratio of the cache.
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        hits as f64 / (hits + misses) as f64
    }

    /// Insert a transaction hash and its recovered signer into the cache.
    pub fn insert(&self, tx_hash: B256, signer: Address) {
        // TODO: perhaps we should make a "no-op"-like hasher?
        self.cache.insert(tx_hash, signer);
    }

    /// Get the recovered signer for a transaction hash from the cache.
    /// Updates the metrics for hits and misses.
    pub fn get(&self, tx_hash: &B256) -> Option<Address> {
        match self.cache.get(tx_hash) {
            Some(address) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(address)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

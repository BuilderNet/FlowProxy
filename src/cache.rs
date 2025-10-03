use std::time::Duration;

use alloy_primitives::{Address, B256};
use mini_moka::sync::Cache;

/// A thread-safe, in-memory cache for deduplicating order IDs.
#[derive(Debug, Clone)]
pub struct OrderCache {
    /// The inner cache.
    cache: Cache<B256, ()>,
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
    pub fn insert(&self, key: B256) {
        self.cache.insert(key, ());
    }

    /// Check if an order ID is in the cache.
    pub fn contains(&self, id: &B256) -> bool {
        self.cache.contains_key(id)
    }
}

/// A thread-safe, in-memory LRU cache for mapping transactions hashes to recovered signers.
#[derive(Debug, Clone)]
pub struct SignerCache {
    /// The inner cache.
    cache: Cache<B256, Address>,
}

impl SignerCache {
    /// Create a new signer cache with the given TTL and size.
    pub fn new(cache_ttl: u64, cache_size: u64) -> Self {
        Self {
            // NOTE: adding a max capacity with `cache_size` makes this a LRU cache.
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl))
                .max_capacity(cache_size)
                .build(),
        }
    }

    /// Insert a transaction hash and its recovered signer into the cache.
    pub fn insert(&self, tx_hash: B256, signer: Address) {
        // TODO: perhaps we should make a "no-op"-like hasher?
        self.cache.insert(tx_hash, signer);
    }

    /// Get the recovered signer for a transaction hash from the cache.
    pub fn get(&self, tx_hash: &B256) -> Option<Address> {
        self.cache.get(tx_hash)
    }
}

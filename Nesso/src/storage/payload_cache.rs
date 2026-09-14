use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub const DEFAULT_PAYLOAD_CACHE_CAPACITY: usize = 10_000;

/// An in-memory hot-path payload cache for recently pushed tasks.
///
/// Analogous to Postgres `shared_buffers` or RocksDB's block cache, this cache
/// retains task payloads in memory upon `push()`, allowing `pop_and_lease()`
/// to return the original payload in nanoseconds without performing physical
/// disk reads or acquiring `WalReader` locks.
///
/// # Durability & Safety Invariants
/// - The WAL remains the **sole durable source of truth**. Every task is durably
///   appended to the WAL before being inserted into `PayloadCache`.
/// - If a payload is not in cache (cache miss, eviction under memory bounds, or
///   following daemon restart), the engine seamlessly falls back to reading from disk.
/// - Zero data loss is possible.
#[derive(Debug)]
pub struct PayloadCache {
    capacity: usize,
    entries: HashMap<u64, Arc<Vec<u8>>>,
    order: VecDeque<u64>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl PayloadCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::with_capacity(capacity.min(1024)),
            order: VecDeque::with_capacity(capacity.min(1024)),
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    /// Looks up a payload by task ID. Returns `Some(Arc<Vec<u8>>)` on hit,
    /// or `None` on miss.
    pub fn get(&mut self, id: u64) -> Option<Arc<Vec<u8>>> {
        if let Some(payload) = self.entries.get(&id) {
            self.hits += 1;
            Some(Arc::clone(payload))
        } else {
            self.misses += 1;
            None
        }
    }

    /// Inserts a task payload into the cache. If the cache exceeds capacity,
    /// the oldest unreferenced entry is evicted.
    pub fn insert(&mut self, id: u64, payload: Arc<Vec<u8>>) {
        if self.capacity == 0 {
            return;
        }

        if self.entries.insert(id, payload).is_none() {
            self.order.push_back(id);
        }

        while self.entries.len() > self.capacity {
            if let Some(oldest_id) = self.order.pop_front() {
                if self.entries.remove(&oldest_id).is_some() {
                    self.evictions += 1;
                    break;
                }
            } else {
                break;
            }
        }

        // Periodic hygiene: compact order queue if stale tombstones accumulate
        if self.order.len() > self.capacity * 2 {
            let entries = &self.entries;
            self.order.retain(|k| entries.contains_key(k));
        }
    }

    /// Evicts a task payload when it reaches a terminal state (Acked or DeadLettered).
    pub fn remove(&mut self, id: u64) -> Option<Arc<Vec<u8>>> {
        self.entries.remove(&id)
    }

    /// Clears all cached payloads.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn stats(&self) -> (u64, u64, u64) {
        (self.hits, self.misses, self.evictions)
    }
}

impl Default for PayloadCache {
    fn default() -> Self {
        Self::new(DEFAULT_PAYLOAD_CACHE_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit_and_miss() {
        let mut cache = PayloadCache::new(10);
        let payload = Arc::new(b"hello world".to_vec());

        cache.insert(1, Arc::clone(&payload));
        assert_eq!(cache.get(1), Some(payload));
        assert_eq!(cache.get(2), None);

        let (hits, misses, evictions) = cache.stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
        assert_eq!(evictions, 0);
    }

    #[test]
    fn test_cache_bounded_eviction() {
        let mut cache = PayloadCache::new(3);

        cache.insert(1, Arc::new(vec![1]));
        cache.insert(2, Arc::new(vec![2]));
        cache.insert(3, Arc::new(vec![3]));
        assert_eq!(cache.len(), 3);

        // Inserting 4th item should evict oldest (1)
        cache.insert(4, Arc::new(vec![4]));
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(1), None);
        assert_eq!(cache.get(2).unwrap().as_slice(), &[2]);
        assert_eq!(cache.get(3).unwrap().as_slice(), &[3]);
        assert_eq!(cache.get(4).unwrap().as_slice(), &[4]);

        let (_, _, evictions) = cache.stats();
        assert_eq!(evictions, 1);
    }

    #[test]
    fn test_cache_explicit_removal() {
        let mut cache = PayloadCache::new(10);
        cache.insert(1, Arc::new(b"task 1".to_vec()));
        assert_eq!(cache.len(), 1);

        let removed = cache.remove(1);
        assert!(removed.is_some());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(1), None);
    }
}

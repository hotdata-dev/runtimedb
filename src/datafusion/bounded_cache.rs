use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

/// Default capacity for bounded caches.
pub const DEFAULT_CACHE_CAPACITY: usize = 100;

/// A simple bounded cache with FIFO eviction.
/// When capacity is reached, the oldest entries are evicted.
pub struct BoundedCache<V> {
    /// Map from key to value
    entries: HashMap<String, V>,
    /// Insertion order for FIFO eviction
    order: VecDeque<String>,
    /// Maximum number of entries
    capacity: usize,
}

impl<V> BoundedCache<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.entries.get(key)
    }

    pub fn insert(&mut self, key: String, value: V) {
        // If key already exists, just update the value (don't change order)
        if let Entry::Occupied(mut e) = self.entries.entry(key.clone()) {
            e.insert(value);
            return;
        }

        // Evict oldest if at capacity
        while self.entries.len() >= self.capacity {
            if let Some(oldest_key) = self.order.pop_front() {
                self.entries.remove(&oldest_key);
            }
        }

        // Insert new entry
        self.entries.insert(key.clone(), value);
        self.order.push_back(key);
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: &str) {
        if self.entries.remove(key).is_some() {
            // Remove from order queue (O(n) but called infrequently)
            self.order.retain(|k| k != key);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounded_cache_basic_operations() {
        let mut cache = BoundedCache::new(3);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_bounded_cache_eviction_at_capacity() {
        let mut cache = BoundedCache::new(2);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        // "a" should be evicted (FIFO)
        assert_eq!(cache.get("a"), None);
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_bounded_cache_update_existing_key_no_eviction() {
        let mut cache = BoundedCache::new(2);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("a".to_string(), 10); // Update existing key

        assert_eq!(cache.get("a"), Some(&10));
        assert_eq!(cache.get("b"), Some(&2));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_bounded_cache_remove() {
        let mut cache = BoundedCache::new(3);

        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        cache.insert("c".to_string(), 3);

        cache.remove("b");

        assert_eq!(cache.get("a"), Some(&1));
        assert_eq!(cache.get("b"), None);
        assert_eq!(cache.get("c"), Some(&3));
        assert_eq!(cache.len(), 2);
    }
}

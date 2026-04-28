use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use thiserror::Error;

use super::page::BpTreeNode;

pub type LruResult<T> = Result<T, LruError>;

#[derive(Error, Debug)]
pub enum LruError {
    #[error("every page in the cache is dirty and cannot be evicted")]
    AllPagesDirty,
}

/// An `LruCache` that uses `HashMap` to store the cache entries and `VecDeque` to
/// manage ordering.
#[derive(Debug, Clone)]
pub struct LruCache<K> {
    capacity: usize,
    map: HashMap<K, CacheEntry<K>>,
    order: VecDeque<K>,
}

/// The cache entry in the `LruCache`; `value` is the actual data we are storing &
/// `key` is just redundant here, might be removed if I don't find it useful.
#[derive(Debug, Clone)]
pub struct CacheEntry<K> {
    key: K,
    value: BpTreeNode,
}

impl<K> LruCache<K>
where
    K: Clone + Eq + Hash,
{
    /// Returns a new instance of `LruCache` initialized with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns the current size of the cache.
    pub fn size(&self) -> usize {
        self.map.len()
    }

    /// Returns `Some(&CacheEntry)` associated with the provided key otherwise
    /// returns `None`.
    pub fn entry(&mut self, key: &K) -> Option<&CacheEntry<K>> {
        if self.map.contains_key(key) {
            // Move the key to the back of the order queue.
            self.order.retain(|k| k != key);
            self.order.push_back(key.clone());
            self.map.get(key)
        } else {
            None
        }
    }

    /// Returns `Some(&BpTreeNode)` associated with the provided key otherwise
    /// returns `None`.
    pub fn node(&mut self, key: &K) -> Option<&BpTreeNode> {
        if self.map.contains_key(key) {
            self.order.retain(|k| k != key);
            self.order.push_back(key.clone());
            let entry = self.map.get(key)?;
            Some(&entry.value)
        } else {
            None
        }
    }

    /// Sets the key/value pair into the cache if not full. If maximum size has
    /// been reached it attempts to evict a non-dirty page and add the new page.
    /// If no non-dirty pages were found, an `Err(LruError::AllPagesDirty)` is
    /// returned.
    ///
    /// If the key existed already, the old value is replaced with the new value
    /// and the key becomes the most recently used.
    pub fn set_entry(&mut self, key: K, value: BpTreeNode) -> LruResult<()> {
        if self.map.contains_key(&key) {
            self.map
                .insert(key.clone(), CacheEntry { key: key.clone(), value });
            self.order.retain(|k| k != &key);
            self.order.push_back(key);
            Ok(())
        } else {
            if self.map.len() == self.capacity {
                let evict_key = self
                    .order
                    .iter()
                    .find(|&k| {
                        self.map
                            .get(k)
                            .map(|entry| !entry.value.is_dirty)
                            .unwrap_or(false)
                    })
                    .cloned();
                match evict_key {
                    None => return Err(LruError::AllPagesDirty),
                    Some(key) => {
                        self.map.remove(&key);
                        self.order.retain(|k| k != &key);
                    }
                }
            }
            self.map
                .insert(key.clone(), CacheEntry { key: key.clone(), value });
            self.order.push_back(key);
            Ok(())
        }
    }
}

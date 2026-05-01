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

#[cfg(test)]
mod tests {
    use crate::storage::page::LeafNode;

    use super::*;

    #[test]
    fn test_lru_get() {
        let mut lru = LruCache::new(3);

        lru.set_entry("A", BpTreeNode::create_leaf(1, LeafNode::new()))
            .unwrap();
        lru.set_entry("B", BpTreeNode::create_leaf(2, LeafNode::new()))
            .unwrap();
        lru.set_entry("C", BpTreeNode::create_leaf(3, LeafNode::new()))
            .unwrap();

        let key = "A";
        let expect = BpTreeNode::create_leaf(1, LeafNode::new());
        let entry = lru.node(&key).unwrap();
        assert_eq!(expect.file_offset, entry.file_offset);

        let key = "C";
        let expect = BpTreeNode::create_leaf(3, LeafNode::new());
        let entry = lru.node(&key).unwrap();
        assert_eq!(expect.file_offset, entry.file_offset);

        let key = "D";
        assert!(lru.node(&key).is_none(), "entry wasn't none");
    }

    #[cfg(test)]
    mod lru_tests {
        use super::*;

        fn make_clean_node(file_offset: u64) -> BpTreeNode {
            BpTreeNode::create_leaf(file_offset, LeafNode::new())
        }

        fn make_dirty_node(file_offset: u64) -> BpTreeNode {
            let mut node =
                BpTreeNode::create_leaf(file_offset, LeafNode::new());
            node.mark_dirty(1);
            node
        }

        // ── ordering tests ───────────────────────────────────────────────────────

        /// Mirrors Go's TestLRUState table-driven tests.
        /// In Rust order.back() == Go's list.Front() (most recent).
        ///         order.front() == Go's list.Back()  (oldest/eviction candidate).
        #[test]
        fn test_lru_state_evicts_oldest_on_full_insert() {
            let mut lru = LruCache::new(5);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.set_entry("D", make_clean_node(4)).unwrap();
            lru.set_entry("E", make_clean_node(5)).unwrap();
            lru.set_entry("F", make_clean_node(6)).unwrap(); // evicts A

            assert_eq!(lru.size(), 5);
            assert_eq!(lru.order.back().unwrap(), &"F"); // most recent
            assert_eq!(lru.order.front().unwrap(), &"B"); // oldest (A was evicted)
        }

        #[test]
        fn test_lru_state_update_existing_moves_to_most_recent() {
            let mut lru = LruCache::new(5);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.set_entry("A", make_clean_node(1)).unwrap(); // re-insert A

            assert_eq!(lru.size(), 3);
            assert_eq!(lru.order.back().unwrap(), &"A"); // moved to most recent
            assert_eq!(lru.order.front().unwrap(), &"B"); // B is now oldest
        }

        #[test]
        fn test_lru_state_get_existing_moves_to_most_recent() {
            let mut lru = LruCache::new(5);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.entry(&"A"); // access A — should move to most recent

            assert_eq!(lru.size(), 3);
            assert_eq!(lru.order.back().unwrap(), &"A");
            assert_eq!(lru.order.front().unwrap(), &"B");
        }

        #[test]
        fn test_lru_state_get_nonexistent_does_not_affect_order() {
            let mut lru = LruCache::new(5);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.entry(&"X"); // does not exist — order unchanged

            assert_eq!(lru.size(), 3);
            assert_eq!(lru.order.back().unwrap(), &"C"); // unchanged
            assert_eq!(lru.order.front().unwrap(), &"A"); // unchanged
        }

        // ── eviction tests ───────────────────────────────────────────────────────

        // Helper: assert the full order of the cache from most-recent to oldest.
        // Mirrors Go's iteration from list.Front() → list.Back().
        fn assert_order(lru: &LruCache<&str>, expected: &[&str]) {
            let actual: Vec<&&str> = lru.order.iter().rev().collect();
            assert_eq!(
                actual.len(),
                expected.len(),
                "cache length mismatch: expected {} got {}",
                expected.len(),
                actual.len()
            );
            for (i, (actual_key, expected_key)) in
                actual.iter().zip(expected).enumerate()
            {
                assert_eq!(
                    **actual_key, *expected_key,
                    "order mismatch at position {i}: expected {expected_key} got {actual_key}"
                );
            }
        }

        #[test]
        fn test_eviction_evicts_oldest_clean_page() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.set_entry("D", make_clean_node(4)).unwrap(); // evicts A

            assert_eq!(lru.size(), 3);
            assert_order(&lru, &["D", "C", "B"]);
        }

        #[test]
        fn test_eviction_skips_dirty_evicts_second_oldest() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_dirty_node(1)).unwrap(); // dirty — cannot evict
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.set_entry("D", make_clean_node(4)).unwrap(); // evicts B (A is dirty)

            assert_eq!(lru.size(), 3);
            assert_order(&lru, &["D", "C", "A"]);
        }

        #[test]
        fn test_eviction_skips_two_dirty_evicts_third_oldest() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_dirty_node(1)).unwrap(); // dirty
            lru.set_entry("B", make_dirty_node(2)).unwrap(); // dirty
            lru.set_entry("C", make_clean_node(3)).unwrap();
            lru.set_entry("D", make_clean_node(4)).unwrap(); // evicts C

            assert_eq!(lru.size(), 3);
            assert_order(&lru, &["D", "B", "A"]);
        }

        #[test]
        fn test_eviction_fails_when_all_pages_dirty() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_dirty_node(1)).unwrap();
            lru.set_entry("B", make_dirty_node(2)).unwrap();
            lru.set_entry("C", make_dirty_node(3)).unwrap();

            let result = lru.set_entry("D", make_clean_node(4));

            assert!(
                matches!(result, Err(LruError::AllPagesDirty)),
                "expected AllPagesDirty error, got {:?}",
                result
            );
            // cache unchanged
            assert_eq!(lru.size(), 3);
            assert_order(&lru, &["C", "B", "A"]);
        }

        // ── get tests ────────────────────────────────────────────────────────────

        #[test]
        fn test_get_returns_value_for_existing_key() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_clean_node(1)).unwrap();
            lru.set_entry("B", make_clean_node(2)).unwrap();
            lru.set_entry("C", make_clean_node(3)).unwrap();

            let node = lru.node(&"A");
            assert!(node.is_some());
            assert_eq!(node.unwrap().file_offset, 1);
        }

        #[test]
        fn test_get_returns_none_for_missing_key() {
            let mut lru = LruCache::new(3);
            lru.set_entry("A", make_clean_node(1)).unwrap();

            assert!(lru.node(&"Z").is_none());
            // order must be unaffected
            assert_order(&lru, &["A"]);
        }
    }
}

use std::error::Error;

use self::storage::cache::LruCache;
use self::storage::page::{BpTreeNode, LeafNodeData, NodeType};

mod storage;

fn main() -> Result<(), Box<dyn Error>> {
    let node = BpTreeNode {
        file_offset: 0,
        free_size: 0,
        is_dirty: false,
        last_lsn: 0,
        node_type: NodeType::Leaf(LeafNodeData::new()),
    };

    let mut cache = LruCache::new(3);
    cache.set_entry("a", node.clone())?;
    cache.set_entry("b", node.clone())?;
    cache.set_entry("c", node.clone())?;

    println!("{:?}", cache.entry(&"a"));
    cache.set_entry("d", node.clone())?;
    println!("{:?}", cache.entry(&"b"));
    println!("{:?}", cache.entry(&"c"));
    println!("{:?}", cache.entry(&"d"));
    Ok(())
}

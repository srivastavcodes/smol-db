#![allow(unused)]

use thiserror::Error;

// ============ core page constraints ================
pub const PAGE_SIZE: usize = 4096;

// ============== common components ==================
pub const OFFSET_ELEM_SIZE: usize = 2;
pub const MAX_VALUE_SIZE: usize = 400;

// =================header sizes =====================

/// cell_type + file_offset + last_lsn + right_offset + cell_count + free_size
///
/// size of fixed space used to store internal node metadata.
pub const INTERNAL_HEADER_SIZE: usize = 1 + 8 + 8 + 8 + 4 + 2;

/// cell_type + file_offset + last_lsn + has_lsib + has_rsib + lsib_file_offset +
/// rsib_file_offset + cell_count + free_size
///
/// size of fixed space used to store leaf metadata.
pub const LEAF_HEADER_SIZE: usize = 1 + 8 + 8 + 2 + 2 + 8 + 8 + 2 + 2;

// ================= cell sizes ======================

/// size of key/internal cell.
///
/// key + file_offset
pub const INTERNAL_CELL: usize = 4 + 8;

/// size of key-val/leaf cell.
///
/// key + deleted? + value_size + value
pub const LEAF_CELL: usize = 4 + 1 + 4 + MAX_VALUE_SIZE;

// ============== cell counts per page ===============

/// maximum number of leaf cells per page.
pub const fn max_leaf_cells() -> usize {
    let cells = (PAGE_SIZE - LEAF_HEADER_SIZE) / (OFFSET_ELEM_SIZE + LEAF_CELL);
    assert!(cells > 0, "max_leaf_cells must be positive");
    cells
}

/// maximum number of internal cells per page.
pub const fn max_internal_cells() -> usize {
    let cells = (PAGE_SIZE - INTERNAL_HEADER_SIZE) / (OFFSET_ELEM_SIZE + INTERNAL_CELL);
    assert!(cells > 0, "max_internal_cells must be positive");
    cells
}

/// A node in a BpTree can be either internal (which contains the key and the
/// location of the key on the leaf node); or leaf node (which contains the
/// val associated with the key, this is where the actual data is stored).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Node {
    Internal,
    Leaf,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("row size {actual} exceeds maximum of {max} bytes")]
    RowTooLarge { actual: usize, max: usize },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// `InternalCell` is an entry in the internal node (non-leaf) of the BpTree.
/// It stores a key and a pointer to the child page that contains all the keys
/// less than (or equal to) this key.
#[derive(Debug, Copy, Clone)]
struct InternalCell {
    key: u32,

    /// offset of the child page less than the key.
    offset: u64,
}

/// LeafCell holds the data entry in a leaf node, this is the actual row value.
#[derive(Debug, Clone)]
struct LeafCell {
    // todo: a parent pointer should be here to know which page owns this cell,
    //  so whichever code-block handles that, we'll instead return the index
    //  of the page that can be used to access the page.
    key: u32,
    value: Vec<u8>,

    /// Deleted is a tombstone marker for scans or point queries to make sure
    /// this cell is skipped. The space is reclaimed during compaction.
    deleted: bool,
}

/// This represents one page of the BpTree. A single page is of 4096 bytes.
/// A single Node can be either an `Node::Internal` or `Node::Leaf`.
#[derive(Debug, Clone)]
struct BpTreeNode {
    /// offset of this node in the database file.
    offset: u64,

    /// slot array in the sorted order.
    slots: Vec<u16>,

    /// bytes of free space between header and data.
    free_size: u16,

    /// whether the page has been modified since last in memory.
    is_dirty: bool,

    /// the last wal entry that modified this page.
    last_lsn: u64,

    /// is a leaf page or not?
    is_leaf: bool,

    // internal nodes fields
    internal_cells: Vec<InternalCell>,
    /// offset of the rightmost child (not stored with a key).
    right_offset: u64,

    // leaf nodes fields and whether it has left/right siblings and if yes, where?
    leaf_cells: Vec<LeafCell>,
    has_lsib: bool,
    has_rsib: bool,
    lsib_offset: u64,
    rsib_offset: u64,
}

impl BpTreeNode {
    /// Directly indexes into the leaf_cells or internal_cells, so the provided
    /// index must be an actual index for the cells array and not a logical one.
    ///
    /// E.g.: let _ = `cell_key(self.slots[i]);` returns the key.
    fn cell_key(&self, offset: usize) -> u32 {
        if self.is_leaf {
            return self.leaf_cells[offset].key;
        }
        self.internal_cells[offset].key
    }

    /// Updates the last lsn of the node and marks the page dirty.
    fn mark_dirty(&mut self, lsn: u64) {
        self.last_lsn = lsn;
        self.is_dirty = true;
    }

    /// Returns the right most key.
    fn right_most_key(&self) -> u32 {
        let last_idx = self.slots[self.slots.len() - 1] as usize;
        self.internal_cells[last_idx].key
    }

    /// Appends into the slot the logical index of the cell being inserted, and append
    /// a new leaf cell into [`BpTreeNode::leaf_cells`].
    fn append_leaf_cell(&mut self, key: u32, value: Vec<u8>) {
        self.slots.push(self.slots.len() as u16);
        self.leaf_cells.push(LeafCell { key, value, deleted: false });
    }

    /// Appends into the slot the logical index of the cell being inserted, and append
    /// a new internal cell into [`BpTreeNode::internal_cells`].
    fn append_internal_cell(&mut self, key: u32, offset: u64) {
        self.slots.push(self.slots.len() as u16);
        self.internal_cells.push(InternalCell { key, offset });
    }
}

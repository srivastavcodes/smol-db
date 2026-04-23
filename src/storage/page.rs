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
    let cells =
        (PAGE_SIZE - INTERNAL_HEADER_SIZE) / (OFFSET_ELEM_SIZE + INTERNAL_CELL);
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
struct InternalCell {
    key: u32,

    /// offset of the child page less than the key.
    offset: u64,
}

/// LeafCell holds the data entry in a leaf node, this is the actual row value.
struct LeafCell {
    key: u32,
    value: Vec<u8>,

    /// Deleted is a tombstone marker for scans or point queries to make sure
    /// this cell is skipped. The space is reclaimed during compaction.
    deleted: bool,

    /// The size of the data this cell is holding, might be removed.
    value_size: u32,
}

/// This represents one page of the BpTree. A single page is of 4096 bytes.
/// A single Node can be either an `Node::Internal` or `Node::Leaf`.
struct BpTreeNode {
    offset: u64,
    slots: Vec<u16>,
}

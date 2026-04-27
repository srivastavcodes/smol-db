use thiserror::Error;

type PageResult<T> = Result<T, PageError>;

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
pub enum PageError {
    #[error("row size {actual} exceeds maximum of {max} bytes")]
    RowTooLarge { actual: usize, max: usize },

    #[error("key `{0}` not found")]
    KeyNotFound(u32),

    #[error("operation not valid for this node type")]
    WrongNodeType,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// `InternalCell` is an entry in the internal node (non-leaf) of the BpTree.
/// It stores a key and a pointer to the child page that contains all the keys
/// less than (or equal to) this key.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalCell {
    pub key: u32,

    /// offset of the child page less than the key.
    pub offset: u64,
}

/// LeafCell holds the data entry in a leaf node, this is the actual row value.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafCell {
    // todo: a parent pointer should be here to know which page owns this cell,
    //  so whichever code-block handles that, we'll instead return the index
    //  of the page that can be used to access the page.
    pub key: u32,
    pub value: Vec<u8>,

    /// Deleted is a tombstone marker for scans or point queries to make sure
    /// this cell is skipped. The space is reclaimed during compaction.
    pub deleted: bool,
}

/// Holds the leaf cells and their offsets with left/right sibling data.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafNodeData {
    /// `cells` holds the actual [`LeafCell`] data with the key and file-offset.
    pub cells: Vec<LeafCell>,

    /// `slots` is the logical sort index: `slots[i]` is the index into
    /// `cells` of `LeafCell` type.
    pub slots: Vec<usize>,
    pub has_lsib: bool,
    pub has_rsib: bool,
    pub lsib_offset: u64,
    pub rsib_offset: u64,
}

impl LeafNodeData {
    pub fn new() -> Self {
        Self {
            cells: Vec::new(),
            slots: Vec::new(),
            has_lsib: false,
            has_rsib: false,
            lsib_offset: 0,
            rsib_offset: 0,
        }
    }

    /// Returns the key of the [`InternalCell`] according to the index provided.
    /// It directly indexes into the cells so the provided index must be the
    /// actual index and not a logical one.
    pub fn cell_key(&self, physical_idx: usize) -> u32 {
        self.cells[physical_idx].key
    }

    /// Append a cell at the end of the physical array, appending its index to the
    /// end of the `slots` array. Used when cells are being added in sorted order.
    /// E.g.: during a split.
    pub fn append_cell(&mut self, key: u32, value: Vec<u8>) -> PageResult<()> {
        check_value_size(&value)?;
        let physical_idx = self.cells.len();
        self.slots.push(physical_idx);
        self.cells.push(LeafCell { key, value, deleted: false });
        Ok(())
    }

    /// Insert a cell at the provided `logical_index`, shifting the slots array
    /// right. The constructed [`LeafCell`] is appended to `cells` and its
    /// physical index is inserted at `slots[logical_index]`.
    pub fn insert_cell(&mut self, logical_idx: usize, key: u32, value: Vec<u8>) -> PageResult<()> {
        check_value_size(&value)?;
        let physical_idx = self.cells.len();
        self.slots.insert(logical_idx, physical_idx);
        self.cells.push(LeafCell { key, value, deleted: false });
        Ok(())
    }

    /// Updates the given key's value or returns a `PageError` in case value size
    /// is larger than max value.
    /// Returns a `KeyNotFound` error if key does not exist.
    pub fn update_cell(&mut self, key: u32, value: Vec<u8>) -> PageResult<()> {
        check_value_size(&value)?;
        // fixme: replace with binary search on keys in cells.
        for cell in &mut self.cells {
            if cell.key == key {
                cell.value = value;
                return Ok(());
            }
        }
        Err(PageError::KeyNotFound(key))
    }
}

/// This represents one page of the BpTree. A single page is of 4096 bytes.
/// A single Node can be either an `Node::Internal` or `Node::Leaf`.
#[derive(Debug, Clone, PartialEq)]
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
    fn push_leaf_cell(&mut self, key: u32, value: Vec<u8>) {
        self.slots.push(self.slots.len() as u16);
        self.leaf_cells.push(LeafCell { key, value, deleted: false });
    }

    /// Appends into the slot the logical index of the cell being inserted, and append
    /// a new internal cell into [`BpTreeNode::internal_cells`].
    fn push_internal_cell(&mut self, key: u32, offset: u64) {
        self.slots.push(self.slots.len() as u16);
        self.internal_cells.push(InternalCell { key, offset });
    }

    /// Inserts a new key-offset pair into the internal node at the given slot index.
    ///
    /// In a B+ tree internal node, keys act as separators between child page pointers.
    /// Each cell stores a key and the file offset of its LEFT child page.
    /// The rightmost child pointer lives separately in `right_offset`.
    ///
    /// The logical layout looks like this:
    ///
    ///   [left_child] | key | [left_child] | key | ... | [right_offset]
    ///
    /// When inserting a new key, we cannot simply place it with its accompanying
    /// offset because that offset is the new page's left child, which must
    /// displace the existing left child of the key currently at `index`.
    /// The existing left child then becomes the left child of the new key.
    ///
    /// Example: insert key=15, offset=pageD at index=1
    ///
    /// Before:
    ///   slots          = [0, 1]
    ///   internal_cells = [(key=10, left=pageA), (key=20, left=pageB)]
    ///   right_offset   = pageC
    ///   logical:  pageA | 10 | pageB | 20 | pageC
    ///
    /// After slot insert and cell push (offsets not yet correct):
    ///   slots          = [0, 2, 1]
    ///   internal_cells = [(key=10, left=pageA), (key=20, left=pageB), (key=15, left=pageD)]
    ///   logical (wrong): pageA | 10 | pageD | 15 | pageB | 20 | pageC
    ///
    /// After swapping offsets at slots[index] and slots[index+1]:
    ///   internal_cells = [(key=10, left=pageA), (key=20, left=pageD), (key=15, left=pageB)]
    ///   logical (correct): pageA | 10 | pageB | 15 | pageD | 20 | pageC
    ///
    /// # Panics
    ///
    /// Panics if `index` equals `slots.len()` — use `append_internal_cell` instead.
    fn insert_internal_cell(&mut self, index: usize, key: u32, offset: u64) {
        let new_cell_idx: u16 = self
            .internal_cells
            .len()
            .try_into()
            .expect("cell count should be less than u16::MAX; page full");

        self.slots.insert(index, new_cell_idx);
        self.internal_cells.push(InternalCell { key, offset });
        // Restore correct child pointer relationships by swapping the offsets between
        // the newly inserted cell and the cell now at index+1.
        let idx1 = self.slots[index] as usize;
        let idx2 = self.slots[index + 1] as usize;

        let offset1 = self.internal_cells[idx1].offset;
        let offset2 = self.internal_cells[idx2].offset;

        self.internal_cells[idx1].offset = offset2;
        self.internal_cells[idx2].offset = offset1;
    }
}

/// Checks if the provided value is smaller than the maximum value size and returns an error
/// if it is larger.
fn check_value_size(value: &[u8]) -> PageResult<()> {
    if value.len() > MAX_VALUE_SIZE {
        Err(PageError::RowTooLarge { actual: value.len(), max: MAX_VALUE_SIZE })
    } else {
        Ok(())
    }
}

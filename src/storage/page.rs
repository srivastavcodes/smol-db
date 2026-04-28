use thiserror::Error;

pub type PageResult<T> = Result<T, PageError>;

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

    /// Returns the key of the `LeafCell` according to the index provided.
    /// It directly indexes into the cells so the provided index must be
    /// the actual index and not a logical one.
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

/// `InternalCell` is an entry in the internal node (non-leaf) of the BpTree.
/// It stores a key and a pointer to the child page that contains all the keys
/// less than (or equal to) this key.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalCell {
    pub key: u32,

    /// offset of the child page less than the key.
    pub child_offset: u64,
}

/// InternalNodeData holds the internal `cells` and `slots` within which, the physical
/// index of the internal nodes are stored.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalNodeData {
    pub cells: Vec<InternalCell>,
    pub slots: Vec<usize>,

    /// The right-most child: the page whose keys are all > the largest key stored in
    /// this mode.
    pub right_child_offset: u64,
}

impl InternalNodeData {
    pub fn new() -> Self {
        Self { cells: Vec::new(), slots: Vec::new(), right_child_offset: 0 }
    }

    /// Returns the key of the `InternalCell` according to the index provided.
    /// It directly indexes into the cells so the provided index must be the
    /// actual index and not a logical one.
    pub fn cell_key(&self, physical_index: usize) -> u32 {
        self.cells[physical_index].key
    }

    pub fn append_cell(&mut self, key: u32, child_offset: u64) {
        let physical_idx = self.cells.len();
        self.slots.push(physical_idx);
        self.cells.push(InternalCell { key, child_offset });
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
        let new_cell_idx = self.cells.len();

        self.slots.insert(index, new_cell_idx);
        self.cells.push(InternalCell { key, child_offset: offset });
        // Restore correct child pointer relationships by swapping the offsets between
        // the newly inserted cell and the cell now at index+1.
        let idx1 = self.slots[index];
        let idx2 = self.slots[index + 1];

        let offset1 = self.cells[idx1].child_offset;
        let offset2 = self.cells[idx2].child_offset;

        self.cells[idx1].child_offset = offset2;
        self.cells[idx2].child_offset = offset1;
    }

    /// Returns the right most key in the current internal node.
    pub fn rightmost_key(&self) -> Option<u32> {
        let physical_idx = self.slots.last()?.to_owned();
        Some(self.cells[physical_idx].key)
    }
}

/// A node in a BpTree can be either internal (which contains the key and the
/// location of the key on the leaf node); or leaf node (which contains the
/// val associated with the key, this is where the actual data is stored).
#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Internal(InternalNodeData),
    Leaf(LeafNodeData),
}

/// This represents one page of the BpTree. A single page is of 4096 bytes.
/// A single Node can be either an `Node::Internal` or `Node::Leaf`.
#[derive(Debug, Clone, PartialEq)]
pub struct BpTreeNode {
    /// Offset of this page in the database file.
    pub file_offset: u64,

    /// bytes of free space between header and data.
    pub free_size: u16,

    /// whether the page has been modified since last in memory.
    pub is_dirty: bool,

    /// the last wal entry that modified this page.
    pub last_lsn: u64,

    /// whether this node is NodeType::Inner
    pub node_type: NodeType,
}

impl BpTreeNode {
    /// Creates a new leaf node at the given file offset.
    pub fn create_leaf(file_offset: u64, data: LeafNodeData) -> Self {
        Self {
            file_offset,
            free_size: 0,
            is_dirty: false,
            last_lsn: 0,
            node_type: NodeType::Leaf(data),
        }
    }

    /// Creates a new internal node at the given file offset.
    pub fn create_internal(file_offset: u64, data: InternalNodeData) -> Self {
        Self {
            file_offset,
            free_size: 0,
            is_dirty: false,
            last_lsn: 0,
            node_type: NodeType::Internal(data),
        }
    }

    /// Returns true if the node has reached its maximum cell capacity.
    ///
    /// Leaf and internal nodes have different capacities derived from
    /// [`PAGE_SIZE`] — a full node must be split before the next insertion.
    pub fn is_full(&self) -> bool {
        match &self.node_type {
            NodeType::Internal(data) => data.slots.len() >= max_internal_cells(),
            NodeType::Leaf(data) => data.slots.len() >= max_leaf_cells(),
        }
    }

    /// Returns true if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self.node_type, NodeType::Leaf(..))
    }

    /// Marks the page as modified and records the LSN of the modifying operation.
    ///
    /// The LSN is used during WAL replay to skip operations that have already
    /// been applied to disk — if the page's `last_lsn` >= the WAL entry's LSN,
    /// the operation is a no-op.
    pub fn mark_dirty(&mut self, lsn: u64) {
        self.last_lsn = lsn;
        self.is_dirty = true;
    }

    /// Marks the page as clean after it has been flushed to disk.
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Returns the key of the cell at the given physical index.
    ///
    /// Physical indexes are stored in `slots` — you almost always want to go
    /// through [`find_cell_offset_by_key`] or iterate over `slots()` rather
    /// than calling this directly.
    pub fn cell_key_at(&self, physical_idx: usize) -> u32 {
        match &self.node_type {
            NodeType::Leaf(data) => data.cell_key(physical_idx),
            NodeType::Internal(data) => data.cell_key(physical_idx),
        }
    }

    /// Returns the slot array as a sorted slice of physical cell indexes.
    ///
    /// `slots[i]` is the physical index into the cell array of the i-th
    /// smallest key. Iterating this slice in order gives keys in ascending
    /// order without physically sorting the cell array.
    pub fn slots(&self) -> &[usize] {
        match &self.node_type {
            NodeType::Internal(data) => &data.slots,
            NodeType::Leaf(data) => &data.slots,
        }
    }

    /// Searches the slot array for `key` using binary search.
    ///
    /// Returns `(logical_index, true)` if found, where `logical_index` is the
    /// position in `slots` that points to the matching cell.
    ///
    /// Returns `(insertion_point, false)` if not found, where `insertion_point`
    /// is the index at which `key` would need to be inserted to keep `slots`
    /// sorted. The B+ tree uses this insertion point directly when inserting
    /// new cells or navigating to the correct child.
    pub fn find_cell_offset_by_key(&self, key: u32) -> (usize, bool) {
        let slots = self.slots();
        match slots.binary_search_by_key(&key, |&physical_idx| self.cell_key_at(physical_idx)) {
            Ok(logical_idx) => (logical_idx, true),
            Err(logical_idx) => (logical_idx, false),
        }
    }

    /// Splits the current node into two halves and appends the second half into the provided
    /// new_node. Returns the separator key (first key of the second half) to be pushed up to
    /// the parent.
    pub fn split_leaf_append_to(&mut self, new_node: &mut LeafNodeData) -> PageResult<u32> {
        let data = self.as_leaf_mut()?;
        let mid = data.slots.len() / 2;

        for i in mid..data.slots.len() {
            let physical_idx = data.slots[i];
            let cell = &data.cells[physical_idx];
            new_node.append_cell(cell.key, cell.value.clone())?;
        }
        data.slots.truncate(mid);
        // Orphaned cells remain in `cells` vector - physical indexes in `slots` are still
        // valid. Compaction can be added later.
        let separator = new_node.cells[new_node.slots[0]].key;
        Ok(separator)
    }

    /// Splits the current node into two halves and appends the second half into the provided
    /// new_node. The middle key is *not* kept in either node - it is returned as the
    /// separator to be inserted into the parent. The middle cell's `child_offset` becomes the
    /// original's `right_child_offset` and the original's `right_child_offset` becomes the
    /// `right_child_offset` of the new_node.
    pub fn split_internal_append_to(&mut self, new_node: &mut InternalNodeData) -> PageResult<u32> {
        let data = self.as_internal_mut()?;
        let mid = data.slots.len() / 2;

        for i in (mid + 1)..data.slots.len() {
            let physical_idx = data.slots[i];
            let cell = &data.cells[physical_idx];
            new_node.append_cell(cell.key, cell.child_offset);
        }
        new_node.right_child_offset = data.right_child_offset;
        // The mid cell's key is the separator key; its right_child offset becomes
        // the og's new right_child offset.
        let physical_mid = data.slots[mid];
        data.right_child_offset = data.cells[physical_mid].child_offset;

        let separator_key = data.cells[physical_mid].key;
        data.slots.truncate(mid);
        Ok(separator_key)
    }
}

impl BpTreeNode {
    /// Borrows the leaf data, returning an error if this is an internal node.
    pub fn as_leaf(&self) -> PageResult<&LeafNodeData> {
        match &self.node_type {
            NodeType::Leaf(data) => Ok(data),
            NodeType::Internal(..) => Err(PageError::WrongNodeType),
        }
    }

    /// Mutably borrows the leaf data, returning an error if this is an internal node.
    pub fn as_leaf_mut(&mut self) -> PageResult<&mut LeafNodeData> {
        match &mut self.node_type {
            NodeType::Leaf(data) => Ok(data),
            NodeType::Internal(..) => Err(PageError::WrongNodeType),
        }
    }

    /// Borrows the internal node data, returning an error if this is a leaf node.
    pub fn as_internal(&self) -> PageResult<&InternalNodeData> {
        match &self.node_type {
            NodeType::Internal(data) => Ok(data),
            NodeType::Leaf(..) => Err(PageError::WrongNodeType),
        }
    }

    /// Mutably borrows the internal node data, returning an error if this is a leaf node.
    pub fn as_internal_mut(&mut self) -> PageResult<&mut InternalNodeData> {
        match &mut self.node_type {
            NodeType::Internal(data) => Ok(data),
            NodeType::Leaf(..) => Err(PageError::WrongNodeType),
        }
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

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;

    #[test]
    fn test_find_cell_offset_by_key_leaf() {
        let node = BpTreeNode {
            file_offset: 0,
            last_lsn: 0,
            free_size: 0,
            is_dirty: false,
            node_type: NodeType::Leaf(LeafNodeData {
                cells: vec![
                    LeafCell { key: 1000, value: b"a".to_vec(), deleted: false },
                    LeafCell { key: 1003, value: b"c".to_vec(), deleted: false },
                    LeafCell { key: 1005, value: b"e".to_vec(), deleted: false },
                    LeafCell { key: 1001, value: b"b".to_vec(), deleted: false },
                ],
                slots: vec![0, 3, 1, 2],
                has_lsib: false,
                has_rsib: false,
                lsib_offset: 0,
                rsib_offset: 0,
            }),
        };
        assert_eq!(node.find_cell_offset_by_key(1000), (0, true));
        assert_eq!(node.find_cell_offset_by_key(1005), (3, true));
        assert_eq!(node.find_cell_offset_by_key(1001), (1, true));
        assert_eq!(node.find_cell_offset_by_key(1003), (2, true));
        assert_eq!(node.find_cell_offset_by_key(1004), (3, false));
    }

    #[test]
    fn test_find_cell_offset_by_key_internal() {
        let node = BpTreeNode {
            file_offset: 0,
            last_lsn: 0,
            free_size: 0,
            is_dirty: false,
            node_type: NodeType::Internal(InternalNodeData {
                cells: vec![
                    InternalCell { key: 3, child_offset: 10 },
                    InternalCell { key: 5, child_offset: 20 },
                    InternalCell { key: 1, child_offset: 30 },
                    InternalCell { key: 9, child_offset: 40 },
                    InternalCell { key: 7, child_offset: 50 },
                ],
                slots: vec![2, 0, 1, 4, 3],
                right_child_offset: 60,
            }),
        };
        assert_eq!(node.find_cell_offset_by_key(3), (1, true));
        assert_eq!(node.find_cell_offset_by_key(4), (2, false));
        assert_eq!(node.find_cell_offset_by_key(10), (5, false));
    }

    #[test]
    fn test_is_full_leaf_at_and_below_capacity() {
        let mut data = LeafNodeData::new();
        for i in 0..max_leaf_cells() - 1 {
            data.append_cell(i as u32, vec![0; 5]).expect("leaf cell should have been appended");
        }
        let mut node = BpTreeNode::create_leaf(0, data);
        assert!(node.is_full().not(), "leaf before `max_leaf_cells()` must not be full");

        let data = node.as_leaf_mut().unwrap();

        data.append_cell(max_leaf_cells() as u32, vec![0; 5])
            .expect("leaf cell should have been appended");
        assert!(node.is_full(), "leaf at `max_leaf_cells()` must be full");
    }

    #[test]
    fn test_is_full_internal_at_and_below_capacity() {
        let mut data = InternalNodeData::new();
        for i in 0..max_internal_cells() - 1 {
            data.append_cell(i as u32, i as u64);
        }
        let mut node = BpTreeNode::create_internal(0, data);
        assert!(node.is_full().not(), "internal before `max_internal_cells()` must not be full");

        let data = node.as_internal_mut().unwrap();

        data.append_cell(max_internal_cells() as u32, max_internal_cells() as u64);
        assert!(node.is_full(), "internal at `max_internal_cells()` must be full");
    }

    #[test]
    fn test_insert_leaf_cell_respects_size_limit() {
        let mut data = LeafNodeData::new();
        assert!(data.insert_cell(0, 0, vec![0; MAX_VALUE_SIZE]).is_ok());
        assert!(data.append_cell(1, vec![0; MAX_VALUE_SIZE]).is_ok());

        assert!(data.insert_cell(2, 0, vec![0; MAX_VALUE_SIZE + 1]).is_err());
        assert!(data.append_cell(3, vec![0; MAX_VALUE_SIZE + 1]).is_err());
    }

    #[test]
    fn test_split_leaf_append_to() {
        let mut data = LeafNodeData::new();
        for i in 0..5 {
            data.append_cell(i, format!("Hello{i}").into_bytes()).unwrap();
        }
        let mut original = BpTreeNode::create_leaf(0, data);
        let mut new_node = LeafNodeData::new();

        let mid_key = original.split_leaf_append_to(&mut new_node).unwrap();
        assert_eq!(2u32, mid_key);
        assert_eq!(3, new_node.slots.len());
        assert_eq!(3, new_node.cells.len());

        let og_leaf = original.as_leaf().unwrap();
        for i in 0..2 {
            let physical_idx = og_leaf.slots[i];
            let actual = &og_leaf.cells[physical_idx];
            assert_eq!(i, actual.key as usize);
            assert_eq!(format!("Hello{i}").into_bytes(), actual.value)
        }

        for i in 0..3 {
            let physical_idx = new_node.slots[i];
            let actual = &new_node.cells[physical_idx];
            assert_eq!(i + 2, actual.key as usize);
            assert_eq!(format!("Hello{}", i + 2).into_bytes(), actual.value);
        }
    }

    #[test]
    fn test_split_internal_append_to() {
        let mut data = InternalNodeData::new();
        for i in 0..5 {
            data.append_cell(i, (100 + i) as u64);
        }
        let mut original = BpTreeNode::create_internal(0, data);
        let mut new_node = InternalNodeData::new();

        let mid_key = original.split_internal_append_to(&mut new_node).unwrap();
        assert_eq!(2u32, mid_key);
        assert_eq!(2, new_node.slots.len());
        assert_eq!(2, new_node.cells.len());

        let og_leaf = original.as_internal().unwrap();
        for i in 0..2 {
            let physical_idx = og_leaf.slots[i];
            let actual = &og_leaf.cells[physical_idx];
            assert_eq!(i, actual.key as usize);
            assert_eq!((100 + i) as u64, actual.child_offset)
        }

        for i in 0..2 {
            let physical_idx = new_node.slots[i];
            let actual = &new_node.cells[physical_idx];
            assert_eq!(i + 3, actual.key as usize);
            assert_eq!((100 + i + 3) as u64, actual.child_offset);
        }
    }
}

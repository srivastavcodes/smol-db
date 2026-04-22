pub mod page_layout {
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

	/// maximum number of leaf cells per page.
	pub const fn max_leaf_cells() -> usize {
		(PAGE_SIZE - LEAF_HEADER_SIZE) / (OFFSET_ELEM_SIZE + LEAF_CELL)
	}

	/// maximum number of internal cells per page.
	pub const fn max_internal_cells() -> usize {
		(PAGE_SIZE - INTERNAL_HEADER_SIZE) / (OFFSET_ELEM_SIZE + INTERNAL_CELL)
	}
}

#[derive(Debug)]
enum Node {
    Internal,
    Leaf,
}


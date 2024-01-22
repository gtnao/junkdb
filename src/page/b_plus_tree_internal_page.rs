use crate::common::{PageID, LSN, PAGE_SIZE};

use super::{PageType, PAGE_ID_OFFSET, PAGE_ID_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE};

pub const B_PLUS_TREE_INTERNAL_PAGE_PAGE_TYPE: PageType = PageType(3);

const LSN_OFFSET: usize = PAGE_ID_OFFSET + PAGE_ID_SIZE;
const LSN_SIZE: usize = 8;
const PARENT_PAGE_ID_OFFSET: usize = LSN_OFFSET + LSN_SIZE;
const PARENT_PAGE_ID_SIZE: usize = 4;
const LOWER_OFFSET_OFFSET: usize = PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE;
const LOWER_OFFSET_SIZE: usize = 4;
const UPPER_OFFSET_OFFSET: usize = LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE;
const UPPER_OFFSET_SIZE: usize = 4;
const HEADER_SIZE: usize = PAGE_TYPE_SIZE
    + PAGE_ID_SIZE
    + LSN_SIZE
    + PARENT_PAGE_ID_SIZE
    + LOWER_OFFSET_SIZE
    + UPPER_OFFSET_SIZE;
// const LINE_POINTER_OFFSET_SIZE: usize = 4;
// const LINE_POINTER_SIZE_SIZE: usize = 4;
// const LINE_POINTER_SIZE: usize = LINE_POINTER_OFFSET_SIZE + LINE_POINTER_SIZE_SIZE;

pub struct BPlusTreeInternalPage {
    pub data: Box<[u8]>,
}

impl BPlusTreeInternalPage {
    pub fn new(page_id: PageID, parent_page_id: PageID) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];
        data[PAGE_TYPE_OFFSET..(PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE)]
            .copy_from_slice(&B_PLUS_TREE_INTERNAL_PAGE_PAGE_TYPE.0.to_le_bytes());
        data[PAGE_ID_OFFSET..(PAGE_ID_OFFSET + PAGE_ID_SIZE)]
            .copy_from_slice(&page_id.0.to_le_bytes());
        data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)]
            .copy_from_slice(&parent_page_id.0.to_le_bytes());
        data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
        data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        BPlusTreeInternalPage { data: data.into() }
    }
    pub fn from_data(data: &[u8]) -> Self {
        BPlusTreeInternalPage { data: data.into() }
    }

    pub fn lsn(&self) -> LSN {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.data[LSN_OFFSET..(LSN_OFFSET + LSN_SIZE)]);
        LSN(u64::from_le_bytes(buf))
    }
    pub fn set_lsn(&mut self, lsn: LSN) {
        self.data[LSN_OFFSET..(LSN_OFFSET + LSN_SIZE)].copy_from_slice(&lsn.0.to_le_bytes());
    }
    pub fn parent_page_id(&self) -> PageID {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)],
        );
        PageID(u32::from_le_bytes(buf))
    }
    pub fn set_parent_page_id(&mut self, parent_page_id: PageID) {
        self.data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)]
            .copy_from_slice(&parent_page_id.0.to_le_bytes());
    }
}

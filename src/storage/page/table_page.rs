use anyhow::{anyhow, Result};

use crate::common::{PageID, INVALID_PAGE_ID, PAGE_SIZE};

use super::{PAGE_ID_OFFSET, PAGE_ID_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE};

pub const TABLE_PAGE_PAGE_TYPE: u32 = 1;

const NEXT_PAGE_ID_OFFSET: usize = PAGE_ID_OFFSET + PAGE_ID_SIZE;
const NEXT_PAGE_ID_SIZE: usize = 8;
const LOWER_OFFSET_OFFSET: usize = NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE;
const LOWER_OFFSET_SIZE: usize = 4;
const UPPER_OFFSET_OFFSET: usize = LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE;
const UPPER_OFFSET_SIZE: usize = 4;
const HEADER_SIZE: usize =
    PAGE_TYPE_SIZE + PAGE_ID_SIZE + NEXT_PAGE_ID_SIZE + LOWER_OFFSET_SIZE + UPPER_OFFSET_SIZE;
const LINE_POINTER_OFFSET_SIZE: usize = 4;
const LINE_POINTER_SIZE_SIZE: usize = 4;
const LINE_POINTER_SIZE: usize = LINE_POINTER_OFFSET_SIZE + LINE_POINTER_SIZE_SIZE;

pub struct TablePage {
    pub data: Box<[u8]>,
}

impl TablePage {
    pub fn new(page_id: PageID) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];
        data[PAGE_TYPE_OFFSET..(PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE)]
            .copy_from_slice(&TABLE_PAGE_PAGE_TYPE.to_le_bytes());
        data[PAGE_ID_OFFSET..(PAGE_ID_OFFSET + PAGE_ID_SIZE)]
            .copy_from_slice(&page_id.0.to_le_bytes());
        data[NEXT_PAGE_ID_OFFSET..(NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE)]
            .copy_from_slice(&INVALID_PAGE_ID.0.to_le_bytes());
        data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
        data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        TablePage {
            data: data.into_boxed_slice(),
        }
    }
    pub fn from_data(data: Box<[u8]>) -> Self {
        TablePage { data }
    }
    pub fn insert(&mut self, data: &[u8]) -> Result<()> {
        // TODO: too large for one page
        if self.free_space() < data.len() + LINE_POINTER_SIZE {
            return Err(anyhow!("free space not enough"));
        }

        let data_size = data.len() as u32;
        let lower_offset = self.lower_offset();
        let upper_offset = self.upper_offset();
        let next_lower_offset: u32 = lower_offset + LINE_POINTER_SIZE as u32;
        let next_upper_offset: u32 = upper_offset - data.len() as u32;
        self.data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&next_lower_offset.to_le_bytes());
        self.data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&next_upper_offset.to_le_bytes());
        self.data[(lower_offset as usize)..(lower_offset as usize + LINE_POINTER_OFFSET_SIZE)]
            .copy_from_slice(&next_upper_offset.to_le_bytes());
        self.data[((lower_offset as usize) + LINE_POINTER_OFFSET_SIZE)
            ..((lower_offset as usize) + LINE_POINTER_SIZE)]
            .copy_from_slice(&data_size.to_le_bytes());
        self.data[(next_upper_offset as usize)..(upper_offset as usize)].copy_from_slice(data);

        Ok(())
    }
    pub fn get_tuples(&self) -> Vec<Box<[u8]>> {
        let count = self.tuple_count();
        (0..count).map(|i| self.get_tuple(i)).collect()
    }
    fn free_space(&self) -> usize {
        let lower_offset = self.lower_offset();
        let upper_offset = self.upper_offset();
        (upper_offset - lower_offset) as usize
    }
    fn lower_offset(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(
            &self.data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)],
        );
        u32::from_le_bytes(bytes)
    }
    fn upper_offset(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(
            &self.data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)],
        );
        u32::from_le_bytes(bytes)
    }
    fn tuple_count(&self) -> usize {
        let lower_offset = self.lower_offset();
        (lower_offset as usize - HEADER_SIZE) / LINE_POINTER_SIZE
    }
    fn get_tuple(&self, index: usize) -> Box<[u8]> {
        let offset = self.line_pointer_offset(index) as usize;
        let size = self.line_pointer_size(index) as usize;
        self.data[offset..(offset + size)].into()
    }
    fn line_pointer_offset(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * LINE_POINTER_SIZE;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[offset..(offset + LINE_POINTER_OFFSET_SIZE)]);
        u32::from_le_bytes(bytes)
    }
    fn line_pointer_size(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[offset..(offset + LINE_POINTER_SIZE_SIZE)]);
        u32::from_le_bytes(bytes)
    }
}

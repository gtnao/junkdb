use std::ops::Range;

use crate::common::PAGE_SIZE;

use super::{PageKind, PageLayout, PageView, PageViewMut};

const SLOT_DIR_END: Range<usize> = 10..12;
const FREE_SPACE_START: Range<usize> = 12..14;
const HEADER_END: usize = 16;
/// 1 slot = (u16 tuple_offset, u16 tuple_len) = 4B
const SLOT_SIZE: usize = 4;

pub struct TablePage;

impl PageLayout for TablePage {
    const KIND: PageKind = 0x01;
}

impl TablePage {
    pub fn init_header(bytes: &mut [u8; PAGE_SIZE]) {
        set_slot_dir_end(bytes, HEADER_END as u16);
        set_free_space_start(bytes, PAGE_SIZE as u16);
    }
}

#[inline]
fn slot_dir_end(b: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(b[SLOT_DIR_END].try_into().unwrap())
}
#[inline]
fn free_space_start(b: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(b[FREE_SPACE_START].try_into().unwrap())
}
#[inline]
fn set_slot_dir_end(b: &mut [u8; PAGE_SIZE], v: u16) {
    b[SLOT_DIR_END].copy_from_slice(&v.to_le_bytes())
}
#[inline]
fn set_free_space_start(b: &mut [u8; PAGE_SIZE], v: u16) {
    b[FREE_SPACE_START].copy_from_slice(&v.to_le_bytes())
}
#[inline]
fn tuple_count(b: &[u8; PAGE_SIZE]) -> usize {
    ((slot_dir_end(b) as usize) - HEADER_END) / SLOT_SIZE
}

impl PageView<'_, TablePage> {
    pub fn tuple(&self, idx: usize) -> &[u8] {
        assert!(idx < tuple_count(self.bytes));
        let entry_pos = HEADER_END + idx * SLOT_SIZE;
        let off =
            u16::from_le_bytes(self.bytes[entry_pos..entry_pos + 2].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.bytes[entry_pos + 2..entry_pos + 4].try_into().unwrap())
            as usize;
        &self.bytes[off..off + len]
    }
}

impl PageViewMut<'_, TablePage> {
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Option<usize /*slot id*/> {
        let lower = slot_dir_end(self.bytes) as usize;
        let upper = free_space_start(self.bytes) as usize;
        let need_space = SLOT_SIZE + tuple.len();
        if upper < lower + need_space {
            return None;
        }

        let new_upper = upper - tuple.len();
        self.bytes[new_upper..upper].copy_from_slice(tuple);

        let new_slot_off = lower;
        self.bytes[new_slot_off..new_slot_off + 2]
            .copy_from_slice(&(new_upper as u16).to_le_bytes());
        self.bytes[new_slot_off + 2..new_slot_off + 4]
            .copy_from_slice(&(tuple.len() as u16).to_le_bytes());

        set_slot_dir_end(self.bytes, (lower + SLOT_SIZE) as u16);
        set_free_space_start(self.bytes, new_upper as u16);

        Some((new_slot_off - HEADER_END) / SLOT_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::PageBuffer;

    use super::*;

    fn new_buf() -> PageBuffer {
        let mut buf = PageBuffer::with_header(0 /*pid*/, TablePage::KIND);
        TablePage::init_header(buf.as_bytes_mut());
        buf
    }

    #[test]
    fn init_header_sets_correct_bounds() {
        let buf = new_buf();
        assert_eq!(slot_dir_end(buf.as_bytes()) as usize, HEADER_END);
        assert_eq!(free_space_start(buf.as_bytes()) as usize, PAGE_SIZE);
        assert_eq!(tuple_count(buf.as_bytes()), 0);
    }

    #[test]
    fn insert_and_fetch_tuple() {
        let mut buf = new_buf();
        {
            let mut page = PageViewMut::<TablePage>::new(buf.as_bytes_mut());
            let id0 = page.insert_tuple(b"hello").expect("insert 0");
            let id1 = page.insert_tuple(b"world!").expect("insert 1");
            assert_eq!(id0, 0);
            assert_eq!(id1, 1);
        }
        let page = PageView::<TablePage>::new(buf.as_bytes());
        assert_eq!(tuple_count(buf.as_bytes()), 2);
        assert_eq!(page.tuple(0), b"hello");
        assert_eq!(page.tuple(1), b"world!");
    }

    #[test]
    fn insert_returns_none_on_overflow() {
        let mut buf = new_buf();
        let free_bytes =
            free_space_start(buf.as_bytes()) as usize - slot_dir_end(buf.as_bytes()) as usize;
        let large = vec![0u8; free_bytes - SLOT_SIZE + 1];
        let mut page = PageViewMut::<TablePage>::new(buf.as_bytes_mut());
        assert!(page.insert_tuple(&large).is_none());
    }
}

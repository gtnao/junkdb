use std::{marker::PhantomData, ops::Range};

use crate::common::{PageID, PAGE_SIZE};

pub mod heap;

type PageKind = u16;

const PAGE_KIND_OFFSET: Range<usize> = 0..2;
const PAGE_ID_OFFSET: Range<usize> = 2..10;

pub struct PageBuffer([u8; PAGE_SIZE]);

impl PageBuffer {
    pub fn zeroed() -> Self {
        Self([0; PAGE_SIZE])
    }
    pub fn with_header(id: PageID, kind: PageKind) -> Self {
        let mut buffer = Self::zeroed();
        buffer.0[PAGE_KIND_OFFSET].copy_from_slice(&kind.to_le_bytes());
        buffer.0[PAGE_ID_OFFSET].copy_from_slice(&id.to_le_bytes());
        buffer
    }
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.0
    }
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.0
    }
}

pub trait PageLayout {
    const KIND: PageKind;
    fn validate(bytes: &[u8; PAGE_SIZE]) -> bool {
        u16::from_le_bytes(bytes[PAGE_KIND_OFFSET].try_into().unwrap()) == Self::KIND
    }
}

pub struct PageView<'a, T: PageLayout> {
    bytes: &'a [u8; PAGE_SIZE],
    _marker: PhantomData<T>,
}

pub struct PageViewMut<'a, T: PageLayout> {
    bytes: &'a mut [u8; PAGE_SIZE],
    _marker: PhantomData<T>,
}

impl<'a, T: PageLayout> PageView<'a, T> {
    pub fn new(bytes: &'a [u8; PAGE_SIZE]) -> Self {
        assert!(T::validate(bytes));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: PageLayout> PageViewMut<'a, T> {
    pub fn new(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        assert!(T::validate(bytes));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PageID;

    struct DummyHeap;
    impl PageLayout for DummyHeap {
        const KIND: PageKind = 42;
    }
    impl PageView<'_, DummyHeap> {
        pub fn read(&self) -> u8 {
            self.bytes[16]
        }
    }
    impl PageViewMut<'_, DummyHeap> {
        pub fn write(&mut self, value: u8) {
            self.bytes[16] = value;
        }
    }

    #[test]
    fn zeroed_page_is_all_zero() {
        let buf = PageBuffer::zeroed();
        assert!(buf.as_bytes().iter().all(|b| *b == 0));
    }

    #[test]
    fn with_header_sets_kind_and_id() {
        let id: PageID = 1234;
        let buf = PageBuffer::with_header(id, DummyHeap::KIND);

        let kind_raw =
            u16::from_le_bytes(buf.as_bytes()[super::PAGE_KIND_OFFSET].try_into().unwrap());
        assert_eq!(kind_raw, DummyHeap::KIND);

        let id_raw = u64::from_le_bytes(buf.as_bytes()[super::PAGE_ID_OFFSET].try_into().unwrap());
        assert_eq!(id_raw, id);
    }

    #[test]
    fn create_view_success() {
        let buf = PageBuffer::with_header(1, DummyHeap::KIND);
        let _view: PageView<'_, DummyHeap> = PageView::new(buf.as_bytes());
    }

    #[test]
    #[should_panic]
    fn create_view_panics_on_wrong_kind() {
        let buf = PageBuffer::with_header(1, 999);
        let _ = PageView::<DummyHeap>::new(buf.as_bytes());
    }

    #[test]
    fn view_mut_can_modify_underlying_bytes() {
        let mut buf = PageBuffer::with_header(1, DummyHeap::KIND);

        {
            let mut view = PageViewMut::<DummyHeap>::new(buf.as_bytes_mut());
            view.write(0xAA);
        }
        {
            let view = PageView::<DummyHeap>::new(buf.as_bytes());
            assert_eq!(view.read(), 0xAA);
        }
    }
}

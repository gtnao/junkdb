use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    ops::Range,
    path::Path,
};

use anyhow::Result;

const PAGE_SIZE: usize = 8 * 1024; // 8kB

type PageId = u64;

type PageKind = u16;

const PAGE_KIND_OFFSET: Range<usize> = 0..2;
const PAGE_ID_OFFSET: Range<usize> = 2..10;

struct PageBuffer {
    bytes: [u8; PAGE_SIZE],
}

impl PageBuffer {
    fn zeroed() -> Self {
        Self {
            bytes: [0; PAGE_SIZE],
        }
    }
    fn with_header(id: PageId, kind: PageKind) -> Self {
        let mut buffer = Self::zeroed();
        buffer.bytes[PAGE_KIND_OFFSET].copy_from_slice(&kind.to_le_bytes());
        buffer.bytes[PAGE_ID_OFFSET].copy_from_slice(&id.to_le_bytes());
        buffer
    }
    fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }
    fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }
}

trait PageLayout {
    const KIND: PageKind;
    fn validate(bytes: &[u8; PAGE_SIZE]) -> bool {
        u16::from_le_bytes(bytes[PAGE_KIND_OFFSET].try_into().unwrap()) == Self::KIND
    }
}

struct PageView<'a, T: PageLayout> {
    bytes: &'a [u8; PAGE_SIZE],
    _marker: PhantomData<T>,
}

struct PageViewMut<'a, T: PageLayout> {
    bytes: &'a mut [u8; PAGE_SIZE],
    _marker: PhantomData<T>,
}

impl<'a, T: PageLayout> PageView<'a, T> {
    fn new(bytes: &'a [u8; PAGE_SIZE]) -> Self {
        assert!(T::validate(bytes));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: PageLayout> PageViewMut<'a, T> {
    fn new(bytes: &'a mut [u8; PAGE_SIZE]) -> Self {
        assert!(T::validate(bytes));
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

const TUPLE_COUNT_OFFSET: Range<usize> = 10..14;
const TUPLE_AREA_OFFSET_START: usize = 14;

struct TablePage;

impl PageLayout for TablePage {
    const KIND: PageKind = 0x01;
}

fn tuple_count(bytes: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_le_bytes(bytes[TUPLE_COUNT_OFFSET].try_into().unwrap())
}

impl PageView<'_, TablePage> {
    fn tuples(&self) -> &[u8] {
        let end = TUPLE_AREA_OFFSET_START + tuple_count(self.bytes) as usize;
        &self.bytes[TUPLE_AREA_OFFSET_START..end]
    }
}

impl PageViewMut<'_, TablePage> {
    fn append_tuple(&mut self, tuple: &[u8]) {
        let current_count = tuple_count(self.bytes);
        let start = TUPLE_AREA_OFFSET_START + current_count as usize;
        let end = start + tuple.len();
        assert!(end <= PAGE_SIZE, "tuple overflow page");
        self.bytes[start..end].copy_from_slice(tuple);
        self.set_tuple_count(current_count + tuple.len() as u32);
    }
    fn set_tuple_count(&mut self, count: u32) {
        self.bytes[TUPLE_COUNT_OFFSET].copy_from_slice(&count.to_le_bytes());
    }
}

struct IndexPage;

impl PageLayout for IndexPage {
    const KIND: PageKind = 0x02;
}

impl PageView<'_, IndexPage> {}

impl PageViewMut<'_, IndexPage> {}

struct PageManager {
    file: File,
}

impl PageManager {
    fn load<T: AsRef<Path>>(file_path: T) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_path.as_ref())?,
        })
    }
    fn bootstrap<T: AsRef<Path>>(file_path: T) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_path.as_ref())?,
        })
    }
    fn read_page(&mut self, page_id: PageId) -> Result<PageBuffer> {
        let mut buffer = PageBuffer::zeroed();
        self.file
            .seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.read_exact(buffer.as_bytes_mut())?;
        Ok(buffer)
    }
    fn write_page(&mut self, page_id: PageId, page: &PageBuffer) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.write_all(page.as_bytes())?;
        self.file.sync_all()?;
        Ok(())
    }
    fn allocate_page(&mut self, page_kind: PageKind) -> Result<(PageId, PageBuffer)> {
        let page_id = self.next_page_id();
        let page = PageBuffer::with_header(page_id, page_kind);
        self.file
            .seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.write_all(page.as_bytes())?;
        self.file.sync_all()?;
        Ok((page_id, page))
    }
    fn next_page_id(&self) -> PageId {
        let metadata = self.file.metadata().unwrap();
        metadata.len() / PAGE_SIZE as u64
    }
}

fn main() -> Result<()> {
    let file_name = "example.db";
    let mut page_manager = PageManager::bootstrap(file_name)?;

    let (page_id, mut buffer) = page_manager.allocate_page(TablePage::KIND)?;
    {
        let mut view = PageViewMut::<TablePage>::new(buffer.as_bytes_mut());
        view.append_tuple(&[1, 2, 3, 4]);
    }
    page_manager.write_page(page_id, &buffer)?;

    let buffer = page_manager.read_page(page_id)?;
    {
        let view = PageView::<TablePage>::new(buffer.as_bytes());
        let tuples = view.tuples();
        println!("tuples: {:?}", tuples);
    };

    let mut buffer = page_manager.read_page(page_id)?;
    {
        let mut view = PageViewMut::<TablePage>::new(buffer.as_bytes_mut());
        view.append_tuple(&[5, 6, 7, 8]);
    }
    page_manager.write_page(page_id, &buffer)?;

    let buffer = page_manager.read_page(page_id)?;
    {
        let view = PageView::<TablePage>::new(buffer.as_bytes());
        let tuples = view.tuples();
        println!("tuples: {:?}", tuples);
    }
    Ok(())
}

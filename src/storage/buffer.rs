use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Result};

use crate::common::{PageID, PAGE_SIZE};

use self::replacer::{LRUReplacer, Replacer};

use super::{
    disk::DiskManager,
    page::{Page, PageType},
};

mod replacer;

pub struct Buffer {
    page: Arc<RwLock<Page>>,
    pin_count: u32,
    is_dirty: bool,
}
impl Buffer {
    pub fn new(page: Arc<RwLock<Page>>) -> Self {
        Self {
            page,
            pin_count: 0,
            is_dirty: false,
        }
    }
    pub fn add_pin_count(&mut self) {
        self.pin_count += 1;
    }
    pub fn sub_pin_count(&mut self) {
        self.pin_count -= 1;
    }
    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}

// TODO: temp value
pub const BUFFER_POOL_SIZE: usize = 10;

pub struct BufferPoolManager {
    disk_manager: DiskManager,
    pages: Vec<Buffer>,
    page_table: HashMap<PageID, usize>,
    replacer: Replacer,
}
impl BufferPoolManager {
    pub fn new(disk_manager: DiskManager) -> Self {
        Self {
            disk_manager,
            pages: Vec::with_capacity(BUFFER_POOL_SIZE),
            page_table: HashMap::new(),
            // TODO: variable
            replacer: Replacer::LRU(LRUReplacer::new()),
        }
    }
    pub fn fetch_page(&mut self, page_id: PageID) -> Result<Arc<RwLock<Page>>> {
        if !self.page_table.contains_key(&page_id) {
            self.evict_page()?;
            let mut data = vec![0u8; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut data)?;
            let page = Arc::new(RwLock::new(Page::from_data(data.into_boxed_slice())));
            let frame_id = self.pages.len();
            self.pages.push(Buffer::new(page.clone()));
            self.page_table.insert(page_id, frame_id);
        }
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let buffer = &mut self.pages[frame_id];
            buffer.add_pin_count();
            self.replacer.pin(frame_id);
            return Ok(buffer.page.clone());
        }
        Err(anyhow!("fetch page error"))
    }
    pub fn unpin_page(&mut self, page_id: PageID, is_dirty: bool) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let buffer = &mut self.pages[frame_id];
            if is_dirty {
                buffer.mark_dirty();
            }
            buffer.sub_pin_count();
            if !buffer.is_pinned() {
                self.replacer.unpin(frame_id);
            }
            return Ok(());
        }
        Err(anyhow!("unpin page error"))
    }
    pub fn new_page(&mut self, page_type: PageType) -> Result<Arc<RwLock<Page>>> {
        self.evict_page()?;
        let page_id = self.disk_manager.allocate_page()?;
        let page = Arc::new(RwLock::new(Page::new(page_id, page_type)));
        let frame_id = self.pages.len();
        self.pages.push(Buffer::new(page.clone()));
        self.page_table.insert(page_id, frame_id);

        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let buffer = &mut self.pages[frame_id];
            buffer.add_pin_count();
            self.replacer.pin(frame_id);
            return Ok(buffer.page.clone());
        }
        Err(anyhow!("fetch page error"))
    }
    pub fn flush_all_pages(&mut self) -> Result<()> {
        let keys = self.page_table.keys().cloned().collect::<Vec<_>>();
        for page_id in keys {
            self.flush_page(page_id)?;
        }
        Ok(())
    }
    fn flush_page(&mut self, page_id: PageID) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let buffer = &mut self.pages[frame_id];
            if buffer.is_dirty {
                let page = buffer.page.read().map_err(|_| anyhow!("lock error"))?;
                self.disk_manager.write_page(page_id, &page.data())?;
            }
        }
        Ok(())
    }
    fn evict_page(&mut self) -> Result<()> {
        if self.pages.len() < BUFFER_POOL_SIZE {
            return Ok(());
        }
        if let Some(frame_id) = self.replacer.victim() {
            let page_id = self.pages[frame_id]
                .page
                .read()
                .map_err(|_| anyhow!("lock error"))?
                .page_id();
            self.flush_page(page_id)?;
            self.pages.remove(frame_id);
            self.page_table.remove(&page_id);
        } else {
            // TODO: wait
            return Err(anyhow!("buffer pool is full"));
        }
        Ok(())
    }
}

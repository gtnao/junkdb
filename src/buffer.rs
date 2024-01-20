use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{anyhow, Result};

use crate::{
    common::{PageID, PAGE_SIZE},
    disk::DiskManager,
    log::LogManager,
    page::{Page, PageType},
};

struct Frame {
    page: Arc<RwLock<Page>>,
    pin_count: u32,
    is_dirty: bool,
}
impl Frame {
    fn new(page: Arc<RwLock<Page>>) -> Self {
        Self {
            page,
            pin_count: 0,
            is_dirty: false,
        }
    }
    fn add_pin_count(&mut self) {
        self.pin_count += 1;
    }
    fn sub_pin_count(&mut self) {
        self.pin_count -= 1;
    }
    fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }
    fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}

pub struct BufferPoolManager {
    disk_manager: DiskManager,
    log_manager: Arc<Mutex<LogManager>>,
    size: usize,
    frames: Vec<Option<Frame>>,
    page_table: HashMap<PageID, usize>,
    replacer: Replacer,
}
impl BufferPoolManager {
    pub fn new(
        disk_manager: DiskManager,
        log_manager: Arc<Mutex<LogManager>>,
        size: usize,
    ) -> Self {
        Self {
            disk_manager,
            log_manager,
            size,
            frames: Vec::with_capacity(size),
            page_table: HashMap::new(),
            replacer: Replacer::LRU(LRUReplacer::default()),
        }
    }
    pub fn fetch_page(&mut self, page_id: PageID) -> Result<Arc<RwLock<Page>>> {
        if !self.page_table.contains_key(&page_id) {
            if self.is_full() {
                self.evict_page()?;
            }
            let mut data = vec![0u8; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut data)?;
            let page = Arc::new(RwLock::new(Page::from_data(&data)));
            let frame_id = self.frames.len();
            self.frames.push(Some(Frame::new(page.clone())));
            self.page_table.insert(page_id, frame_id);
        }
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if let Some(frame) = &mut self.frames[frame_id] {
                frame.add_pin_count();
                self.replacer.pin(frame_id);
                return Ok(frame.page.clone());
            }
        }
        unreachable!("page not found")
    }
    pub fn unpin_page(&mut self, page_id: PageID, is_dirty: bool) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if let Some(frame) = &mut self.frames[frame_id] {
                if is_dirty {
                    frame.mark_dirty();
                }
                frame.sub_pin_count();
                if !frame.is_pinned() {
                    self.replacer.unpin(frame_id);
                }
                return Ok(());
            }
        }
        unreachable!("page not found")
    }
    pub fn new_page(&mut self, page_type: PageType) -> Result<Arc<RwLock<Page>>> {
        if self.is_full() {
            self.evict_page()?;
        }
        let page_id = self.disk_manager.allocate_page()?;
        let page = Arc::new(RwLock::new(Page::new(page_id, page_type)));
        let frame_id = self.frames.len();
        self.frames.push(Some(Frame::new(page.clone())));
        self.page_table.insert(page_id, frame_id);

        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if let Some(frame) = &mut self.frames[frame_id] {
                frame.add_pin_count();
                self.replacer.pin(frame_id);
                return Ok(frame.page.clone());
            }
        }
        unreachable!("page not found")
    }
    pub fn shutdown(&mut self) -> Result<()> {
        self.flush_all_pages()?;
        Ok(())
    }
    fn flush_all_pages(&mut self) -> Result<()> {
        let keys = self.page_table.keys().cloned().collect::<Vec<_>>();
        for page_id in keys {
            self.flush_page(page_id)?;
        }
        Ok(())
    }
    fn flush_page(&mut self, page_id: PageID) -> Result<()> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            if let Some(frame) = &mut self.frames[frame_id] {
                if frame.is_dirty {
                    let page = frame.page.read().map_err(|_| anyhow!("lock error"))?;
                    self.log_manager
                        .lock()
                        .map_err(|_| anyhow!("lock error"))?
                        .flush()?;
                    self.disk_manager.write_page(page_id, &page.data())?;
                }
            }
        }
        Ok(())
    }
    fn is_full(&self) -> bool {
        self.frames.len() == self.size
    }
    fn evict_page(&mut self) -> Result<()> {
        if let Some(frame_id) = self.replacer.victim() {
            if let Some(frame) = &mut self.frames[frame_id] {
                let page_id = frame
                    .page
                    .read()
                    .map_err(|_| anyhow!("lock error"))?
                    .page_id();
                self.flush_page(page_id)?;
                self.frames[frame_id] = None;
                self.page_table.remove(&page_id);
            }
            return Ok(());
        }
        // TODO: wait
        Err(anyhow!("buffer pool is full"))
    }
}

enum Replacer {
    LRU(LRUReplacer),
}
impl Replacer {
    pub fn victim(&mut self) -> Option<usize> {
        match self {
            Self::LRU(replacer) => replacer.victim(),
        }
    }
    pub fn pin(&mut self, frame_id: usize) {
        match self {
            Self::LRU(replacer) => replacer.pin(frame_id),
        }
    }
    pub fn unpin(&mut self, frame_id: usize) {
        match self {
            Self::LRU(replacer) => replacer.unpin(frame_id),
        }
    }
}

struct LRUReplacer {
    frame_map: HashMap<usize, u128>,
    counter: u128,
}
impl Default for LRUReplacer {
    fn default() -> Self {
        Self {
            frame_map: HashMap::new(),
            counter: 0,
        }
    }
}
impl LRUReplacer {
    fn victim(&mut self) -> Option<usize> {
        if self.frame_map.is_empty() {
            return None;
        }
        let mut min_frame_id = 0;
        let mut min_counter = u128::MAX;
        for (&frame_id, &counter) in self.frame_map.iter() {
            if counter < min_counter {
                min_frame_id = frame_id;
                min_counter = counter;
            }
        }
        self.frame_map.remove(&min_frame_id);
        Some(min_frame_id)
    }
    fn pin(&mut self, frame_id: usize) {
        self.frame_map.remove(&frame_id);
    }
    fn unpin(&mut self, frame_id: usize) {
        self.frame_map.insert(frame_id, self.counter);
        self.counter = self.counter.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use crate::page::table_page::TABLE_PAGE_PAGE_TYPE;

    use super::*;

    #[test]
    fn test_lru_replacer() {
        let mut replacer = Replacer::LRU(LRUReplacer::default());

        assert_eq!(replacer.victim(), None);
        replacer.pin(1);
        replacer.pin(2);
        replacer.pin(3);
        replacer.pin(4);
        replacer.pin(2);
        replacer.unpin(2);
        replacer.unpin(1);
        replacer.unpin(3);
        replacer.pin(1);
        assert_eq!(replacer.victim(), Some(2));
        assert_eq!(replacer.victim(), Some(3));
        assert_eq!(replacer.victim(), None);
        replacer.unpin(1);
        assert_eq!(replacer.victim(), Some(1));
        assert_eq!(replacer.victim(), None);
        replacer.unpin(4);
        assert_eq!(replacer.victim(), Some(4));
        assert_eq!(replacer.victim(), None);
    }

    // TODO: improve test
    #[test]
    fn test_buffer_pool_manager() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let data_file_path = dir.path().join("data");
        let log_file_path = dir.path().join("log");
        let disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            log_file_path.to_str().unwrap(),
        )?));
        let mut buffer_pool_manager = BufferPoolManager::new(disk_manager, log_manager, 3);

        buffer_pool_manager.new_page(TABLE_PAGE_PAGE_TYPE)?;
        buffer_pool_manager.new_page(TABLE_PAGE_PAGE_TYPE)?;
        buffer_pool_manager.new_page(TABLE_PAGE_PAGE_TYPE)?;
        buffer_pool_manager.unpin_page(PageID(1), true)?;
        buffer_pool_manager.new_page(TABLE_PAGE_PAGE_TYPE)?;
        buffer_pool_manager.unpin_page(PageID(2), true)?;
        let page1 = buffer_pool_manager.fetch_page(PageID(1))?;
        assert_eq!(
            page1.read().map_err(|_| anyhow!("lock error"))?.page_id(),
            PageID(1)
        );
        buffer_pool_manager.unpin_page(PageID(3), true)?;
        buffer_pool_manager.unpin_page(PageID(4), true)?;
        buffer_pool_manager.unpin_page(PageID(1), false)?;
        buffer_pool_manager.shutdown()?;

        // restart
        let disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            log_file_path.to_str().unwrap(),
        )?));
        let mut buffer_pool_manager = BufferPoolManager::new(disk_manager, log_manager, 3);
        let page1 = buffer_pool_manager.fetch_page(PageID(1))?;
        let page2 = buffer_pool_manager.fetch_page(PageID(2))?;
        let page3 = buffer_pool_manager.fetch_page(PageID(3))?;
        assert_eq!(
            page1.read().map_err(|_| anyhow!("lock error"))?.page_id(),
            PageID(1)
        );
        assert_eq!(
            page2.read().map_err(|_| anyhow!("lock error"))?.page_id(),
            PageID(2)
        );
        assert_eq!(
            page3.read().map_err(|_| anyhow!("lock error"))?.page_id(),
            PageID(3)
        );
        buffer_pool_manager.unpin_page(PageID(1), false)?;
        let page4 = buffer_pool_manager.fetch_page(PageID(4))?;
        assert_eq!(
            page4.read().map_err(|_| anyhow!("lock error"))?.page_id(),
            PageID(4)
        );
        buffer_pool_manager.unpin_page(PageID(2), false)?;
        buffer_pool_manager.unpin_page(PageID(3), false)?;
        buffer_pool_manager.unpin_page(PageID(4), false)?;
        buffer_pool_manager.shutdown()?;

        Ok(())
    }
}

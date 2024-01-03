use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::common::{PageID, INVALID_PAGE_ID};

use super::{buffer::BufferPoolManager, page::table_page::TABLE_PAGE_PAGE_TYPE};

pub mod table_iterator;

pub struct TableHeap {
    first_page_id: PageID,
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
}

impl TableHeap {
    pub fn new(first_page_id: PageID, buffer_pool_manager: Arc<Mutex<BufferPoolManager>>) -> Self {
        Self {
            first_page_id,
            buffer_pool_manager,
        }
    }
    pub fn insert(&mut self, data: &[u8]) -> Result<()> {
        let mut page_id = self.first_page_id;
        loop {
            let page = self
                .buffer_pool_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .fetch_page(page_id)?;
            let result = page
                .write()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .with_table_page_mut(|table_page| table_page.insert(data));
            // TODO: only free space not enough
            if result.is_ok() {
                self.buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .unpin_page(page_id, true)?;
                break;
            }

            let next_page_id = page
                .read()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .with_table_page(|table_page| table_page.next_page_id());
            if next_page_id == INVALID_PAGE_ID {
                let next_page = self
                    .buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .new_page(TABLE_PAGE_PAGE_TYPE)?;
                let next_page_id = next_page
                    .read()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .page_id();
                page.write()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .with_table_page_mut(|table_page| table_page.set_next_page_id(next_page_id));
                self.buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .unpin_page(page_id, true)?;
                page_id = next_page_id;
                self.buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .unpin_page(page_id, true)?;
            } else {
                self.buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .unpin_page(page_id, false)?;
                page_id = next_page_id;
            }
        }
        Ok(())
    }
}

use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    common::{PageID, TransactionID, INVALID_PAGE_ID, INVALID_TRANSACTION_ID},
    concurrency::TransactionManager,
    value::Value,
};

use super::{page::table_page::TABLE_PAGE_PAGE_TYPE, tuple::Tuple};

pub mod table_iterator;

pub struct TableHeap {
    first_page_id: PageID,
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    txn_id: TransactionID,
}

impl TableHeap {
    pub fn new(
        first_page_id: PageID,
        buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        txn_id: TransactionID,
    ) -> Self {
        Self {
            first_page_id,
            buffer_pool_manager,
            transaction_manager,
            txn_id,
        }
    }
    pub fn insert(&mut self, values: &[Value]) -> Result<()> {
        let tuple_data = Tuple::serialize(self.txn_id, INVALID_TRANSACTION_ID, &values);
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
                .with_table_page_mut(|table_page| table_page.insert(&tuple_data));
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

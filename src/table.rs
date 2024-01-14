use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    common::{PageID, TransactionID, INVALID_PAGE_ID, INVALID_TRANSACTION_ID, RID},
    concurrency::TransactionManager,
    lock::LockManager,
    page::table_page::TABLE_PAGE_PAGE_TYPE,
    tuple::Tuple,
    value::Value,
};

pub struct TableHeap {
    first_page_id: PageID,
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    lock_manager: Arc<RwLock<LockManager>>,
    txn_id: TransactionID,
}

impl TableHeap {
    pub fn new(
        first_page_id: PageID,
        buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        lock_manager: Arc<RwLock<LockManager>>,
        txn_id: TransactionID,
    ) -> Self {
        Self {
            first_page_id,
            buffer_pool_manager,
            transaction_manager,
            lock_manager,
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

    pub fn delete(&mut self, rid: RID) -> Result<()> {
        let page_id = rid.0;
        let tuple_index = rid.1;
        self.lock_manager
            .read()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .lock(rid, self.txn_id)?;
        let page = self
            .buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .fetch_page(page_id)?;
        page.write()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .with_table_page_mut(|table_page| table_page.delete(tuple_index, self.txn_id));
        Ok(())
    }
}

pub struct TableIterator {
    heap: TableHeap,
    current_page_id: PageID,
    next_page_id: Option<PageID>,
    tuples: Vec<Box<[u8]>>,
    tuple_index: usize,
}

impl TableHeap {
    pub fn iter(self) -> TableIterator {
        let page_id = self.first_page_id;
        TableIterator {
            heap: self,
            current_page_id: page_id,
            next_page_id: Some(page_id),
            tuples: Vec::new(),
            tuple_index: 0,
        }
    }
}

impl Iterator for TableIterator {
    type Item = Tuple;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let tuple = self.next_internal();
            match tuple {
                Some(tuple) => {
                    if self.heap.transaction_manager.lock().ok()?.is_visible(
                        self.heap.txn_id,
                        tuple.xmin(),
                        tuple.xmax(),
                    ) {
                        return Some(tuple);
                    }
                }
                None => return None,
            }
        }
    }
}

impl TableIterator {
    pub fn reset(&mut self) {
        self.current_page_id = self.heap.first_page_id;
        self.next_page_id = Some(self.heap.first_page_id);
        self.tuples = Vec::new();
        self.tuple_index = 0;
    }
    fn next_internal(&mut self) -> Option<Tuple> {
        if self.tuple_index >= self.tuples.len() {
            let next_page_id = self.next_page_id?;
            let page = self
                .heap
                .buffer_pool_manager
                .lock()
                .ok()?
                .fetch_page(next_page_id)
                .ok()?;
            self.current_page_id = next_page_id;
            self.next_page_id = page.read().ok()?.with_table_page(|table_page| {
                if table_page.next_page_id() == INVALID_PAGE_ID {
                    None
                } else {
                    Some(table_page.next_page_id())
                }
            });
            self.tuples = page
                .read()
                .ok()?
                .with_table_page(|table_page| table_page.get_tuples());
            self.tuple_index = 0;
            self.heap
                .buffer_pool_manager
                .lock()
                .ok()?
                .unpin_page(next_page_id, false)
                .ok()?;
        }
        if self.tuple_index >= self.tuples.len() {
            return None;
        }
        let tuple = Tuple::new(
            Some(RID(self.current_page_id, self.tuple_index as u32)),
            &self.tuples[self.tuple_index],
        );
        self.tuple_index += 1;
        Some(tuple)
    }
}

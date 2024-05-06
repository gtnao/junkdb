use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    common::{TransactionID, INVALID_PAGE_ID},
    log::{LogRecord, LogRecordBody},
    page::{b_plus_tree_leaf_page::BPlusTreeLeafPage, table_page::TablePage, Page},
};

pub struct RecoveryManager {
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    log_records: Vec<LogRecord>,
    active_txn_ids: Vec<TransactionID>,
}

impl RecoveryManager {
    pub fn new(
        buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
        log_records: Vec<LogRecord>,
    ) -> Self {
        Self {
            buffer_pool_manager,
            log_records,
            active_txn_ids: vec![],
        }
    }

    pub fn recover(&mut self) -> Result<()> {
        self.redo()?;
        self.undo()?;
        self.buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .flush_all_pages()?;
        Ok(())
    }

    pub fn redo(&mut self) -> Result<()> {
        for log_record in &self.log_records {
            match log_record.body {
                LogRecordBody::BeginTransaction => {
                    self.active_txn_ids.push(log_record.txn_id);
                }
                LogRecordBody::CommitTransaction | LogRecordBody::AbortTransaction => {
                    self.active_txn_ids.retain(|&x| x != log_record.txn_id);
                }
                LogRecordBody::InsertToTablePage(ref body) => {
                    let page = self
                        .buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .fetch_page(body.page_id)?;
                    let page_lsn = page
                        .read()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .with_table_page(|table_page| table_page.lsn());
                    if page_lsn < log_record.lsn {
                        page.write()
                            .map_err(|_| anyhow::anyhow!("lock error"))?
                            .with_table_page_mut(|table_page| -> Result<()> {
                                table_page
                                    .insert(&body.data)
                                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                                table_page.set_lsn(log_record.lsn);
                                Ok(())
                            })?;
                    }
                    self.buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .unpin_page(body.page_id, true)?;
                }
                LogRecordBody::DeleteFromTablePage(ref body) => {
                    let page = self
                        .buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .fetch_page(body.rid.0)?;
                    let page_lsn = page
                        .read()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .with_table_page(|table_page| table_page.lsn());
                    if page_lsn < log_record.lsn {
                        page.write()
                            .map_err(|_| anyhow::anyhow!("lock error"))?
                            .with_table_page_mut(|table_page| {
                                table_page.delete(body.rid.1, log_record.txn_id);
                                table_page.set_lsn(log_record.lsn);
                            });
                    }
                    self.buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .unpin_page(body.rid.0, true)?;
                }
                LogRecordBody::NewTablePage(ref body) => {
                    let mut table_page = TablePage::new(body.page_id);
                    table_page.set_lsn(log_record.lsn);
                    let page = Page::Table(table_page);
                    self.buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .init_page_for_recovery(body.page_id, page)?;
                }
                LogRecordBody::NewBPlusTreeLeafPage(ref body) => {
                    // TODO:
                    let mut b_plus_tree_leaf_page =
                        BPlusTreeLeafPage::new(body.page_id, INVALID_PAGE_ID, None);
                    b_plus_tree_leaf_page.set_lsn(log_record.lsn);
                    let page = Page::BPlusTreeLeaf(b_plus_tree_leaf_page);
                    self.buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .init_page_for_recovery(body.page_id, page)?;
                }
                LogRecordBody::SetNextPageID(ref body) => {
                    let page = self
                        .buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .fetch_page(body.page_id)?;
                    let page_lsn = page
                        .read()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .with_table_page(|table_page| table_page.lsn());
                    if page_lsn < log_record.lsn {
                        page.write()
                            .map_err(|_| anyhow::anyhow!("lock error"))?
                            .with_table_page_mut(|table_page| {
                                table_page.set_next_page_id(body.next_page_id);
                                table_page.set_lsn(log_record.lsn);
                            });
                    }
                    self.buffer_pool_manager
                        .lock()
                        .map_err(|_| anyhow::anyhow!("lock error"))?
                        .unpin_page(body.page_id, true)?;
                }
            }
        }
        Ok(())
    }

    pub fn undo(&self) -> Result<()> {
        // TODO:
        Ok(())
    }
}

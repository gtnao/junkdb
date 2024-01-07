use anyhow::{anyhow, Result};

use crate::{
    plan::SeqScanPlan,
    table::{TableHeap, TableIterator},
    tuple::Tuple,
};

use super::ExecutorContext;

pub struct SeqScanExecutor<'a> {
    pub plan: SeqScanPlan,
    pub executor_context: &'a ExecutorContext,
    pub table_iterator: Option<TableIterator>,
}

impl SeqScanExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        let txn_id = self.executor_context.transaction_id;
        let first_page_id = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_first_page_id_by_table_name(&self.plan.table_name, txn_id)?;
        let table_heap = TableHeap::new(
            first_page_id,
            self.executor_context.buffer_pool_manager.clone(),
            self.executor_context.transaction_manager.clone(),
            self.executor_context.lock_manager.clone(),
            txn_id,
        );
        self.table_iterator = Some(table_heap.iter());
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let table_iterator = self.table_iterator.as_mut().ok_or_else(|| {
            anyhow!("table_iterator is not initialized. call init() before calling next()")
        })?;
        Ok(table_iterator.next())
    }
}

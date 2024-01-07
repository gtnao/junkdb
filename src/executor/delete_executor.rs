use anyhow::{anyhow, Result};

use crate::{plan::DeletePlan, table::TableHeap, tuple::Tuple};

use super::{Executor, ExecutorContext};

pub struct DeleteExecutor<'a> {
    pub plan: DeletePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: Option<TableHeap>,
}

impl DeleteExecutor<'_> {
    pub fn init<'a>(&mut self) -> Result<()> {
        self.child.init()?;
        let txn_id = self.executor_context.transaction_id;
        let first_page_id = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_first_page_id_by_table_name(&self.plan.table_name, txn_id)?;
        self.table_heap = Some(TableHeap::new(
            first_page_id,
            self.executor_context.buffer_pool_manager.clone(),
            self.executor_context.transaction_manager.clone(),
            self.executor_context.lock_manager.clone(),
            txn_id,
        ));
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let tuple = self.child.next()?;
        if let Some(row) = tuple {
            let table_heap = self.table_heap.as_mut().ok_or_else(|| {
                anyhow!("table_heap is not initialized. call init() before calling next()")
            })?;
            let rid = row.rid.ok_or_else(|| anyhow!("rid is None"))?;
            table_heap.delete(rid)?;
            return Ok(Some(Tuple::new(None, &vec![])));
        }
        Ok(None)
    }
}

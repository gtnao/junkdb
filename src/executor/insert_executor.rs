use anyhow::{anyhow, Result};

use crate::{catalog::Schema, plan::InsertPlan, table::TableHeap, tuple::Tuple};

use super::ExecutorContext;

pub struct InsertExecutor<'a> {
    pub plan: InsertPlan,
    pub executor_context: &'a ExecutorContext,
}

impl InsertExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let txn_id = self.executor_context.transaction_id;
        let first_page_id = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_first_page_id_by_table_name(&self.plan.table_name, txn_id)?;
        let mut table_heap = TableHeap::new(
            first_page_id,
            self.executor_context.buffer_pool_manager.clone(),
            self.executor_context.transaction_manager.clone(),
            self.executor_context.lock_manager.clone(),
            txn_id,
        );
        let values = self
            .plan
            .values
            .iter()
            // dummy
            .map(|e| e.eval(&Tuple::new(&vec![]), &Schema { columns: vec![] }))
            .collect::<Vec<_>>();
        table_heap.insert(&values)?;
        Ok(None)
    }
}

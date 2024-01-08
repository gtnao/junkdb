use anyhow::{anyhow, Result};

use crate::{plan::UpdatePlan, table::TableHeap, tuple::Tuple};

use super::{Executor, ExecutorContext};

pub struct UpdateExecutor<'a> {
    pub plan: UpdatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: Option<TableHeap>,
    pub count: usize,
}

impl UpdateExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let txn_id = self.executor_context.transaction_id;
        self.table_heap = Some(TableHeap::new(
            self.plan.first_page_id,
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
            let mut new_values = row.values(self.plan.child.schema());
            for assignment in self.plan.assignments.iter() {
                new_values[assignment.column_index] =
                    assignment.value.eval(&row, self.plan.child.schema());
            }
            table_heap.delete(rid)?;
            table_heap.insert(&new_values)?;
            self.count += 1;
            return Ok(Some(Tuple::new(None, &vec![])));
        }
        Ok(None)
    }
}

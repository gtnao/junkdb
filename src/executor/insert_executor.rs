use anyhow::Result;

use crate::{catalog::Schema, plan::InsertPlan, table::TableHeap, tuple::Tuple};

use super::ExecutorContext;

pub struct InsertExecutor<'a> {
    pub plan: InsertPlan,
    pub executor_context: &'a ExecutorContext,
    pub count: usize,
}

impl InsertExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let txn_id = self.executor_context.transaction_id;
        let mut table_heap = TableHeap::new(
            self.plan.first_page_id,
            self.executor_context.buffer_pool_manager.clone(),
            self.executor_context.transaction_manager.clone(),
            self.executor_context.lock_manager.clone(),
            txn_id,
        );
        let values = self
            .plan
            .table_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let raw_value = self.plan.values[i].eval(
                    &vec![&Tuple::new(None, &vec![])],
                    &vec![&Schema { columns: vec![] }],
                );
                raw_value.convert_to(&c.data_type)
            })
            .collect::<Result<Vec<_>>>()?;
        table_heap.insert(&values)?;
        self.count += 1;
        Ok(None)
    }
}

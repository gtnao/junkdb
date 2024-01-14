use anyhow::{anyhow, Result};

use crate::{
    common::INVALID_TRANSACTION_ID,
    plan::UpdatePlan,
    table::TableHeap,
    tuple::Tuple,
    value::{unsigned_big_integer::UnsignedBigIntegerValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct UpdateExecutor<'a> {
    pub plan: UpdatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: TableHeap,
    pub count: u64,
    pub executed: bool,
}

impl UpdateExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        while let Some(row) = self.child.next()? {
            let rid = row.rid.ok_or_else(|| anyhow!("rid is None"))?;
            let mut new_values = row.values(self.plan.child.schema());
            for assignment in self.plan.assignments.iter() {
                new_values[assignment.column_index] = assignment
                    .value
                    .eval(&vec![&row], &vec![&self.plan.child.schema()]);
            }
            self.table_heap.delete(rid)?;
            self.table_heap.insert(&new_values)?;
            self.count += 1;
        }
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        let values = vec![Value::UnsignedBigInteger(UnsignedBigIntegerValue(
            self.count,
        ))];
        let bytes = Tuple::serialize(INVALID_TRANSACTION_ID, INVALID_TRANSACTION_ID, &values);
        self.executed = true;
        Ok(Some(Tuple::new(None, &bytes)))
    }
}

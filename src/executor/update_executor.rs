use anyhow::{anyhow, Result};

use crate::{plan::UpdatePlan, table::TableHeap, tuple::Tuple};

use super::{Executor, ExecutorContext};

pub struct UpdateExecutor<'a> {
    pub plan: UpdatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: TableHeap,
    pub count: usize,
}

impl UpdateExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let tuple = self.child.next()?;
        if let Some(row) = tuple {
            let rid = row.rid.ok_or_else(|| anyhow!("rid is None"))?;
            let mut new_values = row.values(self.plan.child.schema());
            let tuples = vec![&row];
            for assignment in self.plan.assignments.iter() {
                new_values[assignment.column_index] = assignment
                    .value
                    .eval(&tuples, &vec![&self.plan.child.schema()]);
            }
            self.table_heap.delete(rid)?;
            self.table_heap.insert(&new_values)?;
            self.count += 1;
            return Ok(Some(Tuple::new(None, &vec![])));
        }
        Ok(None)
    }
}

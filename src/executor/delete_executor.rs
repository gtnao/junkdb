use anyhow::{anyhow, Result};

use crate::{plan::DeletePlan, table::TableHeap, tuple::Tuple};

use super::{Executor, ExecutorContext};

pub struct DeleteExecutor<'a> {
    pub plan: DeletePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: TableHeap,
    pub count: usize,
}

impl DeleteExecutor<'_> {
    pub fn init<'a>(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(row) = self.child.next()? {
            let rid = row.rid.ok_or_else(|| anyhow!("rid is None"))?;
            self.table_heap.delete(rid)?;
            self.count += 1;
            return Ok(Some(Tuple::new(None, &vec![])));
        }
        Ok(None)
    }
}

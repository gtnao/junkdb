use anyhow::{anyhow, Result};

use crate::{
    common::INVALID_TRANSACTION_ID,
    plan::DeletePlan,
    table::TableHeap,
    tuple::Tuple,
    value::{UnsignedBigIntegerValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct DeleteExecutor<'a> {
    pub plan: DeletePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: TableHeap,
    pub count: u64,
    pub executed: bool,
}

impl DeleteExecutor<'_> {
    pub fn init<'a>(&mut self) -> Result<()> {
        self.child.init()?;
        while let Some(row) = self.child.next()? {
            let rid = row.rid.ok_or_else(|| anyhow!("rid is None"))?;
            self.table_heap.delete(rid)?;
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

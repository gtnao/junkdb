use anyhow::Result;

use crate::{plan::SeqScanPlan, table::TableIterator, tuple::Tuple};

use super::ExecutorContext;

pub struct SeqScanExecutor<'a> {
    pub plan: SeqScanPlan,
    pub executor_context: &'a ExecutorContext,
    pub table_iterator: TableIterator,
}

impl SeqScanExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.table_iterator.reset();
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        Ok(self.table_iterator.next())
    }
}

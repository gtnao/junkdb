use anyhow::Result;

use crate::{plan::IndexScanPlan, tuple::Tuple};

use super::ExecutorContext;

pub struct IndexScanExecutor<'a> {
    pub plan: IndexScanPlan,
    pub executor_context: &'a ExecutorContext,
    pub index_id: i64,
}

impl IndexScanExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        let index = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("Catalog lock error"))?
            .get_index(self.index_id, self.executor_context.transaction_id)?;
        println!("Index: {:?}", index);
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        Ok(None)
    }
}

use anyhow::Result;

use crate::{common::RID, index::IndexManager, plan::IndexScanPlan, tuple::Tuple};

use super::ExecutorContext;

pub struct IndexScanExecutor<'a> {
    pub plan: IndexScanPlan,
    pub executor_context: &'a ExecutorContext,
    pub index_id: i64,
    pub rids: Option<Vec<RID>>,
    pub cursor: usize,
}

impl IndexScanExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        let mut index = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("Catalog lock error"))?
            .get_index(self.index_id, self.executor_context.transaction_id)?;
        index.set_schema(self.plan.table_schema.clone());
        let index_manager = IndexManager::new(
            index,
            self.executor_context.catalog.clone(),
            self.executor_context.buffer_pool_manager.clone(),
        );
        let right_value = self
            .plan
            .binary_expression
            .right
            .eval(&vec![&Tuple::new(None, &[])], &vec![])
            .map_err(|_| anyhow::anyhow!("eval error"))?;
        self.rids = index_manager.lookup(&right_value)?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(rids) = &self.rids {
            if self.cursor >= rids.len() {
                return Ok(None);
            }
            let rid = rids[self.cursor];
            self.cursor += 1;
            let page = self
                .executor_context
                .buffer_pool_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .fetch_page(rid.0)?;
            let tuple_data = page
                .read()
                .map_err(|_| anyhow::anyhow!("read error"))?
                .with_table_page(|table_page| table_page.get_tuple(rid.1 as usize));
            let tuple = Tuple::new(None, &tuple_data);
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}

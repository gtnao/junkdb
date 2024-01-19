use anyhow::Result;

use crate::{plan::EmptyRowPlan, tuple::Tuple};

use super::ExecutorContext;

pub struct EmptyRowExecutor<'a> {
    pub plan: EmptyRowPlan,
    pub executor_context: &'a ExecutorContext,
    pub returned: bool,
}

impl EmptyRowExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if self.returned {
            Ok(None)
        } else {
            self.returned = true;
            Ok(Some(Tuple::temp_tuple(&vec![])))
        }
    }
}

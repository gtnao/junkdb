use anyhow::Result;

use crate::{plan::ProjectPlan, tuple::Tuple, value::Value};

use super::{Executor, ExecutorContext};

pub struct ProjectExecutor<'a> {
    pub plan: ProjectPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}

impl ProjectExecutor<'_> {
    pub fn init<'a>(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let tuple = self.child.next()?;
        if let Some(row) = tuple {
            let tuples = vec![&row];
            let values = self
                .plan
                .select_elements
                .iter()
                .map(|element| {
                    element
                        .expression
                        .eval(&tuples, &vec![&self.plan.child.schema()])
                })
                .collect::<Vec<Value>>();
            return Ok(Some(Tuple::temp_tuple(&values)));
        }
        Ok(None)
    }
}

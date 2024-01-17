use anyhow::Result;

use crate::{
    plan::FilterPlan,
    tuple::Tuple,
    value::{boolean::BooleanValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct FilterExecutor<'a> {
    pub plan: FilterPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}

impl FilterExecutor<'_> {
    pub fn init<'a>(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let mut tuple = self.child.next()?;
        while let Some(row) = tuple {
            let tuples = vec![&row];
            if self
                .plan
                .condition
                .eval(&tuples, &vec![&self.plan.child.schema()])?
                == Value::Boolean(BooleanValue(true))
            {
                return Ok(Some(row));
            }
            tuple = self.child.next()?;
        }
        Ok(None)
    }
}

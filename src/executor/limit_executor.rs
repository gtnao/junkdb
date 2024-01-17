use anyhow::Result;

use crate::{plan::LimitPlan, tuple::Tuple, value::Value};

use super::{Executor, ExecutorContext};

pub struct LimitExecutor<'a> {
    pub plan: LimitPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub result: Vec<Tuple>,
    pub cursor: usize,
}

impl LimitExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let mut limit = match self.plan.limit.count.eval(&vec![], &vec![])? {
            Value::Integer(v) => v.0,
            _ => Err(anyhow::anyhow!("limit count must be integer"))?,
        };
        let mut offset = match self.plan.limit.offset.eval(&vec![], &vec![])? {
            Value::Integer(v) => v.0,
            _ => Err(anyhow::anyhow!("offset count must be integer"))?,
        };
        let mut result = vec![];
        while let Some(tuple) = self.child.next()? {
            if offset > 0 {
                offset -= 1;
                continue;
            }
            if limit > 0 {
                limit -= 1;
                result.push(tuple);
            } else {
                break;
            }
        }
        self.result = result;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if self.cursor < self.result.len() {
            let tuple = self.result[self.cursor].clone();
            self.cursor += 1;
            return Ok(Some(tuple));
        }
        Ok(None)
    }
}

use anyhow::Result;

use crate::{
    common::INVALID_TRANSACTION_ID,
    plan::NestedLoopJoinPlan,
    tuple::Tuple,
    value::{BooleanValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct NestedLoopJoinExecutor<'a> {
    pub plan: NestedLoopJoinPlan,
    pub outer_child: Box<Executor<'a>>,
    pub inner_children: Vec<Box<Executor<'a>>>,
    pub tuples: Vec<Option<Tuple>>,
    pub executor_context: &'a ExecutorContext,
}

impl NestedLoopJoinExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.outer_child.init()?;
        self.tuples.push(self.outer_child.next()?);
        for inner_child in &mut self.inner_children {
            inner_child.init()?;
            self.tuples.push(inner_child.next()?);
        }
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(_) = self.tuples[0] {
            if let Some(inner_tuples) = self.inner(0)? {
                let mut tuples = vec![self.tuples[0].clone()];
                tuples.extend(inner_tuples);
                let mut values = tuples[0]
                    .clone()
                    .unwrap()
                    .values(&self.plan.outer_child.schema());
                for i in 1..tuples.len() {
                    if let Some(inner_tuple) = &tuples[i] {
                        values
                            .extend(inner_tuple.values(&self.plan.inner_children[i - 1].schema()));
                    } else {
                        values.extend(vec![
                            Value::Null;
                            self.plan.inner_children[i - 1].schema().columns.len()
                        ]);
                    }
                }
                let buffer =
                    Tuple::serialize(INVALID_TRANSACTION_ID, INVALID_TRANSACTION_ID, &values);
                let merged_tuple = Tuple::new(None, &buffer);
                return Ok(Some(merged_tuple));
            }
            self.tuples[0] = self.outer_child.next()?;
            self.inner_children[0].init()?;
            self.tuples[1] = self.inner_children[0].next()?;
        }
        Ok(None)
    }

    fn inner(&mut self, index: usize) -> Result<Option<Vec<Option<Tuple>>>> {
        while let Some(_) = &self.tuples[index + 1] {
            let tuples = self
                .tuples
                .iter()
                .map(|tuple| tuple.as_ref().unwrap())
                .collect::<Vec<_>>();
            let condition = self.plan.conditions[index].as_ref().map_or_else(
                || true,
                |condition| {
                    let mut schemas = vec![self.plan.outer_child.schema()];
                    for inner_child in &self.plan.inner_children {
                        schemas.push(inner_child.schema());
                    }
                    condition.eval(&tuples, &schemas) == Value::Boolean(BooleanValue(true))
                },
            );
            if condition {
                if index == self.inner_children.len() - 1 {
                    let tuple = self.tuples[index + 1].clone();
                    self.tuples[index + 1] = self.inner_children[index].next()?;
                    return Ok(Some(vec![tuple]));
                }
                let result = self.inner(index + 1)?;
                if let Some(tuples) = result {
                    let tuple = self.tuples[index + 1].clone();
                    self.tuples[index + 1] = self.inner_children[index].next()?;
                    let mut ret = vec![tuple];
                    ret.extend(tuples);
                    return Ok(Some(ret));
                }
            }
            self.tuples[index + 1] = self.inner_children[index].next()?;
            if index != self.inner_children.len() - 1 {
                self.tuples[index + 2] = self.inner_children[index + 1].next()?;
                self.inner_children[index + 1].init()?;
            }
        }
        Ok(None)
    }
}

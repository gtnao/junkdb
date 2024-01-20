use anyhow::Result;

use crate::{
    binder::BoundExpressionAST,
    parser::Order,
    plan::SortPlan,
    tuple::Tuple,
    value::{boolean::BooleanValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct SortExecutor<'a> {
    pub plan: SortPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub result: Vec<Tuple>,
    pub cursor: usize,
}

impl SortExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let mut result = vec![];
        while let Some(tuple) = self.child.next()? {
            result.push(tuple);
        }
        let indexes = self
            .plan
            .order_by
            .iter()
            .map(|order_by_element| match &order_by_element.expression {
                BoundExpressionAST::Path(path) => Ok(path.column_index),
                _ => Err(anyhow::anyhow!("order by expression must be path")),
            })
            .collect::<Result<Vec<usize>>>()?;
        result.sort_by(|a, b| {
            for (i, order_by_element) in self.plan.order_by.iter().enumerate() {
                let a_values = a.values(self.plan.child.schema());
                let b_values = b.values(self.plan.child.schema());
                let a_value = &a_values[indexes[i]];
                let b_value = &b_values[indexes[i]];
                let cmp = match order_by_element.order {
                    Order::Asc => {
                        // TODO: remove unwrap
                        if a_value.perform_equal(b_value).unwrap()
                            == Value::Boolean(BooleanValue(true))
                        {
                            std::cmp::Ordering::Equal
                        } else if a_value.perform_greater_than(b_value).unwrap()
                            == Value::Boolean(BooleanValue(true))
                        {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        }
                    }
                    Order::Desc => {
                        if a_value.perform_equal(b_value).unwrap()
                            == Value::Boolean(BooleanValue(true))
                        {
                            std::cmp::Ordering::Equal
                        } else if a_value.perform_greater_than(b_value).unwrap()
                            == Value::Boolean(BooleanValue(true))
                        {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        }
                    }
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
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

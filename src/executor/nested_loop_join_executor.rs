use std::mem::replace;

use anyhow::Result;

use crate::{
    parser::JoinType,
    plan::NestedLoopJoinPlan,
    tuple::Tuple,
    value::{boolean::BooleanValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct NestedLoopJoinExecutor<'a> {
    pub plan: NestedLoopJoinPlan,
    pub children: Vec<Box<Executor<'a>>>,
    pub tuples: Vec<Option<Tuple>>,
    pub executor_context: &'a ExecutorContext,
    // TODO: other implementation
    pub internal_left_join_statuses: Vec<bool>,
}

impl NestedLoopJoinExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        for child in &mut self.children {
            child.init()?;
            self.tuples.push(child.next()?);
        }
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let res = self.internal_next(0)?;
        if let Some(mut res) = res {
            res.reverse();
            let mut values_list = vec![];
            for (i, tuple) in res.iter().enumerate() {
                let values = tuple.values(&self.plan.children[i].schema());
                values_list.push(values);
            }
            let values = values_list.into_iter().flatten().collect::<Vec<_>>();
            let tuple = Tuple::temp_tuple(&values);
            return Ok(Some(tuple));
        }
        Ok(None)
    }
    fn internal_next(&mut self, depth: usize) -> Result<Option<Vec<Tuple>>> {
        let max_depth = self.plan.children.len() - 1;
        while self.tuples[depth].is_some() {
            // last depth
            if depth == max_depth {
                // left join check
                if self.plan.join_types[depth - 1] == JoinType::Left
                    && self.internal_left_join_statuses[depth - 1]
                {
                    let v = replace(&mut self.tuples[depth], None);
                    self.internal_left_join_statuses[depth - 1] = false;
                    return Ok(Some(vec![v.unwrap()]));
                }

                // condition check
                let condition_result = self.plan.conditions[depth - 1].as_ref().map_or_else(
                    || true,
                    |condition| {
                        let tuples = self
                            .tuples
                            .iter()
                            .map(|tuple| tuple.as_ref().unwrap())
                            .collect::<Vec<_>>();
                        let schemas = self
                            .plan
                            .children
                            .iter()
                            .map(|child| child.schema())
                            .collect::<Vec<_>>();
                        condition.eval(&tuples, &schemas) == Value::Boolean(BooleanValue(true))
                    },
                );
                if !condition_result {
                    self.tuples[depth] = self.children[depth].next()?;

                    // left join
                    if self.tuples[depth].is_none() {
                        if self.plan.join_types[depth - 1] == JoinType::Left {
                            self.internal_left_join_statuses[depth - 1] = true;
                            let dummy = Tuple::temp_tuple(&vec![
                                Value::Null;
                                self.plan.children[depth]
                                    .schema()
                                    .columns
                                    .len()
                            ]);
                            self.tuples[depth] = Some(dummy);
                        }
                    }

                    continue;
                }
                // get and update
                let v = replace(&mut self.tuples[depth], self.children[depth].next()?);
                if let Some(v) = v {
                    return Ok(Some(vec![v]));
                } else {
                    return Ok(None);
                }
            }

            // root and internal depth

            // none check(for left join)
            let none_exist = self.tuples.iter().any(|v| v.is_none());
            if none_exist {
                // reset child iterator
                self.children[depth + 1].init()?;
                self.tuples[depth + 1] = self.children[depth + 1].next()?;
                // update self iterator
                self.tuples[depth] = self.children[depth].next()?;
                continue;
            }
            // condition check(except root)
            if depth != 0 {
                let condition_result = self.plan.conditions[depth - 1].as_ref().map_or_else(
                    || true,
                    |condition| {
                        let tuples = self
                            .tuples
                            .iter()
                            .map(|tuple| tuple.as_ref().unwrap())
                            .collect::<Vec<_>>();
                        let schemas = self
                            .plan
                            .children
                            .iter()
                            .map(|child| child.schema())
                            .collect::<Vec<_>>();
                        condition.eval(&tuples, &schemas) == Value::Boolean(BooleanValue(true))
                    },
                );
                if !condition_result {
                    self.tuples[depth] = self.children[depth].next()?;
                    continue;
                }
            }
            let res = self.internal_next(depth + 1)?;
            if let Some(mut res) = res {
                // child iterator has result
                let v = self.tuples[depth].as_ref().unwrap();
                res.push(v.clone());
                return Ok(Some(res));
            } else {
                // child iterator has no result
                // reset child iterator
                self.children[depth + 1].init()?;
                self.tuples[depth + 1] = self.children[depth + 1].next()?;
                // update self iterator
                self.tuples[depth] = self.children[depth].next()?;
            }
        }
        Ok(None)
    }
}

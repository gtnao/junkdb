use anyhow::Result;

use crate::{
    catalog::Schema,
    common::INVALID_TRANSACTION_ID,
    plan::InsertPlan,
    table::TableHeap,
    tuple::Tuple,
    value::{integer::IntegerValue, Value},
};

use super::ExecutorContext;

pub struct InsertExecutor<'a> {
    pub plan: InsertPlan,
    pub executor_context: &'a ExecutorContext,
    pub table_heap: TableHeap,
    pub count: u32,
    pub executed: bool,
}

impl InsertExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        let values = self
            .plan
            .table_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let raw_value = self.plan.values[i].eval(
                    &vec![&Tuple::new(None, &[])],
                    &vec![&Schema { columns: vec![] }],
                )?;
                raw_value.convert_to(&c.data_type)
            })
            .collect::<Result<Vec<_>>>()?;
        self.table_heap.insert(&values)?;
        self.count += 1;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if self.executed {
            return Ok(None);
        }
        let values = vec![Value::Integer(IntegerValue(self.count as i64))];
        let bytes = Tuple::serialize(INVALID_TRANSACTION_ID, INVALID_TRANSACTION_ID, &values);
        self.executed = true;
        Ok(Some(Tuple::new(None, &bytes)))
    }
}

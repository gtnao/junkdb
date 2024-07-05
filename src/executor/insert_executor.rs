use anyhow::Result;

use crate::{
    catalog::Schema,
    common::INVALID_TRANSACTION_ID,
    index::IndexManager,
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
    pub table_schema: Schema,
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
                let index;
                match &self.plan.column_names {
                    Some(column_names) => {
                        let position = column_names.iter().position(|x| x == &c.name);
                        match position {
                            Some(pos) => index = pos,
                            None => return Ok(Value::Null)
                        }
                    },
                    None => {
                        index = i;
                    }
                }
                let raw_value = self.plan.values[index].eval(
                    &vec![&Tuple::new(None, &[])],
                    &vec![&Schema { columns: vec![] }],
                )?;
                if raw_value.is_null_value() {
                    return Ok(Value::Null);
                }
                raw_value.convert_to(&c.data_type)
            })
            .collect::<Result<Vec<_>>>()?;
        let rid = self.table_heap.insert(&values)?;
        self.count += 1;
        let mut indexes = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("Catalog lock error"))?
            .get_indexes_by_table_name(
                &self.plan.table_name,
                self.executor_context.transaction_id,
            )?;
        for index in indexes.iter_mut() {
            index.set_schema(self.plan.table_schema.clone());
        }
        for index in indexes {
            // TODO: only support single column index
            let column_name = index.columns[0].clone();
            let index_manager = IndexManager::new(
                index,
                self.executor_context.catalog.clone(),
                self.executor_context.buffer_pool_manager.clone(),
            );
            for (i, column) in self.plan.table_schema.columns.iter().enumerate() {
                if column.name == column_name {
                    let right_value = values[i].clone();
                    index_manager.insert(&right_value, rid)?;
                    break;
                }
            }
        }
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

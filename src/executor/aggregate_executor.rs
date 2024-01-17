use std::collections::HashMap;

use anyhow::Result;

use crate::{
    catalog::DataType,
    plan::AggregatePlan,
    tuple::Tuple,
    value::{boolean::BooleanValue, integer::IntegerValue, Value},
};

use super::{Executor, ExecutorContext};

pub struct AggregateExecutor<'a> {
    pub plan: AggregatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
    pub aggregate_table: AggregateTable,
    pub result: Vec<Vec<Value>>,
    pub index: usize,
}

impl AggregateExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        self.child.init()?;
        while let Some(tuple) = self.child.next()? {
            let mut keys = vec![];
            for expression in &self.plan.group_by {
                keys.push(expression.eval(&vec![&tuple], &vec![&self.plan.child.schema()])?);
            }
            for (i, expression) in self.plan.aggregate_functions.iter().enumerate() {
                let value = match &*expression.function_name {
                    "COUNT" => Value::Integer(IntegerValue(1)),
                    _ => expression
                        .arguments
                        .first()
                        .ok_or(anyhow::anyhow!("SUM argument error"))?
                        .eval(&vec![&tuple], &vec![&self.plan.child.schema()])?,
                };
                self.aggregate_table.add(
                    keys.clone(),
                    value,
                    i,
                    self.plan.aggregate_functions.len(),
                );
            }
        }
        self.result = self.aggregate_table.aggregate(
            self.plan
                .aggregate_functions
                .clone()
                .into_iter()
                .map(|f| f.function_name)
                .collect(),
        )?;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        if self.index >= self.result.len() {
            return Ok(None);
        }
        let tuple = Tuple::temp_tuple(&self.result[self.index]);
        self.index += 1;
        Ok(Some(tuple))
    }
}

pub struct AggregateTable {
    map: Box<HashMap<Value, AggregateTableValue>>,
}
enum AggregateTableValue {
    Table(AggregateTable),
    Value(Vec<Vec<Value>>),
}
impl AggregateTable {
    pub fn new() -> Self {
        Self {
            map: Box::new(HashMap::new()),
        }
    }
    fn add(&mut self, keys: Vec<Value>, value: Value, function_index: usize, function_max: usize) {
        let depth = keys.len();
        let mut current: &mut AggregateTable = self;
        for (i, key) in keys.into_iter().enumerate() {
            let aggregate_table_value = current.map.entry(key).or_insert_with(|| {
                if i == depth - 1 {
                    AggregateTableValue::Value(vec![vec![]; function_max])
                } else {
                    AggregateTableValue::Table(AggregateTable::new())
                }
            });
            match aggregate_table_value {
                AggregateTableValue::Table(table) => {
                    current = table;
                }
                AggregateTableValue::Value(values) => {
                    values[function_index].push(value);
                    return;
                }
            }
        }
    }

    fn aggregate(&self, function_names: Vec<String>) -> Result<Vec<Vec<Value>>> {
        let mut result = vec![];
        for (key, value) in self.map.iter() {
            match value {
                AggregateTableValue::Table(table) => {
                    let mut rows = table.aggregate(function_names.clone())?;
                    for row in &mut rows {
                        row.insert(0, key.clone());
                    }
                    result.append(&mut rows);
                }
                AggregateTableValue::Value(values_list) => {
                    let mut row = vec![];
                    row.push(key.clone());
                    for (i, values) in values_list.iter().enumerate() {
                        match &*function_names[i] {
                            "COUNT" => {
                                let mut sum = 0;
                                for value in values {
                                    if value.is_null_value() {
                                        continue;
                                    }
                                    sum += 1;
                                }
                                row.push(Value::Integer(IntegerValue(sum)));
                            }
                            "SUM" => {
                                let mut sum = 0;
                                for value in values {
                                    if value.is_null_value() {
                                        continue;
                                    }
                                    match value.convert_to(&DataType::Integer)? {
                                        Value::Integer(v) => {
                                            sum += v.0;
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                                row.push(Value::Integer(IntegerValue(sum)));
                            }
                            "MAX" => {
                                let mut max = Value::Integer(IntegerValue(i64::MIN));
                                for value in values {
                                    if value.is_null_value() {
                                        continue;
                                    }
                                    match value.convert_to(&DataType::Integer)? {
                                        Value::Integer(v) => {
                                            if value.perform_greater_than(&max)?
                                                == Value::Boolean(BooleanValue(true))
                                            {
                                                max = Value::Integer(v);
                                            }
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                                row.push(max);
                            }
                            "MIN" => {
                                let mut min = Value::Integer(IntegerValue(i64::MAX));
                                for value in values {
                                    if value.is_null_value() {
                                        continue;
                                    }
                                    match value.convert_to(&DataType::Integer)? {
                                        Value::Integer(v) => {
                                            if value.perform_less_than(&min)?
                                                == Value::Boolean(BooleanValue(true))
                                            {
                                                min = Value::Integer(v);
                                            }
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                                row.push(min);
                            }
                            "AVG" => {
                                let mut sum = 0;
                                let mut count = 0;
                                for value in values {
                                    if value.is_null_value() {
                                        continue;
                                    }
                                    match value.convert_to(&DataType::Integer)? {
                                        Value::Integer(v) => {
                                            sum += v.0;
                                            count += 1;
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                                row.push(Value::Integer(IntegerValue(sum / count)));
                            }
                            _ => Err(anyhow::anyhow!("unknown aggregate function error"))?,
                        }
                    }
                    result.push(row);
                }
            }
        }
        Ok(result)
    }
}

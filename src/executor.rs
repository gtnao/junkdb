use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    common::TransactionID,
    concurrency::TransactionManager,
    lock::LockManager,
    plan::Plan,
    tuple::Tuple,
    value::{UnsignedBigIntegerValue, Value},
};

use self::{
    delete_executor::DeleteExecutor, filter_executor::FilterExecutor,
    insert_executor::InsertExecutor, nested_loop_join_executor::NestedLoopJoinExecutor,
    project_executor::ProjectExecutor, seq_scan_executor::SeqScanExecutor,
    update_executor::UpdateExecutor,
};

mod delete_executor;
mod filter_executor;
mod insert_executor;
mod nested_loop_join_executor;
mod project_executor;
mod seq_scan_executor;
mod update_executor;

pub struct ExecutorContext {
    pub transaction_id: TransactionID,
    pub buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    pub lock_manager: Arc<RwLock<LockManager>>,
    pub transaction_manager: Arc<Mutex<TransactionManager>>,
    pub catalog: Arc<Mutex<Catalog>>,
}

pub struct ExecutorEngine {
    plan: Plan,
    context: ExecutorContext,
}
impl ExecutorEngine {
    pub fn new(plan: Plan, context: ExecutorContext) -> Self {
        Self { plan, context }
    }
    pub fn execute(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut executor = self.create_executor(&self.plan);
        executor.init()?;
        let mut tuple = executor.next()?;
        let mut result = vec![];
        while let Some(t) = &tuple {
            if !self.is_count_supported(&self.plan) {
                result.push(t.values(&self.plan.schema()));
            }
            tuple = executor.next()?;
        }
        if let Some(count_result) = self.count_result(&executor) {
            return Ok(count_result);
        }
        Ok(result)
    }
    fn create_executor(&self, plan: &Plan) -> Executor {
        match plan {
            Plan::SeqScan(plan) => Executor::SeqScan(SeqScanExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                table_iterator: None,
            }),
            Plan::Filter(plan) => Executor::Filter(FilterExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
            }),
            Plan::Project(plan) => Executor::Project(ProjectExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
            }),
            Plan::NestedLoopJoin(plan) => {
                let outer_child = self.create_executor(&plan.outer_child);
                let inner_children = plan
                    .inner_children
                    .iter()
                    .map(|child| Box::new(self.create_executor(child)))
                    .collect::<Vec<_>>();
                Executor::NestedLoopJoin(nested_loop_join_executor::NestedLoopJoinExecutor {
                    plan: plan.clone(),
                    outer_child: Box::new(outer_child),
                    inner_children,
                    tuples: vec![],
                    executor_context: &self.context,
                })
            }
            Plan::Insert(plan) => Executor::Insert(InsertExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                count: 0,
            }),
            Plan::Delete(plan) => Executor::Delete(DeleteExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                table_heap: None,
                count: 0,
            }),
            Plan::Update(plan) => Executor::Update(UpdateExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                table_heap: None,
                count: 0,
            }),
        }
    }

    // TODO: use aggregation
    fn count_result(&self, executor: &Executor) -> Option<Vec<Vec<Value>>> {
        let count = match executor {
            Executor::Insert(executor) => executor.count,
            Executor::Delete(executor) => executor.count,
            Executor::Update(executor) => executor.count,
            _ => {
                return None;
            }
        };
        Some(vec![vec![Value::UnsignedBigInteger(
            UnsignedBigIntegerValue(count as u64),
        )]])
    }
    fn is_count_supported(&self, plan: &Plan) -> bool {
        match plan {
            Plan::Insert(_) => true,
            Plan::Delete(_) => true,
            Plan::Update(_) => true,
            _ => false,
        }
    }
}

pub enum Executor<'a> {
    SeqScan(SeqScanExecutor<'a>),
    Filter(FilterExecutor<'a>),
    Project(ProjectExecutor<'a>),
    NestedLoopJoin(NestedLoopJoinExecutor<'a>),
    Insert(InsertExecutor<'a>),
    Delete(DeleteExecutor<'a>),
    Update(UpdateExecutor<'a>),
}
impl Executor<'_> {
    pub fn init(&mut self) -> Result<()> {
        match self {
            Executor::SeqScan(executor) => executor.init(),
            Executor::Filter(executor) => executor.init(),
            Executor::Project(executor) => executor.init(),
            Executor::NestedLoopJoin(executor) => executor.init(),
            Executor::Insert(executor) => executor.init(),
            Executor::Delete(executor) => executor.init(),
            Executor::Update(executor) => executor.init(),
        }
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        match self {
            Executor::SeqScan(executor) => executor.next(),
            Executor::Filter(executor) => executor.next(),
            Executor::Project(executor) => executor.next(),
            Executor::NestedLoopJoin(executor) => executor.next(),
            Executor::Insert(executor) => executor.next(),
            Executor::Delete(executor) => executor.next(),
            Executor::Update(executor) => executor.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        catalog::Schema,
        common::TransactionID,
        instance::Instance,
        lexer::tokenize,
        parser::Parser,
        test_helpers::setup_test_database,
        value::{BooleanValue, IntegerValue, Value, VarcharValue},
    };

    fn execute(
        sql: &str,
        instance: &Instance,
        txn_id: TransactionID,
    ) -> Result<(Vec<Vec<Value>>, Schema)> {
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;
        instance.execute(&statement, txn_id)
    }

    #[test]
    fn test_crud() -> Result<()> {
        let instance = setup_test_database()?;
        let txn_id = instance.begin(None)?;

        let sql = "INSERT INTO t1 VALUES (1, 'foo')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t1 VALUES (2, 'bar')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t1 VALUES (3, 'baz')";
        execute(sql, &instance, txn_id)?;
        let sql = "UPDATE t1 SET c2 = 'qux' WHERE c1 = 1";
        execute(sql, &instance, txn_id)?;
        let sql = "DELETE FROM t1 WHERE c1 = 2";
        execute(sql, &instance, txn_id)?;

        let sql = "SELECT * FROM t1";
        let (rows, _) = execute(sql, &instance, txn_id)?;

        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("baz".to_string()))
                ],
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("qux".to_string()))
                ],
            ]
        );

        let sql = "SELECT * FROM t1 WHERE c2 = 'qux'";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(rows.len(), 1);

        let sql = "SELECT * FROM (SELECT c1 = 1 FROM t1) AS sub1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![
                vec![Value::Boolean(BooleanValue(false))],
                vec![Value::Boolean(BooleanValue(true))],
            ]
        );

        Ok(())
    }
}

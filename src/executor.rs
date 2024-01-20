use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager, catalog::Catalog, common::TransactionID,
    concurrency::TransactionManager, lock::LockManager, log::LogManager, plan::Plan,
    table::TableHeap, tuple::Tuple, value::Value,
};

use self::{
    aggregate_executor::{AggregateExecutor, AggregateTable, AggregateTableValue},
    delete_executor::DeleteExecutor,
    empty_row_executor::EmptyRowExecutor,
    filter_executor::FilterExecutor,
    insert_executor::InsertExecutor,
    limit_executor::LimitExecutor,
    nested_loop_join_executor::NestedLoopJoinExecutor,
    project_executor::ProjectExecutor,
    seq_scan_executor::SeqScanExecutor,
    sort_executor::SortExecutor,
    update_executor::UpdateExecutor,
};

mod aggregate_executor;
mod delete_executor;
mod empty_row_executor;
mod filter_executor;
mod insert_executor;
mod limit_executor;
mod nested_loop_join_executor;
mod project_executor;
mod seq_scan_executor;
mod sort_executor;
mod update_executor;

pub struct ExecutorContext {
    pub transaction_id: TransactionID,
    pub buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    pub lock_manager: Arc<RwLock<LockManager>>,
    pub transaction_manager: Arc<Mutex<TransactionManager>>,
    pub log_manager: Arc<Mutex<LogManager>>,
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
            result.push(t.values(&self.plan.schema()));
            tuple = executor.next()?;
        }
        Ok(result)
    }
    fn create_executor(&self, plan: &Plan) -> Executor {
        match plan {
            Plan::SeqScan(plan) => Executor::SeqScan(SeqScanExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                table_iterator: TableHeap::new(
                    plan.first_page_id,
                    self.context.buffer_pool_manager.clone(),
                    self.context.transaction_manager.clone(),
                    self.context.lock_manager.clone(),
                    self.context.log_manager.clone(),
                    self.context.transaction_id,
                )
                .iter(),
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
                let children = plan
                    .children
                    .iter()
                    .map(|child| Box::new(self.create_executor(child)))
                    .collect::<Vec<_>>();
                Executor::NestedLoopJoin(NestedLoopJoinExecutor {
                    plan: plan.clone(),
                    children,
                    tuples: vec![],
                    executor_context: &self.context,
                    matched_statuses: vec![false; plan.children.len() - 1],
                    in_guard_statuses: vec![false; plan.children.len() - 1],
                })
            }
            Plan::Aggregate(plan) => {
                let mut aggregate_tables = vec![];
                for _ in 0..plan.aggregate_functions.len() {
                    aggregate_tables.push(AggregateTable::new());
                }
                Executor::Aggregate(AggregateExecutor {
                    plan: plan.clone(),
                    child: Box::new(self.create_executor(&plan.child)),
                    executor_context: &self.context,
                    aggregate_table_value: if plan.group_by.len() == 0 {
                        AggregateTableValue::Value(vec![vec![]; plan.aggregate_functions.len()])
                    } else {
                        AggregateTableValue::Table(AggregateTable::new())
                    },
                    result: vec![],
                    index: 0,
                })
            }
            Plan::Sort(plan) => Executor::Sort(SortExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                result: vec![],
                cursor: 0,
            }),
            Plan::Limit(plan) => Executor::Limit(limit_executor::LimitExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                result: vec![],
                cursor: 0,
            }),
            Plan::EmptyRow(plan) => Executor::EmptyRow(EmptyRowExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                returned: false,
            }),
            Plan::Insert(plan) => Executor::Insert(InsertExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                table_heap: TableHeap::new(
                    plan.first_page_id,
                    self.context.buffer_pool_manager.clone(),
                    self.context.transaction_manager.clone(),
                    self.context.lock_manager.clone(),
                    self.context.log_manager.clone(),
                    self.context.transaction_id,
                ),
                count: 0,
                executed: false,
            }),
            Plan::Delete(plan) => Executor::Delete(DeleteExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                table_heap: TableHeap::new(
                    plan.first_page_id,
                    self.context.buffer_pool_manager.clone(),
                    self.context.transaction_manager.clone(),
                    self.context.lock_manager.clone(),
                    self.context.log_manager.clone(),
                    self.context.transaction_id,
                ),
                count: 0,
                executed: false,
            }),
            Plan::Update(plan) => Executor::Update(UpdateExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                table_heap: TableHeap::new(
                    plan.first_page_id,
                    self.context.buffer_pool_manager.clone(),
                    self.context.transaction_manager.clone(),
                    self.context.lock_manager.clone(),
                    self.context.log_manager.clone(),
                    self.context.transaction_id,
                ),
                count: 0,
                executed: false,
            }),
        }
    }
}

pub enum Executor<'a> {
    SeqScan(SeqScanExecutor<'a>),
    Filter(FilterExecutor<'a>),
    Project(ProjectExecutor<'a>),
    NestedLoopJoin(NestedLoopJoinExecutor<'a>),
    Aggregate(AggregateExecutor<'a>),
    Sort(SortExecutor<'a>),
    Limit(LimitExecutor<'a>),
    EmptyRow(EmptyRowExecutor<'a>),
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
            Executor::Aggregate(executor) => executor.init(),
            Executor::Sort(executor) => executor.init(),
            Executor::Limit(executor) => executor.init(),
            Executor::EmptyRow(executor) => executor.init(),
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
            Executor::Aggregate(executor) => executor.next(),
            Executor::Sort(executor) => executor.next(),
            Executor::Limit(executor) => executor.next(),
            Executor::EmptyRow(executor) => executor.next(),
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
        value::{boolean::BooleanValue, integer::IntegerValue, varchar::VarcharValue, Value},
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
        let sql =
            "SELECT __c1 FROM (SELECT _c1 AS __c1 FROM (SELECT c1 AS _c1 FROM t1) AS sub2) AS sub1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(IntegerValue(3))],
                vec![Value::Integer(IntegerValue(1))],
            ]
        );

        Ok(())
    }

    #[test]
    fn test_join() -> Result<()> {
        let instance = setup_test_database()?;
        let txn_id = instance.begin(None)?;

        let sql = "INSERT INTO t1 VALUES (1, 'foo')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t1 VALUES (2, 'bar')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t1 VALUES (3, 'baz')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (1, 1, 'hoge')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (1, 2, 'fuga')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (3, 3, 'piyo')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t3 VALUES (3, 1, 'aaaa')";
        execute(sql, &instance, txn_id)?;

        let sql = "SELECT * FROM t1 INNER JOIN t2 ON t1.c1 = t2.t1_c1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("hoge".to_string())),
                ],
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("fuga".to_string())),
                ],
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("baz".to_string())),
                    Value::Integer(IntegerValue(3)),
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("piyo".to_string())),
                ],
            ]
        );

        let sql =
            "SELECT * FROM t1 INNER JOIN t2 ON t1.c1 = t2.t1_c1 INNER JOIN t3 ON t2.c1 = t3.t2_c1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![vec![
                Value::Integer(IntegerValue(3)),
                Value::Varchar(VarcharValue("baz".to_string())),
                Value::Integer(IntegerValue(3)),
                Value::Integer(IntegerValue(3)),
                Value::Varchar(VarcharValue("piyo".to_string())),
                Value::Integer(IntegerValue(3)),
                Value::Integer(IntegerValue(1)),
                Value::Varchar(VarcharValue("aaaa".to_string())),
            ],]
        );

        let sql = "SELECT * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.t1_c1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("hoge".to_string())),
                ],
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("fuga".to_string())),
                ],
                vec![
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("baz".to_string())),
                    Value::Integer(IntegerValue(3)),
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("piyo".to_string())),
                ],
            ]
        );

        let sql =
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.t1_c1 LEFT JOIN t3 ON t2.c1 = t3.t2_c1";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("hoge".to_string())),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                    Value::Integer(IntegerValue(1)),
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("fuga".to_string())),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("baz".to_string())),
                    Value::Integer(IntegerValue(3)),
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("piyo".to_string())),
                    Value::Integer(IntegerValue(3)),
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("aaaa".to_string())),
                ],
            ]
        );

        Ok(())
    }

    #[test]
    fn test_aggregate() -> Result<()> {
        let instance = setup_test_database()?;
        let txn_id = instance.begin(None)?;

        let sql = "INSERT INTO t2 VALUES (1, 1, 'hoge')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (1, 2, 'hoge')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (1, 3, 'fuga')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (1, 4, 'hoge')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (3, 5, 'piyo')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (3, 6, 'hoge')";
        execute(sql, &instance, txn_id)?;
        let sql = "INSERT INTO t2 VALUES (3, 6, 'piyo')";
        execute(sql, &instance, txn_id)?;

        let sql = "SELECT t1_c1, c2, COUNT(1) FROM t2 GROUP BY t1_c1, c2 HAVING COUNT(1) = 2";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![vec![
                Value::Integer(IntegerValue(3)),
                Value::Varchar(VarcharValue("piyo".to_string())),
                Value::Integer(IntegerValue(2)),
            ]]
        );
        Ok(())
    }

    #[test]
    fn test_without_from() -> Result<()> {
        let instance = setup_test_database()?;
        let txn_id = instance.begin(None)?;

        let sql = "SELECT 1 + 2";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(rows, vec![vec![Value::Integer(IntegerValue(3))]]);

        let sql = "SELECT 1 + 2, 3 + 4";
        let (rows, _) = execute(sql, &instance, txn_id)?;
        assert_eq!(
            rows,
            vec![vec![
                Value::Integer(IntegerValue(3)),
                Value::Integer(IntegerValue(7)),
            ]]
        );

        Ok(())
    }
}

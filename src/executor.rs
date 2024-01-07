use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    common::TransactionID,
    concurrency::TransactionManager,
    lock::LockManager,
    plan::{Plan, UpdatePlan},
    tuple::Tuple,
    value::Value,
};

use self::{
    delete_executor::DeleteExecutor, filter_executor::FilterExecutor,
    insert_executor::InsertExecutor, project_executor::ProjectExecutor,
    seq_scan_executor::SeqScanExecutor,
};

mod delete_executor;
mod filter_executor;
mod insert_executor;
mod project_executor;
mod seq_scan_executor;

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
            Plan::Insert(plan) => Executor::Insert(InsertExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
            }),
            Plan::Delete(plan) => Executor::Delete(DeleteExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
                table_heap: None,
            }),
            Plan::Update(plan) => Executor::Update(UpdateExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor(&plan.child)),
                executor_context: &self.context,
            }),
        }
    }
}

pub enum Executor<'a> {
    SeqScan(SeqScanExecutor<'a>),
    Filter(FilterExecutor<'a>),
    Project(ProjectExecutor<'a>),
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
            Executor::Insert(executor) => executor.next(),
            Executor::Delete(executor) => executor.next(),
            Executor::Update(executor) => executor.next(),
        }
    }
}

pub struct UpdateExecutor<'a> {
    pub plan: UpdatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}

impl UpdateExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        unimplemented!()
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, RwLock};

    use anyhow::anyhow;
    use tempfile::tempdir;

    use crate::{
        buffer::BufferPoolManager,
        catalog::{Catalog, Column, DataType, Schema},
        concurrency::{IsolationLevel, TransactionManager},
        disk::DiskManager,
        executor::{ExecutorContext, ExecutorEngine},
        lock::LockManager,
        plan::{
            BinaryExpression, DeletePlan, Expression, FilterPlan, InsertPlan, LiteralExpression,
            PathExpression, Plan, ProjectPlan, SeqScanPlan,
        },
        value::{IntValue, Value, VarcharValue},
    };

    #[test]
    fn test_executor() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, 10)));
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
            lock_manager.clone(),
            IsolationLevel::RepeatableRead,
        )));
        let catalog = Arc::new(Mutex::new(Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
        )));

        // init
        catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .bootstrap(true)?;

        // create_table and insert
        {
            let txn_id = transaction_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .begin();
            catalog
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .create_table(
                    "test",
                    &Schema {
                        columns: vec![
                            Column {
                                name: "id".to_string(),
                                data_type: DataType::Int,
                            },
                            Column {
                                name: "name".to_string(),
                                data_type: DataType::Varchar,
                            },
                            Column {
                                name: "age".to_string(),
                                data_type: DataType::Int,
                            },
                        ],
                    },
                    txn_id,
                )?;
            let executor_context = ExecutorContext {
                transaction_id: txn_id,
                buffer_pool_manager: buffer_pool_manager.clone(),
                lock_manager: lock_manager.clone(),
                transaction_manager: transaction_manager.clone(),
                catalog: catalog.clone(),
            };
            let plan = Plan::Insert(InsertPlan {
                table_name: "test".to_string(),
                values: vec![
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(1)),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Varchar(VarcharValue("name1".to_string())),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(10)),
                    }),
                ],
                schema: Schema { columns: vec![] },
            });
            let mut executor = ExecutorEngine::new(plan, executor_context);
            executor.execute()?;
            let executor_context = ExecutorContext {
                transaction_id: txn_id,
                buffer_pool_manager: buffer_pool_manager.clone(),
                lock_manager: lock_manager.clone(),
                transaction_manager: transaction_manager.clone(),
                catalog: catalog.clone(),
            };
            let plan = Plan::Insert(InsertPlan {
                table_name: "test".to_string(),
                values: vec![
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(2)),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Varchar(VarcharValue("name2".to_string())),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(20)),
                    }),
                ],
                schema: Schema { columns: vec![] },
            });
            let mut executor = ExecutorEngine::new(plan, executor_context);
            executor.execute()?;
            let executor_context = ExecutorContext {
                transaction_id: txn_id,
                buffer_pool_manager: buffer_pool_manager.clone(),
                lock_manager: lock_manager.clone(),
                transaction_manager: transaction_manager.clone(),
                catalog: catalog.clone(),
            };
            let plan = Plan::Insert(InsertPlan {
                table_name: "test".to_string(),
                values: vec![
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(3)),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Varchar(VarcharValue("name3".to_string())),
                    }),
                    crate::plan::Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(30)),
                    }),
                ],
                schema: Schema { columns: vec![] },
            });
            let mut executor = ExecutorEngine::new(plan, executor_context);
            executor.execute()?;
            let schema = catalog
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .get_schema_by_table_name("test", txn_id)?;
            let executor_context = ExecutorContext {
                transaction_id: txn_id,
                buffer_pool_manager: buffer_pool_manager.clone(),
                lock_manager: lock_manager.clone(),
                transaction_manager: transaction_manager.clone(),
                catalog: catalog.clone(),
            };
            let plan = Plan::Delete(DeletePlan {
                table_name: "test".to_string(),
                schema: Schema { columns: vec![] },
                child: Box::new(Plan::Filter(FilterPlan {
                    predicate: Expression::Binary(BinaryExpression {
                        operator: crate::plan::BinaryOperator::Equal,
                        left: Box::new(Expression::Path(PathExpression {
                            column_name: "id".to_string(),
                        })),
                        right: Box::new(Expression::Literal(crate::plan::LiteralExpression {
                            value: Value::Int(IntValue(1)),
                        })),
                    }),
                    schema: schema.clone(),
                    child: Box::new(Plan::SeqScan(SeqScanPlan {
                        table_name: "test".to_string(),
                        schema: schema.clone(),
                    })),
                })),
            });
            let mut executor = ExecutorEngine::new(plan, executor_context);
            executor.execute()?;
            transaction_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .commit(txn_id)?;
        }

        // select
        let txn_id = transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        let executor_context = ExecutorContext {
            transaction_id: txn_id,
            buffer_pool_manager: buffer_pool_manager.clone(),
            lock_manager: lock_manager.clone(),
            transaction_manager: transaction_manager.clone(),
            catalog: catalog.clone(),
        };
        let schema = catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_schema_by_table_name("test", txn_id)?;
        let plan = Plan::Project(ProjectPlan {
            select_elements: vec![
                crate::plan::SelectElement {
                    expression: Expression::Path(PathExpression {
                        column_name: "name".to_string(),
                    }),
                    alias: None,
                },
                crate::plan::SelectElement {
                    expression: Expression::Literal(LiteralExpression {
                        value: Value::Int(IntValue(9999)),
                    }),
                    alias: Some("literal_value".to_string()),
                },
            ],
            schema: Schema {
                columns: vec![
                    Column {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                    },
                    Column {
                        name: "literal_value".to_string(),
                        data_type: DataType::Int,
                    },
                ],
            },
            child: Box::new(Plan::Filter(FilterPlan {
                predicate: Expression::Binary(BinaryExpression {
                    operator: crate::plan::BinaryOperator::Equal,
                    left: Box::new(Expression::Path(PathExpression {
                        column_name: "age".to_string(),
                    })),
                    right: Box::new(Expression::Literal(crate::plan::LiteralExpression {
                        value: Value::Int(IntValue(20)),
                    })),
                }),
                schema: schema.clone(),
                child: Box::new(Plan::SeqScan(SeqScanPlan {
                    table_name: "test".to_string(),
                    schema: schema.clone(),
                })),
            })),
        });
        let mut executor = ExecutorEngine::new(plan, executor_context);
        let tuples = executor.execute()?;
        assert_eq!(
            tuples,
            vec![vec![
                Value::Varchar(VarcharValue("name2".to_string())),
                Value::Int(IntValue(9999)),
            ]]
        );
        let executor_context = ExecutorContext {
            transaction_id: txn_id,
            buffer_pool_manager: buffer_pool_manager.clone(),
            lock_manager: lock_manager.clone(),
            transaction_manager: transaction_manager.clone(),
            catalog: catalog.clone(),
        };
        let plan = Plan::SeqScan(SeqScanPlan {
            table_name: "test".to_string(),
            schema: schema.clone(),
        });
        let mut executor = ExecutorEngine::new(plan, executor_context);
        let tuples = executor.execute()?;
        assert_eq!(tuples.len(), 2);

        Ok(())
    }
}

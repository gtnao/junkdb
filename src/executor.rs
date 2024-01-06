use std::sync::{Arc, Mutex, RwLock};

use anyhow::{anyhow, Result};

use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    common::TransactionID,
    concurrency::TransactionManager,
    lock::LockManager,
    plan::{DeletePlan, FilterPlan, InsertPlan, Plan, ProjectPlan, SeqScanPlan, UpdatePlan},
    table::{TableHeap, TableIterator},
    tuple::Tuple,
    value::Value,
};

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
        let mut executor = self.create_executor();
        executor.init()?;
        let mut tuple = executor.next()?;
        let mut result = vec![];
        while let Some(t) = &tuple {
            result.push(t.values(&self.plan.schema()));
            tuple = executor.next()?;
        }
        Ok(result)
    }
    fn create_executor(&self) -> Executor {
        match &self.plan {
            Plan::SeqScan(plan) => Executor::SeqScan(SeqScanExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
                table_iterator: None,
            }),
            Plan::Filter(plan) => Executor::Filter(FilterExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor()),
                executor_context: &self.context,
            }),
            Plan::Project(plan) => Executor::Project(ProjectExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor()),
                executor_context: &self.context,
            }),
            Plan::Insert(plan) => Executor::Insert(InsertExecutor {
                plan: plan.clone(),
                executor_context: &self.context,
            }),
            Plan::Delete(plan) => Executor::Delete(DeleteExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor()),
                executor_context: &self.context,
            }),
            Plan::Update(plan) => Executor::Update(UpdateExecutor {
                plan: plan.clone(),
                child: Box::new(self.create_executor()),
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

pub struct SeqScanExecutor<'a> {
    pub plan: SeqScanPlan,
    pub executor_context: &'a ExecutorContext,
    pub table_iterator: Option<TableIterator>,
}
pub struct FilterExecutor<'a> {
    pub plan: FilterPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}
pub struct ProjectExecutor<'a> {
    pub plan: ProjectPlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}
pub struct InsertExecutor<'a> {
    pub plan: InsertPlan,
    pub executor_context: &'a ExecutorContext,
}
pub struct DeleteExecutor<'a> {
    pub plan: DeletePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}
pub struct UpdateExecutor<'a> {
    pub plan: UpdatePlan,
    pub child: Box<Executor<'a>>,
    pub executor_context: &'a ExecutorContext,
}

impl SeqScanExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        let txn_id = self.executor_context.transaction_id;
        let first_page_id = self
            .executor_context
            .catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_first_page_id_by_table_name(&self.plan.table_name, txn_id)?;
        let table_heap = TableHeap::new(
            first_page_id,
            self.executor_context.buffer_pool_manager.clone(),
            self.executor_context.transaction_manager.clone(),
            self.executor_context.lock_manager.clone(),
            txn_id,
        );
        self.table_iterator = Some(table_heap.iter());
        Ok(())
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        let table_iterator = self.table_iterator.as_mut().ok_or_else(|| {
            anyhow!("table_iterator is not initialized. call init() before calling next()")
        })?;
        Ok(table_iterator.next())
    }
}
impl FilterExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        unimplemented!()
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        unimplemented!()
    }
}
impl ProjectExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        unimplemented!()
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        unimplemented!()
    }
}
impl InsertExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        unimplemented!()
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        unimplemented!()
    }
}
impl DeleteExecutor<'_> {
    pub fn init(&mut self) -> Result<()> {
        unimplemented!()
    }
    pub fn next(&mut self) -> Result<Option<Tuple>> {
        unimplemented!()
    }
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
        common::TransactionID,
        concurrency::{IsolationLevel, TransactionManager},
        disk::DiskManager,
        executor::{ExecutorContext, ExecutorEngine},
        lock::LockManager,
        plan::{Plan, SeqScanPlan},
        table::TableHeap,
        tuple::Tuple,
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
        let first_page_id = catalog
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .get_first_page_id_by_table_name("test", txn_id)?;
        let mut table_heap = TableHeap::new(
            first_page_id,
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
            txn_id,
        );
        let values = vec![
            Value::Int(IntValue(1)),
            Value::Varchar(VarcharValue("name1".to_string())),
            Value::Int(IntValue(10)),
        ];
        table_heap.insert(&values)?;
        transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;

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
        let plan = Plan::SeqScan(SeqScanPlan {
            table_name: "test".to_string(),
            schema,
        });
        let mut executor = ExecutorEngine::new(plan, executor_context);
        let tuples = executor.execute()?;
        assert_eq!(
            tuples,
            vec![vec![
                Value::Int(IntValue(1)),
                Value::Varchar(VarcharValue("name1".to_string())),
                Value::Int(IntValue(10))
            ]]
        );

        Ok(())
    }
}

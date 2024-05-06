use std::{
    fs,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;

use crate::{
    binder::Binder,
    buffer::BufferPoolManager,
    catalog::{Catalog, Column, Schema},
    common::TransactionID,
    concurrency::{IsolationLevel, TransactionManager},
    disk::DiskManager,
    executor::{ExecutorContext, ExecutorEngine},
    lock::LockManager,
    log::LogManager,
    optimizer::Optimizer,
    parser::{CreateIndexStatementAST, CreateTableStatementAST, StatementAST},
    plan::Planner,
    recovery::RecoveryManager,
    value::Value,
};

pub struct Instance {
    pub buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    pub catalog: Arc<Mutex<Catalog>>,
    pub transaction_manager: Arc<Mutex<TransactionManager>>,
    pub lock_manager: Arc<RwLock<LockManager>>,
    pub log_manager: Arc<Mutex<LogManager>>,
}

impl Instance {
    pub fn new(dir: &str, init: bool, recover: bool) -> Result<Self> {
        if init {
            if fs::metadata(dir).is_ok() && fs::metadata(dir)?.is_dir() {
                fs::remove_dir_all(dir)?;
            }
            fs::create_dir_all(dir)?;
        }

        let data_file = format!("{}/data.db", dir);
        let txn_log_file = format!("{}/txn.log", dir);
        let wal_log_file = format!("{}/wal.log", dir);

        let disk_manager = DiskManager::new(&data_file)?;
        let log_manager = Arc::new(Mutex::new(LogManager::new(&wal_log_file)?));
        let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(
            disk_manager,
            log_manager.clone(),
            32,
        )));
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
            lock_manager.clone(),
            log_manager.clone(),
            &txn_log_file,
            IsolationLevel::RepeatableRead,
        )?));
        let mut catalog = Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
            log_manager.clone(),
        );
        catalog.bootstrap(init)?;
        let catalog = Arc::new(Mutex::new(catalog));

        if recover {
            let log_records = log_manager
                .lock()
                .map_err(|e| anyhow::anyhow!("{}", e))?
                .read()?;
            let mut recovery_manager =
                RecoveryManager::new(buffer_pool_manager.clone(), log_records);
            recovery_manager.recover()?;
        }

        Ok(Self {
            buffer_pool_manager,
            catalog,
            transaction_manager,
            lock_manager,
            log_manager,
        })
    }

    // DDL
    pub fn create_table(
        &self,
        statement: &CreateTableStatementAST,
        txn_id: TransactionID,
    ) -> Result<()> {
        let schema = Schema {
            columns: statement
                .elements
                .iter()
                .map(|e| Column {
                    name: e.column_name.clone(),
                    data_type: e.data_type.clone(),
                })
                .collect(),
        };
        self.catalog
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .create_table(&statement.table_name, &schema, txn_id)
    }
    pub fn create_index(
        &self,
        statement: &CreateIndexStatementAST,
        txn_id: TransactionID,
    ) -> Result<()> {
        self.catalog
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .create_index(
                &statement.index_name,
                &statement.table_name,
                &statement.column_names,
                txn_id,
            )
    }

    // DDL
    pub fn execute(
        &self,
        statement: &StatementAST,
        txn_id: TransactionID,
    ) -> Result<(Vec<Vec<Value>>, Schema)> {
        let mut binder = Binder::new(self.catalog.clone(), txn_id);
        let bound_statement = binder.bind_statement(statement)?;
        let planner = Planner::new(bound_statement);
        let plan = planner.plan();
        let optimizer = Optimizer::new(self.catalog.clone(), txn_id);
        let plan = optimizer.optimize(plan)?;
        let schema = plan.schema().clone();
        let executor_context = ExecutorContext {
            transaction_id: txn_id,
            buffer_pool_manager: self.buffer_pool_manager.clone(),
            lock_manager: self.lock_manager.clone(),
            transaction_manager: self.transaction_manager.clone(),
            log_manager: self.log_manager.clone(),
            catalog: self.catalog.clone(),
        };
        let mut executor_engine = ExecutorEngine::new(plan, executor_context);
        let rows = executor_engine.execute()?;
        Ok((rows, schema))
    }

    // DCL
    pub fn begin(&self, txn_id: Option<TransactionID>) -> Result<TransactionID> {
        if let Some(txn_id) = txn_id {
            return Ok(txn_id);
        }
        self.transaction_manager
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .begin()
    }
    pub fn commit(&self, txn_id: TransactionID) -> Result<()> {
        self.transaction_manager
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .commit(txn_id)
    }
    pub fn rollback(&self, txn_id: TransactionID) -> Result<()> {
        self.transaction_manager
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .abort(txn_id)
    }

    pub fn shutdown(&self) -> Result<()> {
        self.buffer_pool_manager
            .lock()
            .map_err(|e| anyhow::anyhow!("{}", e))?
            .shutdown()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_new_init() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().join("test");
        Instance::new(dir.to_str().unwrap(), true, false)?;
        assert!(dir.exists());
        assert!(dir.join("data.db").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }

    #[test]
    fn test_new_init_exists() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().join("test");
        let instance = Instance::new(dir.to_str().unwrap(), true, false)?;

        let created_at = fs::metadata(&dir)?.created()?;
        thread::sleep(std::time::Duration::from_secs(1));

        // check dir was recreated
        instance.shutdown()?;
        Instance::new(dir.to_str().unwrap(), true, false)?;
        assert!(created_at < fs::metadata(&dir)?.created()?);
        assert!(dir.join("data.db").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }

    #[test]
    fn test_new_not_init() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().join("test");
        let instance = Instance::new(dir.to_str().unwrap(), true, false)?;
        let created_at = fs::metadata(&dir)?.created()?;
        thread::sleep(std::time::Duration::from_secs(1));

        // check dir was not recreated
        instance.shutdown()?;
        Instance::new(dir.to_str().unwrap(), false, false)?;
        assert_eq!(created_at, fs::metadata(&dir)?.created()?);
        assert!(dir.join("data.db").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }
}

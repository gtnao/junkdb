use std::{
    fs,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    catalog::Catalog,
    concurrency::{IsolationLevel, TransactionManager},
    disk::DiskManager,
    lock::LockManager,
};

pub struct Instance {
    pub buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    pub catalog: Arc<Mutex<Catalog>>,
    pub transaction_manager: Arc<Mutex<TransactionManager>>,
    pub lock_manager: Arc<RwLock<LockManager>>,
}

impl Instance {
    pub fn new(dir: &str, init: bool) -> Result<Self> {
        if init {
            if fs::metadata(dir).is_ok() && fs::metadata(dir)?.is_dir() {
                fs::remove_dir_all(dir)?;
            }
            fs::create_dir_all(dir)?;
        }

        let data_file = format!("{}/data", dir);
        let txn_log_file = format!("{}/txn.log", dir);

        let disk_manager = DiskManager::new(&data_file)?;
        let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, 32)));
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
            lock_manager.clone(),
            &txn_log_file,
            IsolationLevel::RepeatableRead,
        )?));
        let mut catalog = Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
        );
        catalog.bootstrap(init)?;
        let catalog = Arc::new(Mutex::new(catalog));

        Ok(Self {
            buffer_pool_manager,
            catalog,
            transaction_manager,
            lock_manager,
        })
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
        Instance::new(dir.to_str().unwrap(), true)?;
        assert!(dir.exists());
        assert!(dir.join("data").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }

    #[test]
    fn test_new_init_exists() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().join("test");
        let instance = Instance::new(dir.to_str().unwrap(), true)?;

        let created_at = fs::metadata(&dir)?.created()?;
        thread::sleep(std::time::Duration::from_secs(1));

        // check dir was recreated
        instance.shutdown()?;
        Instance::new(dir.to_str().unwrap(), true)?;
        assert!(created_at < fs::metadata(&dir)?.created()?);
        assert!(dir.join("data").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }

    #[test]
    fn test_new_not_init() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().join("test");
        let instance = Instance::new(dir.to_str().unwrap(), true)?;
        let created_at = fs::metadata(&dir)?.created()?;
        thread::sleep(std::time::Duration::from_secs(1));

        // check dir was not recreated
        instance.shutdown()?;
        Instance::new(dir.to_str().unwrap(), false)?;
        assert_eq!(created_at, fs::metadata(&dir)?.created()?);
        assert!(dir.join("data").exists());
        assert!(dir.join("txn.log").exists());
        Ok(())
    }
}

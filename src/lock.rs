use std::{
    collections::HashMap,
    sync::{Condvar, Mutex, RwLock},
};

use anyhow::{anyhow, Result};

use crate::common::{TransactionID, RID};

pub struct LockRequest {
    pub txn_id: Mutex<Option<TransactionID>>,
    pub condvar: Condvar,
}
impl LockRequest {
    pub fn new(txn_id: TransactionID) -> Self {
        Self {
            txn_id: Mutex::new(Some(txn_id)),
            condvar: Condvar::new(),
        }
    }
}

pub struct LockManager {
    lock_requests_by_rid: RwLock<HashMap<RID, LockRequest>>,
    rids_by_txn_id: RwLock<HashMap<TransactionID, Vec<RID>>>,
}

impl<'a> Default for LockManager {
    fn default() -> Self {
        Self {
            lock_requests_by_rid: RwLock::new(HashMap::new()),
            rids_by_txn_id: RwLock::new(HashMap::new()),
        }
    }
}

impl LockManager {
    pub fn lock(&self, rid: RID, txn_id: TransactionID) -> Result<()> {
        {
            let requests = self
                .lock_requests_by_rid
                .read()
                .map_err(|_| anyhow!("lock error"))?;
            if let Some(request) = requests.get(&rid) {
                let mut locked_txn_id = request.txn_id.lock().map_err(|_| anyhow!("lock error"))?;
                while locked_txn_id.is_some() {
                    locked_txn_id = request
                        .condvar
                        .wait(locked_txn_id)
                        .map_err(|_| anyhow!("lock error"))?;
                }
                locked_txn_id.replace(txn_id);
                self.rids_by_txn_id
                    .write()
                    .map_err(|_| anyhow!("lock error"))?
                    .entry(txn_id)
                    .or_insert_with(Vec::new)
                    .push(rid);
                return Ok(());
            }
        }

        let mut requests = self
            .lock_requests_by_rid
            .write()
            .map_err(|_| anyhow!("lock error"))?;
        let request = LockRequest::new(txn_id);
        requests.insert(rid, request);
        self.rids_by_txn_id
            .write()
            .map_err(|_| anyhow!("lock error"))?
            .entry(txn_id)
            .or_insert_with(Vec::new)
            .push(rid);
        Ok(())
    }
    pub fn unlock(&self, transaction_id: TransactionID) -> Result<()> {
        let mut rids_map = self
            .rids_by_txn_id
            .write()
            .map_err(|_| anyhow!("lock error"))?;
        for rid in rids_map.get(&transaction_id).unwrap_or(&vec![]).iter() {
            let requests = self
                .lock_requests_by_rid
                .read()
                .map_err(|_| anyhow!("lock error"))?;
            if let Some(request) = requests.get(&rid) {
                let mut locked_txn_id = request.txn_id.lock().map_err(|_| anyhow!("lock error"))?;
                locked_txn_id.take();
                request.condvar.notify_all();
            }
        }
        rids_map.remove(&transaction_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;
    use crate::common::{PageID, TransactionID};

    // this is not a test
    #[test]
    fn test_lock_() -> Result<()> {
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let lock_manager_clone = lock_manager.clone();
        thread::spawn(move || -> Result<()> {
            let txn_id = TransactionID(1);
            let rid = RID(PageID(1), 1);
            lock_manager_clone
                .read()
                .map_err(|_| anyhow!("lock error"))?
                .lock(rid, txn_id)?;
            // println!("locked txn_id: {:?}, rid: {:?}", txn_id, rid);
            thread::sleep(std::time::Duration::from_millis(500));
            lock_manager_clone
                .read()
                .map_err(|_| anyhow!("lock error"))?
                .unlock(txn_id)?;
            // println!("unlocked txn_id: {:?}", txn_id);
            Ok(())
        });
        thread::sleep(std::time::Duration::from_millis(100));
        let mut handles = vec![];
        for i in 2..12 {
            let lock_manager = lock_manager.clone();
            let handle = thread::spawn(move || -> Result<()> {
                // println!("start txn_id: {:?}", i);
                let txn_id = TransactionID(i);
                let rid = RID(PageID(1), 1);
                lock_manager
                    .read()
                    .map_err(|_| anyhow!("lock error"))?
                    .lock(rid, txn_id)?;
                // println!("locked txn_id: {:?}, rid: {:?}", txn_id, rid);
                lock_manager
                    .read()
                    .map_err(|_| anyhow!("lock error"))?
                    .unlock(txn_id)?;
                // println!("unlocked txn_id: {:?}", txn_id);
                Ok(())
            });
            handles.push(handle);
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("thread error"))??;
        }
        Ok(())
    }
}

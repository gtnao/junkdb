use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;

use crate::{
    common::TransactionID,
    lock::LockManager,
    log::{LogManager, LogRecordBody},
};

pub struct Transaction {
    snapshot: Vec<TransactionID>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionStatus {
    Running,
    Aborted,
    Committed,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

pub struct TransactionManager {
    lock_manager: Arc<RwLock<LockManager>>,
    log_manager: TransactionLogManager,
    wal_log_manager: Arc<Mutex<LogManager>>,
    isolation_level: IsolationLevel,
    next_txn_id: TransactionID,
    statuses: HashMap<TransactionID, TransactionStatus>,
    active_transactions: HashMap<TransactionID, Transaction>,
}

impl TransactionManager {
    pub fn new(
        lock_manager: Arc<RwLock<LockManager>>,
        wal_log_manager: Arc<Mutex<LogManager>>,
        log_file_path: &str,
        isolation_level: IsolationLevel,
    ) -> Result<Self> {
        let mut log_manager = TransactionLogManager::new(log_file_path)?;
        let logs = log_manager.read()?;
        let statuses = logs
            .iter()
            .map(|log| (log.txn_id, log.status))
            .collect::<HashMap<_, _>>();
        let next_txn_id = if statuses.is_empty() {
            TransactionID(1)
        } else {
            TransactionID(statuses.keys().max_by_key(|k| k.0).unwrap().0 + 1)
        };
        Ok(TransactionManager {
            lock_manager,
            log_manager,
            wal_log_manager,
            isolation_level,
            next_txn_id,
            statuses,
            active_transactions: HashMap::new(),
        })
    }

    pub fn begin(&mut self) -> Result<TransactionID> {
        let txn_id = self.next_txn_id;
        self.next_txn_id.0 += 1;
        self.statuses.insert(txn_id, TransactionStatus::Running);
        self.active_transactions.insert(
            txn_id,
            Transaction {
                snapshot: self.active_transactions.keys().cloned().collect(),
            },
        );
        self.wal_log_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))
            .unwrap()
            .append(txn_id, LogRecordBody::BeginTransaction)?;
        Ok(txn_id)
    }

    pub fn commit(&mut self, txn_id: TransactionID) -> Result<()> {
        self.lock_manager
            .write()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .unlock(txn_id)?;
        self.log_manager
            .write(TransactionLog::new(txn_id, TransactionStatus::Committed))?;
        self.statuses.insert(txn_id, TransactionStatus::Committed);
        self.active_transactions.remove(&txn_id);
        self.wal_log_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))
            .unwrap()
            .append(txn_id, LogRecordBody::CommitTransaction)?;
        self.wal_log_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))
            .unwrap()
            .flush()?;
        Ok(())
    }

    pub fn abort(&mut self, txn_id: TransactionID) -> Result<()> {
        self.lock_manager
            .write()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .unlock(txn_id)?;
        self.log_manager
            .write(TransactionLog::new(txn_id, TransactionStatus::Aborted))?;
        self.statuses.insert(txn_id, TransactionStatus::Aborted);
        self.active_transactions.remove(&txn_id);
        self.wal_log_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))
            .unwrap()
            .append(txn_id, LogRecordBody::AbortTransaction)?;
        self.wal_log_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))
            .unwrap()
            .flush()?;
        Ok(())
    }

    pub fn is_visible(
        &self,
        txn_id: TransactionID,
        x_min: TransactionID,
        x_max: TransactionID,
    ) -> bool {
        match self.isolation_level {
            IsolationLevel::ReadCommitted => {
                self.is_visible_with_read_committed(txn_id, x_min, x_max)
            }
            IsolationLevel::RepeatableRead => {
                self.is_visible_with_repeatable_read(txn_id, x_min, x_max)
            }
        }
    }

    fn is_visible_with_read_committed(
        &self,
        txn_id: TransactionID,
        x_min: TransactionID,
        x_max: TransactionID,
    ) -> bool {
        if txn_id == x_min {
            if txn_id == x_max {
                return false;
            } else {
                return true;
            }
        }
        let x_min_committed = match self.statuses.get(&x_min) {
            Some(TransactionStatus::Committed) => true,
            _ => false,
        };
        let x_max_committed = match self.statuses.get(&x_max) {
            Some(TransactionStatus::Committed) => true,
            _ => false,
        };
        if x_min_committed {
            if x_max_committed {
                return false;
            } else {
                return true;
            }
        }
        false
    }

    fn is_visible_with_repeatable_read(
        &self,
        txn_id: TransactionID,
        x_min: TransactionID,
        x_max: TransactionID,
    ) -> bool {
        let transaction = if let Some(transaction) = self.active_transactions.get(&txn_id) {
            transaction
        } else {
            println!("Transaction not found: {:?}", txn_id);
            return false;
        };
        let x_min_visible =
            self.is_valid_txn_id_with_snapshot(txn_id, x_min, &transaction.snapshot);
        let x_max_visible =
            self.is_valid_txn_id_with_snapshot(txn_id, x_max, &transaction.snapshot);

        if x_min_visible {
            if x_max_visible {
                return false;
            } else {
                return true;
            }
        }
        false
    }

    fn is_valid_txn_id_with_snapshot(
        &self,
        txn_id: TransactionID,
        target_txn_id: TransactionID,
        snapshot: &[TransactionID],
    ) -> bool {
        if target_txn_id > txn_id {
            return false;
        }
        if snapshot.contains(&target_txn_id) {
            return false;
        }
        let target_txn_status = match self.statuses.get(&target_txn_id) {
            Some(status) => status,
            None => {
                return false;
            }
        };
        if target_txn_status == &TransactionStatus::Aborted {
            return false;
        }
        true
    }
}

struct TransactionLog {
    txn_id: TransactionID,
    status: TransactionStatus,
}
impl TransactionLog {
    pub fn new(txn_id: TransactionID, status: TransactionStatus) -> Self {
        Self { txn_id, status }
    }
    pub fn serialize(&self) -> Box<[u8]> {
        let mut buffer = self.txn_id.0.to_le_bytes().to_vec();
        match self.status {
            TransactionStatus::Committed => buffer.push(0),
            TransactionStatus::Aborted => buffer.push(1),
            _ => unreachable!(),
        }
        buffer.into()
    }
}
impl From<&[u8]> for TransactionLog {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() == 5);
        let mut txn_id_buffer = [0u8; 4];
        txn_id_buffer.copy_from_slice(&bytes[0..4]);
        let txn_id = TransactionID(u32::from_le_bytes(txn_id_buffer));
        let status = match bytes[4] {
            0 => TransactionStatus::Committed,
            1 => TransactionStatus::Aborted,
            _ => unreachable!(),
        };
        TransactionLog { txn_id, status }
    }
}

struct TransactionLogManager {
    log_file: File,
}
impl TransactionLogManager {
    pub fn new(log_file_path: &str) -> Result<Self> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_path)?;
        Ok(Self { log_file })
    }
    pub fn read(&mut self) -> Result<Vec<TransactionLog>> {
        let mut buffer = vec![];
        self.log_file.read_to_end(&mut buffer)?;
        let mut logs = vec![];
        let mut offset = 0;
        while offset < buffer.len() {
            let log = TransactionLog::from(&buffer[offset..(offset + 5)]);
            logs.push(log);
            offset += 5;
        }
        Ok(logs)
    }
    pub fn write(&mut self, txn_log: TransactionLog) -> Result<()> {
        let buffer = txn_log.serialize();
        self.log_file.write_all(&buffer)?;
        self.log_file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::INVALID_TRANSACTION_ID;

    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_transaction_manager_begin() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("transaction.log");
        let wal_log_file_path = dir.path().join("wal.log");
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let wal_log_manager = Arc::new(Mutex::new(LogManager::new(
            wal_log_file_path.to_str().unwrap(),
        )?));
        let mut transaction_manager = TransactionManager::new(
            lock_manager,
            wal_log_manager,
            log_file_path.to_str().unwrap(),
            IsolationLevel::ReadCommitted,
        )?;
        let txn_id = transaction_manager.begin()?;
        assert_eq!(txn_id, TransactionID(1));
        assert_eq!(
            transaction_manager.statuses.get(&txn_id),
            Some(&TransactionStatus::Running)
        );
        assert_eq!(
            transaction_manager
                .active_transactions
                .get(&txn_id)
                .is_some(),
            true,
        );
        Ok(())
    }

    #[test]
    fn test_transaction_manager_commit() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("transaction.log");
        let wal_log_file_path = dir.path().join("wal.log");
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let wal_log_manager = Arc::new(Mutex::new(LogManager::new(
            wal_log_file_path.to_str().unwrap(),
        )?));
        let mut transaction_manager = TransactionManager::new(
            lock_manager,
            wal_log_manager,
            log_file_path.to_str().unwrap(),
            IsolationLevel::ReadCommitted,
        )?;
        let txn_id = transaction_manager.begin()?;
        transaction_manager.commit(txn_id)?;
        assert_eq!(
            transaction_manager.statuses.get(&txn_id),
            Some(&TransactionStatus::Committed)
        );
        assert_eq!(
            transaction_manager
                .active_transactions
                .get(&txn_id)
                .is_none(),
            true
        );
        Ok(())
    }

    #[test]
    fn test_transaction_manager_abort() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("transaction.log");
        let wal_log_file_path = dir.path().join("wal.log");
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let wal_log_manager = Arc::new(Mutex::new(LogManager::new(
            wal_log_file_path.to_str().unwrap(),
        )?));
        let mut transaction_manager = TransactionManager::new(
            lock_manager,
            wal_log_manager,
            log_file_path.to_str().unwrap(),
            IsolationLevel::ReadCommitted,
        )?;
        let txn_id = transaction_manager.begin()?;
        transaction_manager.abort(txn_id)?;
        assert_eq!(
            transaction_manager.statuses.get(&txn_id),
            Some(&TransactionStatus::Aborted)
        );
        assert_eq!(
            transaction_manager
                .active_transactions
                .get(&txn_id)
                .is_none(),
            true
        );
        Ok(())
    }

    #[test]
    fn test_transaction_manager_visible_with_read_committed() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("transaction.log");
        let wal_log_file_path = dir.path().join("wal.log");
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let wal_log_manager = Arc::new(Mutex::new(LogManager::new(
            wal_log_file_path.to_str().unwrap(),
        )?));
        let mut transaction_manager = TransactionManager::new(
            lock_manager,
            wal_log_manager,
            log_file_path.to_str().unwrap(),
            IsolationLevel::ReadCommitted,
        )?;

        let txn_id_1 = transaction_manager.begin()?;
        // self insert
        assert_eq!(
            transaction_manager.is_visible(txn_id_1, txn_id_1, INVALID_TRANSACTION_ID),
            true
        );
        // self delete
        assert_eq!(
            transaction_manager.is_visible(txn_id_1, txn_id_1, txn_id_1),
            false
        );

        let txn_id_2 = transaction_manager.begin()?;
        // other insert
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, INVALID_TRANSACTION_ID),
            false
        );
        // other insert after commit
        transaction_manager.commit(txn_id_1)?;
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, INVALID_TRANSACTION_ID),
            true
        );

        let txn_id_3 = transaction_manager.begin()?;
        // other delete
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, txn_id_3),
            true
        );
        // other delete after commit
        transaction_manager.commit(txn_id_3)?;
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, txn_id_3),
            false
        );
        Ok(())
    }

    #[test]
    fn test_transaction_manager_visible_with_repeatable_read() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("transaction.log");
        let wal_log_file_path = dir.path().join("wal.log");
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let wal_log_manager = Arc::new(Mutex::new(LogManager::new(
            wal_log_file_path.to_str().unwrap(),
        )?));
        let mut transaction_manager = TransactionManager::new(
            lock_manager,
            wal_log_manager,
            log_file_path.to_str().unwrap(),
            IsolationLevel::RepeatableRead,
        )?;

        let before_commit_txn_id = transaction_manager.begin()?;
        transaction_manager.commit(before_commit_txn_id)?;
        let running_txn_id = transaction_manager.begin()?;
        let before_abort_txn_id = transaction_manager.begin()?;
        transaction_manager.abort(before_abort_txn_id)?;

        let current_txn_id = transaction_manager.begin()?;

        let after_txn_id = transaction_manager.begin()?;

        // self insert
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, current_txn_id, INVALID_TRANSACTION_ID),
            true
        );
        // self delete
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, current_txn_id, current_txn_id),
            false
        );

        // before insert commit
        assert_eq!(
            transaction_manager.is_visible(
                current_txn_id,
                before_commit_txn_id,
                INVALID_TRANSACTION_ID
            ),
            true
        );
        // before delete commit
        assert_eq!(
            transaction_manager.is_visible(
                current_txn_id,
                before_commit_txn_id,
                before_commit_txn_id
            ),
            false
        );
        // before insert abort
        assert_eq!(
            transaction_manager.is_visible(
                current_txn_id,
                before_abort_txn_id,
                INVALID_TRANSACTION_ID
            ),
            false
        );
        // before delete abort
        assert_eq!(
            transaction_manager.is_visible(
                current_txn_id,
                before_commit_txn_id,
                before_abort_txn_id
            ),
            true
        );

        // running insert
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, running_txn_id, INVALID_TRANSACTION_ID),
            false
        );
        // running delete
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, before_commit_txn_id, running_txn_id),
            true
        );
        // running insert after commit
        transaction_manager.commit(running_txn_id)?;
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, running_txn_id, INVALID_TRANSACTION_ID),
            false
        );
        // running delete after commit
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, before_commit_txn_id, running_txn_id),
            true
        );

        // after insert
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, after_txn_id, INVALID_TRANSACTION_ID),
            false
        );
        // after delete
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, before_commit_txn_id, after_txn_id),
            true
        );
        // after insert after commit
        transaction_manager.commit(after_txn_id)?;
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, after_txn_id, INVALID_TRANSACTION_ID),
            false
        );
        Ok(())
    }
}

use std::collections::HashMap;

use crate::common::TransactionID;

pub struct Transaction {
    snapshot: Vec<TransactionID>,
}

#[derive(Debug, PartialEq)]
pub enum TransactionStatus {
    Running,
    Aborted,
    Committed,
}

#[derive(Debug, PartialEq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

pub struct TransactionManager {
    isolation_level: IsolationLevel,
    next_txn_id: TransactionID,
    statuses: HashMap<TransactionID, TransactionStatus>,
    active_transactions: HashMap<TransactionID, Transaction>,
}

impl TransactionManager {
    pub fn new(isolation_level: IsolationLevel) -> TransactionManager {
        TransactionManager {
            isolation_level,
            next_txn_id: TransactionID(1),
            statuses: HashMap::new(),
            active_transactions: HashMap::new(),
        }
    }

    pub fn begin(&mut self) -> TransactionID {
        let txn_id = self.next_txn_id;
        self.next_txn_id.0 += 1;
        self.statuses.insert(txn_id, TransactionStatus::Running);
        self.active_transactions.insert(
            txn_id,
            Transaction {
                snapshot: self.active_transactions.keys().cloned().collect(),
            },
        );
        txn_id
    }

    pub fn commit(&mut self, txn_id: TransactionID) {
        self.statuses.insert(txn_id, TransactionStatus::Committed);
        self.active_transactions.remove(&txn_id);
    }

    pub fn abort(&mut self, txn_id: TransactionID) {
        self.statuses.insert(txn_id, TransactionStatus::Aborted);
        self.active_transactions.remove(&txn_id);
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

#[cfg(test)]
mod tests {
    use crate::common::INVALID_TRANSACTION_ID;

    use super::*;

    #[test]
    fn test_transaction_manager_begin() {
        let mut transaction_manager = TransactionManager::new(IsolationLevel::ReadCommitted);
        let txn_id = transaction_manager.begin();
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
    }

    #[test]
    fn test_transaction_manager_commit() {
        let mut transaction_manager = TransactionManager::new(IsolationLevel::ReadCommitted);
        let txn_id = transaction_manager.begin();
        transaction_manager.commit(txn_id);
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
    }

    #[test]
    fn test_transaction_manager_abort() {
        let mut transaction_manager = TransactionManager::new(IsolationLevel::ReadCommitted);
        let txn_id = transaction_manager.begin();
        transaction_manager.abort(txn_id);
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
    }

    #[test]
    fn test_transaction_manager_visible_with_read_committed() {
        let mut transaction_manager = TransactionManager::new(IsolationLevel::ReadCommitted);

        let txn_id_1 = transaction_manager.begin();
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

        let txn_id_2 = transaction_manager.begin();
        // other insert
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, INVALID_TRANSACTION_ID),
            false
        );
        // other insert after commit
        transaction_manager.commit(txn_id_1);
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, INVALID_TRANSACTION_ID),
            true
        );

        let txn_id_3 = transaction_manager.begin();
        // other delete
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, txn_id_3),
            true
        );
        // other delete after commit
        transaction_manager.commit(txn_id_3);
        assert_eq!(
            transaction_manager.is_visible(txn_id_2, txn_id_1, txn_id_3),
            false
        );
    }

    #[test]
    fn test_transaction_manager_visible_with_repeatable_read() {
        let mut transaction_manager = TransactionManager::new(IsolationLevel::RepeatableRead);

        let before_commit_txn_id = transaction_manager.begin();
        transaction_manager.commit(before_commit_txn_id);
        let running_txn_id = transaction_manager.begin();
        let before_abort_txn_id = transaction_manager.begin();
        transaction_manager.abort(before_abort_txn_id);

        let current_txn_id = transaction_manager.begin();

        let after_txn_id = transaction_manager.begin();

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
        transaction_manager.commit(running_txn_id);
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
        transaction_manager.commit(after_txn_id);
        assert_eq!(
            transaction_manager.is_visible(current_txn_id, after_txn_id, INVALID_TRANSACTION_ID),
            false
        );
    }
}

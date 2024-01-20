use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
};

use anyhow::Result;

use crate::common::{PageID, TransactionID, LSN, RID};

const LOG_BUFFER_SIZE: usize = 4096;

pub struct LogManager {
    pub log_file: File,
    pub buffer: Vec<u8>,
    pub next_lsn: LSN,
}
impl LogManager {
    pub fn new(log_file_path: &str) -> Result<Self> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_path)?;
        let mut log_manager = Self {
            log_file,
            buffer: vec![],
            next_lsn: LSN(1),
        };
        let records = log_manager.read()?;
        let next_lsn = records
            .last()
            .map_or(LSN(1), |record| LSN(record.lsn.0 + 1));
        log_manager.next_lsn = next_lsn;
        Ok(log_manager)
    }
    pub fn append(&mut self, txn_id: TransactionID, body: LogRecordBody) -> Result<()> {
        let log_record = LogRecord {
            lsn: self.next_lsn,
            txn_id,
            body,
        };
        self.next_lsn.0 += 1;

        let bytes = log_record.serialize();
        if bytes.len() > LOG_BUFFER_SIZE {
            Err(anyhow::anyhow!("log record too large"))?;
        }
        if self.buffer.len() + bytes.len() > LOG_BUFFER_SIZE {
            self.flush()?;
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }
    pub fn read(&mut self) -> Result<Vec<LogRecord>> {
        let mut buffer = vec![];
        self.log_file.seek(std::io::SeekFrom::Start(0))?;
        self.log_file.read_to_end(&mut buffer)?;
        let mut records = vec![];
        let mut offset = 0;
        while offset < buffer.len() {
            let record = LogRecord::from(&buffer[offset..]);
            offset += record.size();
            records.push(record);
        }
        Ok(records)
    }
    pub fn flush(&mut self) -> Result<()> {
        self.log_file.write_all(&self.buffer)?;
        self.log_file.sync_all()?;
        self.buffer.clear();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub lsn: LSN,
    pub txn_id: TransactionID,
    pub body: LogRecordBody,
}

impl From<&[u8]> for LogRecord {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(&bytes[0..8]);
        let lsn = LSN(u64::from_be_bytes(buffer));
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[8..12]);
        let txn_id = TransactionID(u32::from_be_bytes(buffer));
        let body = LogRecordBody::from(&bytes[12..]);
        Self { lsn, txn_id, body }
    }
}
impl LogRecord {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.lsn.0.to_be_bytes());
        buffer.extend_from_slice(&self.txn_id.0.to_be_bytes());
        buffer.extend_from_slice(&self.body.serialize());
        buffer
    }
    fn size(&self) -> usize {
        12 + self.body.size()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogRecordBody {
    BeginTransaction,
    CommitTransaction,
    AbortTransaction,
    InsertToTablePage(InsertToTablePage),
    DeleteFromTablePage(DeleteFromTablePage),
    SetNextPageID(SetNextPageID),
    NewTablePage(NewTablePage),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertToTablePage {
    pub page_id: PageID,
    pub data: Box<[u8]>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFromTablePage {
    pub rid: RID,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetNextPageID {
    pub page_id: PageID,
    pub next_page_id: PageID,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTablePage {
    pub page_id: PageID,
}

impl From<&[u8]> for LogRecordBody {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let type_id = u32::from_be_bytes(buffer);
        match type_id {
            0 => LogRecordBody::BeginTransaction,
            1 => LogRecordBody::CommitTransaction,
            2 => LogRecordBody::AbortTransaction,
            3 => LogRecordBody::InsertToTablePage(InsertToTablePage::from(&bytes[4..])),
            4 => LogRecordBody::DeleteFromTablePage(DeleteFromTablePage::from(&bytes[4..])),
            5 => LogRecordBody::SetNextPageID(SetNextPageID::from(&bytes[4..])),
            6 => LogRecordBody::NewTablePage(NewTablePage::from(&bytes[4..])),
            _ => panic!("invalid log record type id"),
        }
    }
}
impl LogRecordBody {
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match &self {
            LogRecordBody::BeginTransaction => {
                buffer.extend_from_slice(&(0u32).to_be_bytes());
            }
            LogRecordBody::CommitTransaction => {
                buffer.extend_from_slice(&(1u32).to_be_bytes());
            }
            LogRecordBody::AbortTransaction => {
                buffer.extend_from_slice(&(2u32).to_be_bytes());
            }
            LogRecordBody::InsertToTablePage(body) => {
                buffer.extend_from_slice(&(3u32).to_be_bytes());
                buffer.extend_from_slice(&body.serialize());
            }
            LogRecordBody::DeleteFromTablePage(body) => {
                buffer.extend_from_slice(&(4u32).to_be_bytes());
                buffer.extend_from_slice(&body.serialize());
            }
            LogRecordBody::SetNextPageID(body) => {
                buffer.extend_from_slice(&(5u32).to_be_bytes());
                buffer.extend_from_slice(&body.serialize());
            }
            LogRecordBody::NewTablePage(body) => {
                buffer.extend_from_slice(&(6u32).to_be_bytes());
                buffer.extend_from_slice(&body.serialize());
            }
        }
        buffer
    }
    fn size(&self) -> usize {
        match &self {
            LogRecordBody::BeginTransaction => 4,
            LogRecordBody::CommitTransaction => 4,
            LogRecordBody::AbortTransaction => 4,
            LogRecordBody::InsertToTablePage(body) => 4 + body.size(),
            LogRecordBody::DeleteFromTablePage(body) => 4 + body.size(),
            LogRecordBody::SetNextPageID(body) => 4 + body.size(),
            LogRecordBody::NewTablePage(body) => 4 + body.size(),
        }
    }
}

impl From<&[u8]> for InsertToTablePage {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let page_id = PageID(u32::from_be_bytes(buffer));
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[4..8]);
        let size = u32::from_be_bytes(buffer);
        let data = bytes[8..(8 + size as usize)].to_vec().into_boxed_slice();
        InsertToTablePage { page_id, data }
    }
}
impl InsertToTablePage {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.page_id.0.to_be_bytes());
        buffer.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&self.data);
        buffer
    }
    fn size(&self) -> usize {
        8 + self.data.len()
    }
}
impl From<&[u8]> for DeleteFromTablePage {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let page_id = PageID(u32::from_be_bytes(buffer));
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[4..8]);
        let index = u32::from_be_bytes(buffer);
        DeleteFromTablePage {
            rid: RID(page_id, index),
        }
    }
}
impl DeleteFromTablePage {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.rid.0 .0.to_be_bytes());
        buffer.extend_from_slice(&self.rid.1.to_be_bytes());
        buffer
    }
    fn size(&self) -> usize {
        8
    }
}
impl From<&[u8]> for SetNextPageID {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let page_id = PageID(u32::from_be_bytes(buffer));
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[4..8]);
        let next_page_id = PageID(u32::from_be_bytes(buffer));
        SetNextPageID {
            page_id,
            next_page_id,
        }
    }
}
impl SetNextPageID {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.page_id.0.to_be_bytes());
        buffer.extend_from_slice(&self.next_page_id.0.to_be_bytes());
        buffer
    }
    fn size(&self) -> usize {
        8
    }
}
impl From<&[u8]> for NewTablePage {
    fn from(bytes: &[u8]) -> Self {
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let page_id = PageID(u32::from_be_bytes(buffer));
        NewTablePage { page_id }
    }
}
impl NewTablePage {
    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.page_id.0.to_be_bytes());
        buffer
    }
    fn size(&self) -> usize {
        4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_log_manager() -> Result<()> {
        let dir = tempdir()?;
        let log_file_path = dir.path().join("log");
        let mut log_manager = LogManager::new(log_file_path.to_str().unwrap())?;

        log_manager.append(TransactionID(1), LogRecordBody::BeginTransaction)?;
        log_manager.append(
            TransactionID(1),
            LogRecordBody::InsertToTablePage(InsertToTablePage {
                page_id: PageID(1),
                data: vec![1, 2, 3].into(),
            }),
        )?;
        log_manager.append(TransactionID(1), LogRecordBody::CommitTransaction)?;
        log_manager.append(TransactionID(2), LogRecordBody::BeginTransaction)?;
        log_manager.append(
            TransactionID(2),
            LogRecordBody::DeleteFromTablePage(DeleteFromTablePage {
                rid: RID(PageID(1), 0),
            }),
        )?;
        log_manager.append(
            TransactionID(2),
            LogRecordBody::NewTablePage(NewTablePage { page_id: PageID(2) }),
        )?;
        log_manager.append(
            TransactionID(2),
            LogRecordBody::SetNextPageID(SetNextPageID {
                page_id: PageID(1),
                next_page_id: PageID(2),
            }),
        )?;
        log_manager.append(TransactionID(2), LogRecordBody::AbortTransaction)?;
        log_manager.flush()?;

        let mut log_manager = LogManager::new(log_file_path.to_str().unwrap())?;
        assert_eq!(log_manager.next_lsn, LSN(9));
        let records = log_manager.read()?;
        assert_eq!(records[0].lsn, LSN(1));
        assert_eq!(records[0].txn_id, TransactionID(1));
        assert_eq!(records[0].body, LogRecordBody::BeginTransaction);
        assert_eq!(records[1].lsn, LSN(2));
        assert_eq!(records[1].txn_id, TransactionID(1));
        assert_eq!(
            records[1].body,
            LogRecordBody::InsertToTablePage(InsertToTablePage {
                page_id: PageID(1),
                data: vec![1, 2, 3].into(),
            })
        );
        assert_eq!(records[2].lsn, LSN(3));
        assert_eq!(records[2].txn_id, TransactionID(1));
        assert_eq!(records[2].body, LogRecordBody::CommitTransaction);
        assert_eq!(records[4].lsn, LSN(5));
        assert_eq!(records[4].txn_id, TransactionID(2));
        assert_eq!(
            records[4].body,
            LogRecordBody::DeleteFromTablePage(DeleteFromTablePage {
                rid: RID(PageID(1), 0),
            })
        );
        assert_eq!(records[5].lsn, LSN(6));
        assert_eq!(records[5].txn_id, TransactionID(2));
        assert_eq!(
            records[5].body,
            LogRecordBody::NewTablePage(NewTablePage { page_id: PageID(2) })
        );
        assert_eq!(records[6].lsn, LSN(7));
        assert_eq!(records[6].txn_id, TransactionID(2));
        assert_eq!(
            records[6].body,
            LogRecordBody::SetNextPageID(SetNextPageID {
                page_id: PageID(1),
                next_page_id: PageID(2),
            })
        );
        assert_eq!(records[7].lsn, LSN(8));
        assert_eq!(records[7].txn_id, TransactionID(2));
        assert_eq!(records[7].body, LogRecordBody::AbortTransaction);

        Ok(())
    }
}

use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    common::{PageID, TransactionID},
    concurrency::TransactionManager,
    lock::LockManager,
    page::table_page::TABLE_PAGE_PAGE_TYPE,
    table::TableHeap,
    value::{integer::IntegerValue, varchar::VarcharValue, Value},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}
impl Schema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DataType {
    Integer,
    Varchar,
    Boolean,
}
impl From<i64> for DataType {
    fn from(data_type: i64) -> Self {
        match data_type {
            0 => Self::Integer,
            1 => Self::Varchar,
            2 => Self::Boolean,
            _ => unreachable!(),
        }
    }
}
impl From<DataType> for i64 {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Integer => 0,
            DataType::Varchar => 1,
            DataType::Boolean => 2,
        }
    }
}

pub struct Catalog {
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    lock_manager: Arc<RwLock<LockManager>>,
    next_table_id: u32,
}

impl Catalog {
    pub fn new(
        buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        lock_manager: Arc<RwLock<LockManager>>,
    ) -> Self {
        Self {
            buffer_pool_manager,
            transaction_manager,
            lock_manager,
            next_table_id: 0,
        }
    }
    pub fn bootstrap(&mut self, init: bool) -> Result<()> {
        if !init {
            self.set_next_table_id()?;
            return Ok(());
        }
        for _ in 0..SYSTEM_TABLE_COUNT {
            self.create_empty_system_table()?;
        }

        let txn_id = self
            .transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        self.create_system_table(
            "system_tables",
            &Self::system_tables_schema(),
            txn_id,
            SYSTEM_TABLES_FIRST_PAGE_ID,
        )?;
        self.create_system_table(
            "system_columns",
            &Self::system_columns_schema(),
            txn_id,
            SYSTEM_COLUMNS_FIRST_PAGE_ID,
        )?;
        self.transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;

        Ok(())
    }
    pub fn create_table(
        &mut self,
        name: &str,
        schema: &Schema,
        txn_id: TransactionID,
    ) -> Result<()> {
        let page = self
            .buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .new_page(TABLE_PAGE_PAGE_TYPE)?;
        self.buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .unpin_page(
                page.read()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .page_id(),
                true,
            )?;
        let mut system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        let table_id = self.next_table_id;
        let values = vec![
            Value::Integer(IntegerValue(table_id as i64)),
            Value::Varchar(VarcharValue(name.to_string())),
            Value::Integer(IntegerValue(
                page.read()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .page_id()
                    .0 as i64,
            )),
        ];
        system_tables_table.insert(&values)?;
        self.next_table_id += 1;
        let mut system_columns_table =
            self.system_table_heap(PageID(SYSTEM_COLUMNS_FIRST_PAGE_ID.0), txn_id);
        for (i, column) in schema.columns.iter().enumerate() {
            let values = vec![
                Value::Integer(IntegerValue(table_id as i64)),
                Value::Varchar(VarcharValue(column.name.to_string())),
                Value::Integer(IntegerValue(i as i64)),
                Value::Integer(IntegerValue(column.data_type.clone().into())),
            ];
            system_columns_table.insert(&values)?;
        }
        Ok(())
    }
    pub fn get_first_page_id_by_table_name(
        &self,
        table_name: &str,
        txn_id: TransactionID,
    ) -> Result<PageID> {
        let system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        for tuple in system_tables_table.iter() {
            let values = tuple.values(&Self::system_tables_schema());
            if let Value::Varchar(VarcharValue(name)) = &values[1] {
                if name == table_name {
                    if let Value::Integer(IntegerValue(first_page_id)) = values[2] {
                        return Ok(PageID(first_page_id as u32));
                    }
                }
            }
        }
        Err(anyhow::anyhow!("table not found"))
    }
    pub fn get_schema_by_table_name(
        &self,
        table_name: &str,
        txn_id: TransactionID,
    ) -> Result<Schema> {
        let table_id = self.get_table_id_by_table_name(table_name, txn_id)?;
        let mut schema = Schema { columns: vec![] };
        let system_columns_table =
            self.system_table_heap(PageID(SYSTEM_COLUMNS_FIRST_PAGE_ID.0), txn_id);
        for tuple in system_columns_table.iter() {
            let values = tuple.values(&Self::system_columns_schema());
            if let Value::Integer(IntegerValue(table_id_)) = values[0] {
                if table_id_ as u32 == table_id {
                    if let Value::Varchar(VarcharValue(name)) = &values[1] {
                        // if let Value::Int(IntValue(ordinal_position)) = values[2] {
                        if let Value::Integer(IntegerValue(data_type)) = values[3] {
                            schema.columns.push(Column {
                                name: name.to_string(),
                                data_type: data_type.into(),
                            });
                        }
                        // }
                    }
                }
            }
        }
        Ok(schema)
    }

    fn create_empty_system_table(&self) -> Result<()> {
        let page = self
            .buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .new_page(TABLE_PAGE_PAGE_TYPE)?;
        self.buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .unpin_page(
                page.read()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .page_id(),
                true,
            )?;
        Ok(())
    }
    // TODO: refactor
    pub fn create_system_table(
        &mut self,
        name: &str,
        schema: &Schema,
        txn_id: TransactionID,
        first_page_id: PageID,
    ) -> Result<()> {
        let mut system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        let table_id = self.next_table_id;
        let values = vec![
            Value::Integer(IntegerValue(table_id as i64)),
            Value::Varchar(VarcharValue(name.to_string())),
            Value::Integer(IntegerValue(first_page_id.0 as i64)),
        ];
        system_tables_table.insert(&values)?;
        self.next_table_id += 1;
        let mut system_columns_table =
            self.system_table_heap(PageID(SYSTEM_COLUMNS_FIRST_PAGE_ID.0), txn_id);
        for (i, column) in schema.columns.iter().enumerate() {
            let values = vec![
                Value::Integer(IntegerValue(table_id as i64)),
                Value::Varchar(VarcharValue(column.name.to_string())),
                Value::Integer(IntegerValue(i as i64)),
                Value::Integer(IntegerValue(match column.data_type {
                    DataType::Integer => 0,
                    DataType::Varchar => 1,
                    DataType::Boolean => 2,
                })),
            ];
            system_columns_table.insert(&values)?;
        }
        Ok(())
    }
    fn system_table_heap(&self, first_page_id: PageID, txn_id: TransactionID) -> TableHeap {
        TableHeap::new(
            first_page_id,
            self.buffer_pool_manager.clone(),
            self.transaction_manager.clone(),
            self.lock_manager.clone(),
            txn_id,
        )
    }
    fn set_next_table_id(&mut self) -> Result<()> {
        let mut max_table_id = 0;
        let txn_id = self
            .transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        let system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        for tuple in system_tables_table.iter() {
            let values = tuple.values(&Self::system_tables_schema());
            if let Value::Integer(IntegerValue(table_id)) = values[0] {
                if table_id > max_table_id {
                    max_table_id = table_id;
                }
            }
        }
        self.next_table_id = (max_table_id as u32) + 1;
        self.transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;
        Ok(())
    }
    fn get_table_id_by_table_name(&self, table_name: &str, txn_id: TransactionID) -> Result<u32> {
        let system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        for tuple in system_tables_table.iter() {
            let values = tuple.values(&Self::system_tables_schema());
            if let Value::Varchar(VarcharValue(name)) = &values[1] {
                if name == table_name {
                    if let Value::Integer(IntegerValue(table_id)) = values[0] {
                        return Ok(table_id as u32);
                    }
                }
            }
        }
        Err(anyhow::anyhow!("table not found"))
    }

    pub fn system_tables_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "first_page_id".to_string(),
                    data_type: DataType::Integer,
                },
            ],
        }
    }
    fn system_columns_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "table_id".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "ordinal_position".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "data_type".to_string(),
                    data_type: DataType::Integer,
                },
            ],
        }
    }
}

const SYSTEM_TABLE_COUNT: usize = 2;
const SYSTEM_TABLES_FIRST_PAGE_ID: PageID = PageID(1);
const SYSTEM_COLUMNS_FIRST_PAGE_ID: PageID = PageID(2);
// const SYSTEM_INDEXES_FIRST_PAGE_ID: PageID = PageID(3);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, RwLock};

    use anyhow::Result;
    use tempfile::tempdir;

    use crate::{
        buffer::BufferPoolManager,
        catalog::{Catalog, Column, DataType, Schema},
        concurrency::{IsolationLevel, TransactionManager},
        disk::DiskManager,
        lock::LockManager,
        table::TableHeap,
        value::{integer::IntegerValue, varchar::VarcharValue, Value},
    };

    #[test]
    fn test_catalog() -> Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let txn_log_file_path = dir.path().join("transaction.log");
        let disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, 10)));
        let lock_manager = Arc::new(RwLock::new(LockManager::default()));
        let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
            lock_manager.clone(),
            txn_log_file_path.to_str().unwrap(),
            IsolationLevel::RepeatableRead,
        )?));
        let mut catalog = Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
        );

        // bootstrap
        catalog.bootstrap(true)?;

        // create_table and insert
        let txn_id = transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        catalog.create_table(
            "test",
            &Schema {
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                    },
                    Column {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                    },
                    Column {
                        name: "age".to_string(),
                        data_type: DataType::Integer,
                    },
                ],
            },
            txn_id,
        )?;
        let first_page_id = catalog.get_first_page_id_by_table_name("test", txn_id)?;
        let mut table_heap = TableHeap::new(
            first_page_id,
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
            txn_id,
        );
        let values = vec![
            Value::Integer(IntegerValue(1)),
            Value::Varchar(VarcharValue("name1".to_string())),
            Value::Integer(IntegerValue(10)),
        ];
        table_heap.insert(&values)?;
        transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;

        // check
        let txn_id = transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        let first_page_id = catalog.get_first_page_id_by_table_name("test", txn_id)?;
        let schema = catalog.get_schema_by_table_name("test", txn_id)?;
        let table_heap = TableHeap::new(
            first_page_id,
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            lock_manager.clone(),
            txn_id,
        );
        for tuple in table_heap.iter() {
            let values = tuple.values(&schema);
            assert_eq!(values[0], Value::Integer(IntegerValue(1)));
            assert_eq!(values[1], Value::Varchar(VarcharValue("name1".to_string())));
            assert_eq!(values[2], Value::Integer(IntegerValue(10)));
        }
        transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;

        Ok(())
    }
}

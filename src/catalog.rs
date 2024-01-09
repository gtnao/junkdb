use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    common::{PageID, TransactionID},
    concurrency::TransactionManager,
    lock::LockManager,
    page::table_page::TABLE_PAGE_PAGE_TYPE,
    table::TableHeap,
    value::{UnsignedBigIntegerValue, UnsignedIntegerValue, Value, VarcharValue},
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
    UnsignedInteger,
    BigInteger,
    UnsignedBigInteger,
    Varchar,
    Boolean,
}
impl From<u32> for DataType {
    fn from(data_type: u32) -> Self {
        match data_type {
            0 => Self::Integer,
            1 => Self::UnsignedInteger,
            2 => Self::BigInteger,
            3 => Self::UnsignedBigInteger,
            4 => Self::Varchar,
            5 => Self::Boolean,
            _ => unreachable!(),
        }
    }
}
impl From<DataType> for u32 {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Integer => 0,
            DataType::UnsignedInteger => 1,
            DataType::BigInteger => 2,
            DataType::UnsignedBigInteger => 3,
            DataType::Varchar => 4,
            DataType::Boolean => 5,
        }
    }
}
impl DataType {
    pub fn convert_with(&self, other: Self) -> Self {
        match (&self, other) {
            (DataType::Integer, DataType::UnsignedInteger) => DataType::UnsignedInteger,
            (DataType::Integer, DataType::BigInteger) => DataType::BigInteger,
            (DataType::Integer, DataType::UnsignedBigInteger) => DataType::UnsignedBigInteger,
            (DataType::UnsignedInteger, DataType::Integer) => DataType::UnsignedInteger,
            (DataType::UnsignedInteger, DataType::BigInteger) => DataType::BigInteger,
            (DataType::UnsignedInteger, DataType::UnsignedBigInteger) => {
                DataType::UnsignedBigInteger
            }
            (DataType::BigInteger, DataType::Integer) => DataType::BigInteger,
            (DataType::BigInteger, DataType::UnsignedInteger) => DataType::BigInteger,
            (DataType::BigInteger, DataType::UnsignedBigInteger) => DataType::UnsignedBigInteger,
            (DataType::UnsignedBigInteger, DataType::Integer) => DataType::UnsignedBigInteger,
            (DataType::UnsignedBigInteger, DataType::UnsignedInteger) => {
                DataType::UnsignedBigInteger
            }
            (DataType::UnsignedBigInteger, DataType::BigInteger) => DataType::UnsignedBigInteger,
            _ => unimplemented!(),
        }
    }
}

pub struct Catalog {
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    lock_manager: Arc<RwLock<LockManager>>,
    next_table_id: u64,
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
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)),
            Value::Varchar(VarcharValue(name.to_string())),
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(
                page.read()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .page_id()
                    .0,
            )),
        ];
        system_tables_table.insert(&values)?;
        self.next_table_id += 1;
        let mut system_columns_table =
            self.system_table_heap(PageID(SYSTEM_COLUMNS_FIRST_PAGE_ID.0), txn_id);
        for (i, column) in schema.columns.iter().enumerate() {
            let values = vec![
                Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)),
                Value::Varchar(VarcharValue(column.name.to_string())),
                Value::UnsignedInteger(UnsignedIntegerValue(i as u32)),
                Value::UnsignedInteger(UnsignedIntegerValue(column.data_type.clone().into())),
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
                    if let Value::UnsignedBigInteger(UnsignedBigIntegerValue(first_page_id)) =
                        values[2]
                    {
                        return Ok(PageID(first_page_id as u64));
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
            if let Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id_)) = values[0] {
                if table_id_ == table_id {
                    if let Value::Varchar(VarcharValue(name)) = &values[1] {
                        // if let Value::Int(IntValue(ordinal_position)) = values[2] {
                        if let Value::UnsignedInteger(UnsignedIntegerValue(data_type)) = values[3] {
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
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)),
            Value::Varchar(VarcharValue(name.to_string())),
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(first_page_id.0)),
        ];
        system_tables_table.insert(&values)?;
        self.next_table_id += 1;
        let mut system_columns_table =
            self.system_table_heap(PageID(SYSTEM_COLUMNS_FIRST_PAGE_ID.0), txn_id);
        for (i, column) in schema.columns.iter().enumerate() {
            let values = vec![
                Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)),
                Value::Varchar(VarcharValue(column.name.to_string())),
                Value::UnsignedInteger(UnsignedIntegerValue(i as u32)),
                Value::UnsignedInteger(UnsignedIntegerValue(match column.data_type {
                    DataType::Integer => 0,
                    DataType::UnsignedInteger => 1,
                    DataType::BigInteger => 2,
                    DataType::UnsignedBigInteger => 3,
                    DataType::Varchar => 4,
                    DataType::Boolean => 5,
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
            if let Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)) = values[0] {
                if table_id > max_table_id {
                    max_table_id = table_id;
                }
            }
        }
        self.next_table_id = max_table_id + 1;
        self.transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;
        Ok(())
    }
    fn get_table_id_by_table_name(&self, table_name: &str, txn_id: TransactionID) -> Result<u64> {
        let system_tables_table =
            self.system_table_heap(PageID(SYSTEM_TABLES_FIRST_PAGE_ID.0), txn_id);
        for tuple in system_tables_table.iter() {
            let values = tuple.values(&Self::system_tables_schema());
            if let Value::Varchar(VarcharValue(name)) = &values[1] {
                if name == table_name {
                    if let Value::UnsignedBigInteger(UnsignedBigIntegerValue(table_id)) = values[0]
                    {
                        return Ok(table_id);
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
                    data_type: DataType::UnsignedBigInteger,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "first_page_id".to_string(),
                    data_type: DataType::UnsignedBigInteger,
                },
            ],
        }
    }
    fn system_columns_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "table_id".to_string(),
                    data_type: DataType::UnsignedBigInteger,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "ordinal_position".to_string(),
                    data_type: DataType::UnsignedInteger,
                },
                Column {
                    name: "data_type".to_string(),
                    data_type: DataType::UnsignedInteger,
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
        value::{UnsignedBigIntegerValue, UnsignedIntegerValue, Value, VarcharValue},
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
                        data_type: DataType::UnsignedBigInteger,
                    },
                    Column {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                    },
                    Column {
                        name: "age".to_string(),
                        data_type: DataType::UnsignedInteger,
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
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(1)),
            Value::Varchar(VarcharValue("name1".to_string())),
            Value::UnsignedInteger(UnsignedIntegerValue(10)),
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
            assert_eq!(
                values[0],
                Value::UnsignedBigInteger(UnsignedBigIntegerValue(1))
            );
            assert_eq!(values[1], Value::Varchar(VarcharValue("name1".to_string())));
            assert_eq!(values[2], Value::UnsignedInteger(UnsignedIntegerValue(10)));
        }
        transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(txn_id)?;

        Ok(())
    }
}

use std::{
    fs::File,
    sync::{Arc, Mutex},
    thread,
};

extern crate prettytable;
use anyhow::Result;
use prettytable::{Cell, Row, Table};
use toydb::{
    buffer::BufferPoolManager,
    catalog::{Column, DataType, Schema},
    common::PageID,
    concurrency::{IsolationLevel, TransactionManager},
    disk::DiskManager,
    page::table_page::TablePage,
    table::TableHeap,
    value::{IntValue, Value, VarcharValue},
};

fn main() -> Result<()> {
    // init
    let mut disk_manager = DiskManager::new("test.db")?;
    let size = File::open("test.db")?.metadata()?.len();
    if size == 0 {
        let page_id = disk_manager.allocate_page()?;
        let table_page = TablePage::new(page_id);
        disk_manager.write_page(page_id, &table_page.data)?;
    }

    // sample schema
    let schema = Schema {
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
    };

    // components
    let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, 10)));
    let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
        // IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
    )));

    // sample inserts
    let mut handles = vec![];
    for i in 0..20 {
        let buffer_pool_manager = buffer_pool_manager.clone();
        let transaction_manager = transaction_manager.clone();
        let handle = thread::spawn(move || -> Result<()> {
            let values = vec![
                Value::Int(IntValue(i as i32)),
                Value::Varchar(VarcharValue(format!("name{}", i))),
                Value::Int(IntValue(i as i32 * 10)),
            ];
            let txn_id = transaction_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .begin();
            let mut table = TableHeap::new(
                PageID(1),
                buffer_pool_manager,
                transaction_manager.clone(),
                txn_id,
            );
            table.insert(&values)?;
            transaction_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .commit(txn_id);
            Ok(())
        });
        handles.push(handle);
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("thread error"))??;
    }

    // read
    let txn_id = transaction_manager
        .lock()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .begin();
    {
        // other write
        let other_txn_id = transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .begin();
        let values = vec![
            Value::Int(IntValue(100 as i32)),
            Value::Varchar(VarcharValue(format!("other_name"))),
            Value::Int(IntValue(30)),
        ];
        let mut table = TableHeap::new(
            PageID(1),
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
            other_txn_id,
        );
        table.insert(&values)?;
        transaction_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .commit(other_txn_id);
    }

    let mut table_view = Table::new();
    table_view.add_row(Row::new(
        schema
            .columns
            .iter()
            .map(|c| Cell::new(&c.name))
            .collect::<Vec<Cell>>(),
    ));
    let mut size = 0;
    let table = TableHeap::new(
        PageID(1),
        buffer_pool_manager.clone(),
        transaction_manager.clone(),
        txn_id,
    );
    for tuple in table.iter() {
        size += 1;
        let values = tuple.values(&schema);
        let cells = values
            .iter()
            .map(|v| match v {
                Value::Int(v) => Cell::new(&v.0.to_string()),
                Value::Varchar(v) => Cell::new(&v.0),
            })
            .collect::<Vec<Cell>>();
        table_view.add_row(Row::new(cells));
    }
    table_view.printstd();
    println!("table size: {}", size);

    // shutdown
    buffer_pool_manager
        .lock()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .shutdown()?;

    Ok(())
}

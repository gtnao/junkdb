use std::{
    fs::File,
    sync::{Arc, Mutex},
    thread,
};

extern crate prettytable;
use anyhow::Result;
use prettytable::{Cell, Row, Table};
use toydb::{
    catalog::{Column, DataType, Schema},
    common::PageID,
    storage::{
        buffer::BufferPoolManager, disk::DiskManager, page::table_page::TablePage, table::TableHeap,
    },
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

    let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager)));

    let mut handles = vec![];
    for i in 0..20 {
        let buffer_pool_manager = buffer_pool_manager.clone();
        let handle = thread::spawn(move || -> Result<()> {
            let values = vec![
                Value::Int(IntValue(i as i32)),
                Value::Varchar(VarcharValue(format!("name{}", i))),
                Value::Int(IntValue(i as i32 * 10)),
            ];
            let bytes = values
                .iter()
                .map(|v| v.serialize())
                .flatten()
                .collect::<Vec<u8>>();
            let mut table = TableHeap::new(PageID(0), buffer_pool_manager);
            table.insert(&bytes)?;
            Ok(())
        });
        handles.push(handle);
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("thread error"))??;
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
    let table = TableHeap::new(PageID(0), buffer_pool_manager.clone());
    for tuple in table.iter() {
        size += 1;
        let values = Value::deserialize_values(&schema, &tuple);
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

    buffer_pool_manager
        .lock()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .flush_all_pages()?;

    Ok(())
}

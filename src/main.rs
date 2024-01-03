use std::{
    fs::File,
    sync::{Arc, Mutex},
    thread,
};

use anyhow::Result;
use toydb::{
    common::PageID,
    storage::{
        buffer::BufferPoolManager, disk::DiskManager, page::table_page::TablePage, table::TableHeap,
    },
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

    let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager)));

    let mut handles = vec![];
    for i in 0..255 {
        let buffer_pool_manager = buffer_pool_manager.clone();
        let handle = thread::spawn(move || -> Result<()> {
            let mut table = TableHeap::new(PageID(0), buffer_pool_manager);
            table.insert(&[i; 16])?;
            Ok(())
        });
        handles.push(handle);
    }
    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("thread error"))??;
    }

    let table = TableHeap::new(PageID(0), buffer_pool_manager.clone());
    for tuple in table.iter() {
        println!("{:?}", tuple);
    }

    buffer_pool_manager
        .lock()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .flush_all_pages()?;

    Ok(())
}

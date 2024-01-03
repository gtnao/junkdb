use std::fs::File;

use anyhow::Result;
use toydb::{
    common::PageID,
    storage::{buffer::BufferPoolManager, disk::DiskManager, page::table_page::TablePage},
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

    let mut buffer_pool_manager = BufferPoolManager::new(disk_manager);
    let page = buffer_pool_manager.fetch_page(PageID(0))?;

    let tuples = page
        .read()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .with_table_page(|table_page| table_page.get_tuples());
    println!("{:?}", tuples);

    page.write()
        .map_err(|_| anyhow::anyhow!("lock error"))?
        .with_table_page_mut(|table_page| table_page.insert(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))?;

    buffer_pool_manager.unpin_page(PageID(0), true)?;
    buffer_pool_manager.flush_all_pages()?;

    Ok(())
}

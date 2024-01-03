use std::fs::File;

use anyhow::Result;
use toydb::{
    common::{PageID, PAGE_SIZE},
    storage::{disk::DiskManager, page::table_page::TablePage},
};

fn main() -> Result<()> {
    let mut disk_manager = DiskManager::new("test.db")?;
    let size = File::open("test.db")?.metadata()?.len();
    if size == 0 {
        let page_id = disk_manager.allocate_page()?;
        let table_page = TablePage::new(page_id);
        disk_manager.write_page(page_id, &table_page.data)?;
    }
    let mut buffer = vec![0u8; PAGE_SIZE];
    disk_manager.read_page(PageID(0), &mut buffer)?;
    let mut table_page = TablePage::from_data(buffer.into_boxed_slice());

    let tuples = table_page.get_tuples();
    println!("{:?}", tuples);

    table_page.insert(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    disk_manager.write_page(PageID(0), &table_page.data)?;

    Ok(())
}

use crate::common::PageID;

use self::table_page::TablePage;

pub mod table_page;

const PAGE_TYPE_OFFSET: usize = 0;
const PAGE_TYPE_SIZE: usize = 4;
const PAGE_ID_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
const PAGE_ID_SIZE: usize = 8;

pub enum Page {
    Table(TablePage),
}
impl Page {
    pub fn with_table_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&TablePage) -> R,
    {
        match self {
            Page::Table(table_page) => f(table_page),
        }
    }
    pub fn with_table_page_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut TablePage) -> R,
    {
        match self {
            Page::Table(table_page) => f(table_page),
        }
    }
    pub fn new(page_id: PageID) -> Self {
        Page::Table(TablePage::new(page_id))
    }
    pub fn from_data(data: Box<[u8]>) -> Self {
        Page::Table(TablePage::from_data(data))
    }
    pub fn data(&self) -> &[u8] {
        match self {
            Page::Table(table_page) => &table_page.data,
        }
    }
    pub fn page_id(&self) -> PageID {
        match self {
            Page::Table(table_page) => table_page.page_id(),
        }
    }
}

use self::table_page::TablePage;

pub mod table_page;

const PAGE_TYPE_OFFSET: usize = 0;
const PAGE_TYPE_SIZE: usize = 4;
const PAGE_ID_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
const PAGE_ID_SIZE: usize = 8;

pub enum Page {
    Table(TablePage),
}

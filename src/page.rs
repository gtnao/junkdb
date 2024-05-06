use crate::common::{PageID, INVALID_PAGE_ID};

use self::{
    b_plus_tree_leaf_page::{BPlusTreeLeafPage, B_PLUS_TREE_LEAF_PAGE_PAGE_TYPE},
    table_page::{TablePage, TABLE_PAGE_PAGE_TYPE},
};

pub mod b_plus_tree_internal_page;
pub mod b_plus_tree_leaf_page;
pub mod table_page;

const PAGE_TYPE_OFFSET: usize = 0;
const PAGE_TYPE_SIZE: usize = 4;
const PAGE_ID_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
const PAGE_ID_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageType(pub u32);

#[derive(Debug)]
pub enum Page {
    Table(TablePage),
    BPlusTreeLeaf(BPlusTreeLeafPage),
}
impl Page {
    pub fn with_table_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&TablePage) -> R,
    {
        match self {
            Page::Table(table_page) => f(table_page),
            _ => panic!("page type not supported"),
        }
    }
    pub fn with_table_page_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut TablePage) -> R,
    {
        match self {
            Page::Table(table_page) => f(table_page),
            _ => panic!("page type not supported"),
        }
    }
    pub fn with_b_plus_tree_leaf_page<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BPlusTreeLeafPage) -> R,
    {
        match self {
            Page::BPlusTreeLeaf(b_plus_tree_leaf_page) => f(b_plus_tree_leaf_page),
            _ => panic!("page type not supported"),
        }
    }
    pub fn with_b_plus_tree_leaf_page_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut BPlusTreeLeafPage) -> R,
    {
        match self {
            Page::BPlusTreeLeaf(b_plus_tree_leaf_page) => f(b_plus_tree_leaf_page),
            _ => panic!("page type not supported"),
        }
    }
    pub fn new(page_id: PageID, page_type: PageType) -> Self {
        match page_type {
            TABLE_PAGE_PAGE_TYPE => Page::Table(TablePage::new(page_id)),
            // TODO:
            B_PLUS_TREE_LEAF_PAGE_PAGE_TYPE => {
                Page::BPlusTreeLeaf(BPlusTreeLeafPage::new(page_id, INVALID_PAGE_ID, None))
            }
            _ => panic!("page type not supported"),
        }
    }
    pub fn from_data(data: &[u8]) -> Self {
        Page::Table(TablePage::from_data(data))
    }
    pub fn data(&self) -> &[u8] {
        match self {
            Page::Table(table_page) => &table_page.data,
            Page::BPlusTreeLeaf(b_plus_tree_leaf_page) => &b_plus_tree_leaf_page.data,
        }
    }
    pub fn page_id(&self) -> PageID {
        match self {
            Page::Table(table_page) => table_page.page_id(),
            Page::BPlusTreeLeaf(b_plus_tree_leaf_page) => b_plus_tree_leaf_page.page_id(),
        }
    }
}

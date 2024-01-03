#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageID(pub u64);
pub const PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageID = PageID(0);

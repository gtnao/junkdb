#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageID(pub u32);
pub const PAGE_SIZE: usize = 4096;
pub const INVALID_PAGE_ID: PageID = PageID(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionID(pub u32);
pub const INVALID_TRANSACTION_ID: TransactionID = TransactionID(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RID(pub PageID, pub u32);

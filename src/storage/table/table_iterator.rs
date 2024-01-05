use crate::{
    common::{PageID, INVALID_PAGE_ID},
    storage::tuple::Tuple,
};

use super::TableHeap;

pub struct TableIterator<'a> {
    heap: &'a TableHeap,
    next_page_id: Option<PageID>,
    tuples: Vec<Box<[u8]>>,
    tuple_index: usize,
}

impl TableHeap {
    pub fn iter(&self) -> TableIterator {
        let page_id = self.first_page_id;
        TableIterator {
            heap: self,
            next_page_id: Some(page_id),
            tuples: Vec::new(),
            tuple_index: 0,
        }
    }
}

impl Iterator for TableIterator<'_> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let tuple = self.next_internal();
            match tuple {
                Some(tuple) => {
                    if self.heap.transaction_manager.lock().ok()?.is_visible(
                        self.heap.txn_id,
                        tuple.xmin(),
                        tuple.xmax(),
                    ) {
                        return Some(tuple);
                    }
                }
                None => return None,
            }
        }
    }
}

impl TableIterator<'_> {
    fn next_internal(&mut self) -> Option<Tuple> {
        if self.tuple_index >= self.tuples.len() {
            let next_page_id = self.next_page_id?;
            let page = self
                .heap
                .buffer_pool_manager
                .lock()
                .ok()?
                .fetch_page(next_page_id)
                .ok()?;
            self.next_page_id = page.read().ok()?.with_table_page(|table_page| {
                if table_page.next_page_id() == INVALID_PAGE_ID {
                    None
                } else {
                    Some(table_page.next_page_id())
                }
            });
            self.tuples = page
                .read()
                .ok()?
                .with_table_page(|table_page| table_page.get_tuples());
            self.tuple_index = 0;
            self.heap
                .buffer_pool_manager
                .lock()
                .ok()?
                .unpin_page(next_page_id, false)
                .ok()?;
        }
        if self.tuple_index >= self.tuples.len() {
            return None;
        }
        let tuple = Tuple::new(&self.tuples[self.tuple_index]);
        self.tuple_index += 1;
        Some(tuple)
    }
}

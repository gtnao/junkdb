use crate::{
    catalog::Schema,
    common::{PageID, INVALID_PAGE_ID, LSN, PAGE_SIZE, RID},
    tuple::Tuple,
    value::Value,
};

use super::{PageType, PAGE_ID_OFFSET, PAGE_ID_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE};

pub const B_PLUS_TREE_LEAF_PAGE_PAGE_TYPE: PageType = PageType(2);

const LSN_OFFSET: usize = PAGE_ID_OFFSET + PAGE_ID_SIZE;
const LSN_SIZE: usize = 8;
const PARENT_PAGE_ID_OFFSET: usize = LSN_OFFSET + LSN_SIZE;
const PARENT_PAGE_ID_SIZE: usize = 4;
const PREV_PAGE_ID_OFFSET: usize = PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE;
const PREV_PAGE_ID_SIZE: usize = 4;
const NEXT_PAGE_ID_OFFSET: usize = PREV_PAGE_ID_OFFSET + PREV_PAGE_ID_SIZE;
const NEXT_PAGE_ID_SIZE: usize = 4;
const LOWER_OFFSET_OFFSET: usize = NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE;
const LOWER_OFFSET_SIZE: usize = 4;
const UPPER_OFFSET_OFFSET: usize = LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE;
const UPPER_OFFSET_SIZE: usize = 4;
const HEADER_SIZE: usize = PAGE_TYPE_SIZE
    + PAGE_ID_SIZE
    + LSN_SIZE
    + PARENT_PAGE_ID_SIZE
    + PREV_PAGE_ID_SIZE
    + NEXT_PAGE_ID_SIZE
    + LOWER_OFFSET_SIZE
    + UPPER_OFFSET_SIZE;
const LINE_POINTER_OFFSET_SIZE: usize = 4;
const LINE_POINTER_SIZE_SIZE: usize = 4;
const LINE_POINTER_SIZE: usize = LINE_POINTER_OFFSET_SIZE + LINE_POINTER_SIZE_SIZE;

// RID
const VALUE_SIZE: usize = 8;

pub struct BPlusTreeLeafPage {
    pub data: Box<[u8]>,
}

impl BPlusTreeLeafPage {
    pub fn new(page_id: PageID, parent_page_id: PageID, prev_page_id: Option<PageID>) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];
        data[PAGE_TYPE_OFFSET..(PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE)]
            .copy_from_slice(&B_PLUS_TREE_LEAF_PAGE_PAGE_TYPE.0.to_le_bytes());
        data[PAGE_ID_OFFSET..(PAGE_ID_OFFSET + PAGE_ID_SIZE)]
            .copy_from_slice(&page_id.0.to_le_bytes());
        data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)]
            .copy_from_slice(&parent_page_id.0.to_le_bytes());
        if let Some(prev_page_id) = prev_page_id {
            data[PREV_PAGE_ID_OFFSET..(PREV_PAGE_ID_OFFSET + PREV_PAGE_ID_SIZE)]
                .copy_from_slice(&prev_page_id.0.to_le_bytes());
        } else {
            data[PREV_PAGE_ID_OFFSET..(PREV_PAGE_ID_OFFSET + PREV_PAGE_ID_SIZE)]
                .copy_from_slice(&INVALID_PAGE_ID.0.to_le_bytes());
        }
        data[NEXT_PAGE_ID_OFFSET..(NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE)]
            .copy_from_slice(&INVALID_PAGE_ID.0.to_le_bytes());
        data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
        data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        BPlusTreeLeafPage { data: data.into() }
    }
    pub fn from_data(data: &[u8]) -> Self {
        BPlusTreeLeafPage { data: data.into() }
    }

    pub fn lsn(&self) -> LSN {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.data[LSN_OFFSET..(LSN_OFFSET + LSN_SIZE)]);
        LSN(u64::from_le_bytes(buf))
    }
    pub fn set_lsn(&mut self, lsn: LSN) {
        self.data[LSN_OFFSET..(LSN_OFFSET + LSN_SIZE)].copy_from_slice(&lsn.0.to_le_bytes());
    }
    pub fn parent_page_id(&self) -> PageID {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)],
        );
        PageID(u32::from_le_bytes(buf))
    }
    pub fn set_parent_page_id(&mut self, parent_page_id: PageID) {
        self.data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)]
            .copy_from_slice(&parent_page_id.0.to_le_bytes());
    }
    pub fn prev_page_id(&self) -> PageID {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[PREV_PAGE_ID_OFFSET..(PREV_PAGE_ID_OFFSET + PREV_PAGE_ID_SIZE)],
        );
        PageID(u32::from_le_bytes(buf))
    }
    pub fn set_prev_page_id(&mut self, prev_page_id: PageID) {
        self.data[PREV_PAGE_ID_OFFSET..(PREV_PAGE_ID_OFFSET + PREV_PAGE_ID_SIZE)]
            .copy_from_slice(&prev_page_id.0.to_le_bytes());
    }
    pub fn next_page_id(&self) -> PageID {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[NEXT_PAGE_ID_OFFSET..(NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE)],
        );
        PageID(u32::from_le_bytes(buf))
    }
    pub fn set_next_page_id(&mut self, next_page_id: PageID) {
        self.data[NEXT_PAGE_ID_OFFSET..(NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE)]
            .copy_from_slice(&next_page_id.0.to_le_bytes());
    }
    pub fn lower_offset(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)],
        );
        u32::from_le_bytes(buf)
    }
    pub fn set_lower_offset(&mut self, lower_offset: u32) {
        self.data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&lower_offset.to_le_bytes());
    }
    pub fn upper_offset(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(
            &self.data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)],
        );
        u32::from_le_bytes(buf)
    }
    pub fn set_upper_offset(&mut self, upper_offset: u32) {
        self.data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&upper_offset.to_le_bytes());
    }
    pub fn line_pointer_offset(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * LINE_POINTER_SIZE;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[offset..(offset + LINE_POINTER_OFFSET_SIZE)]);
        u32::from_le_bytes(bytes)
    }
    pub fn line_pointer_size(&self, index: usize) -> u32 {
        let offset = HEADER_SIZE + index * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[offset..(offset + LINE_POINTER_SIZE_SIZE)]);
        u32::from_le_bytes(bytes)
    }
    pub fn num_line_pointers(&self) -> u32 {
        let lower_offset = self.lower_offset();
        (lower_offset - HEADER_SIZE as u32) / LINE_POINTER_SIZE as u32
    }

    pub fn key_at(&self, index: usize, schema: &Schema) -> Vec<Value> {
        let key_offset = self.line_pointer_offset(index) as usize + VALUE_SIZE;
        let key_size = self.line_pointer_size(index) as usize - VALUE_SIZE;
        let bytes = &self.data[key_offset..(key_offset + key_size)];
        Tuple::new(None, bytes).values(schema)
    }
    pub fn value_at(&self, index: usize) -> RID {
        let value_offset = self.line_pointer_offset(index) as usize;
        let page_id = PageID(u32::from_le_bytes([
            self.data[value_offset],
            self.data[value_offset + 1],
            self.data[value_offset + 2],
            self.data[value_offset + 3],
        ]));
        let tuple_index = u32::from_le_bytes([
            self.data[value_offset + 4],
            self.data[value_offset + 5],
            self.data[value_offset + 6],
            self.data[value_offset + 7],
        ]);
        RID(page_id, tuple_index)
    }
    // lower_bound
    pub fn key_index(&self, key: &[Value], schema: &Schema) -> usize {
        let mut ng = -1;
        let mut ok = self.num_line_pointers() as i32;
        while (ok - ng).abs() > 1 {
            let mid = (ok + ng) / 2;
            let mid_key = self.key_at(mid as usize, schema);
            let order = Value::compare_values(&mid_key, key).unwrap();
            if order.is_gt() || order.is_eq() {
                ok = mid;
            } else {
                ng = mid;
            }
        }
        ok as usize
    }
    // upper_bound
    pub fn key_index_upper(&self, key: &[Value], schema: &Schema) -> usize {
        let mut ng = -1;
        let mut ok = self.num_line_pointers() as i32;
        while (ok - ng).abs() > 1 {
            let mid = (ok + ng) / 2;
            let mid_key = self.key_at(mid as usize, schema);
            let order = Value::compare_values(&mid_key, key).unwrap();
            if order.is_gt() {
                ok = mid;
            } else {
                ng = mid;
            }
        }
        ok as usize
    }
    pub fn lookup(&self, key: &[Value], schema: &Schema) -> Option<Vec<RID>> {
        let index = self.key_index(key, schema);
        if index >= self.num_line_pointers() as usize {
            return None;
        }
        let mut rids = vec![];
        for i in index..self.num_line_pointers() as usize {
            let key_at = self.key_at(i, schema);
            if key
                .iter()
                .zip(key_at.iter())
                .any(|(k, k_at)| k.perform_not_equal(k_at).unwrap().is_true())
            {
                break;
            }
            rids.push(self.value_at(i));
        }
        if rids.is_empty() {
            return None;
        }
        Some(rids)
    }
    pub fn insert(&mut self, key: &[Value], value: RID, schema: &Schema) {
        let index = self.key_index_upper(key, schema);

        let key_bytes = Tuple::temp_tuple(key).data;
        let value_bytes = [value.0 .0.to_le_bytes(), value.1.to_le_bytes()]
            .concat()
            .into_boxed_slice();
        let new_entry = [value_bytes, key_bytes].concat();

        let mut entries = vec![];
        for i in 0..self.num_line_pointers() {
            let offset = self.line_pointer_offset(i as usize) as usize;
            let size = self.line_pointer_size(i as usize) as usize;
            let entry_bytes = self.data[offset..(offset + size)].to_vec();
            entries.push(entry_bytes);
        }
        entries.insert(index, new_entry);

        let mut current_offset = PAGE_SIZE;
        for (i, entry) in entries.iter().enumerate() {
            let size = entry.len() as u32;
            let offset_bytes = ((current_offset as u32) - size).to_le_bytes();
            let size_bytes = size.to_le_bytes();
            self.data[(HEADER_SIZE + i * LINE_POINTER_SIZE)
                ..(HEADER_SIZE + i * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE)]
                .copy_from_slice(&offset_bytes);
            self.data[(HEADER_SIZE + i * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE)
                ..(HEADER_SIZE + i * LINE_POINTER_SIZE + LINE_POINTER_SIZE)]
                .copy_from_slice(&size_bytes);
            self.data[current_offset - size as usize..current_offset].copy_from_slice(entry);
            current_offset -= size as usize;
        }
        self.set_upper_offset(current_offset as u32);
        self.set_lower_offset(HEADER_SIZE as u32 + entries.len() as u32 * LINE_POINTER_SIZE as u32);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::{Column, DataType},
        value::{integer::IntegerValue, varchar::VarcharValue},
    };

    use super::*;
    use anyhow::Result;
    use itertools::Itertools;

    #[test]
    fn test_insert_and_lookup() -> Result<()> {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
            ],
        };
        let values_list = vec![
            (
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                ],
                0,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                ],
                1,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                2,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                3,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                4,
            ),
        ];

        // tests all permutations of the values
        for indexes in (0..5).permutations(5) {
            let mut page = BPlusTreeLeafPage::new(PageID(0), INVALID_PAGE_ID, None);
            for i in indexes {
                page.insert(&values_list[i].0, RID(PageID(1), values_list[i].1), &schema);
            }

            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                ],
                &schema,
            );
            let mut expected = vec![RID(PageID(1), 0), RID(PageID(1), 1)];
            let mut actual = values.unwrap();
            expected.sort();
            actual.sort();
            assert_eq!(expected, actual);

            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                &schema,
            );
            assert_eq!(values, Some(vec![RID(PageID(1), 2),]));
            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(2)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                &schema,
            );
            assert_eq!(values, Some(vec![RID(PageID(1), 3),]));
            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                &schema,
            );
            assert_eq!(values, Some(vec![RID(PageID(1), 4),]));
            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(4)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                &schema,
            );
            assert_eq!(values, None);
            let values = page.lookup(
                &[
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("baz".to_string())),
                ],
                &schema,
            );
            assert_eq!(values, None);
        }
        Ok(())
    }
}

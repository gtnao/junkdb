use crate::{
    catalog::Schema,
    common::{PageID, LSN, PAGE_SIZE},
    tuple::Tuple,
    value::Value,
};

use super::{PageType, PAGE_ID_OFFSET, PAGE_ID_SIZE, PAGE_TYPE_OFFSET, PAGE_TYPE_SIZE};

pub const B_PLUS_TREE_INTERNAL_PAGE_PAGE_TYPE: PageType = PageType(3);

const LSN_OFFSET: usize = PAGE_ID_OFFSET + PAGE_ID_SIZE;
const LSN_SIZE: usize = 8;
const PARENT_PAGE_ID_OFFSET: usize = LSN_OFFSET + LSN_SIZE;
const PARENT_PAGE_ID_SIZE: usize = 4;
const LOWER_OFFSET_OFFSET: usize = PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE;
const LOWER_OFFSET_SIZE: usize = 4;
const UPPER_OFFSET_OFFSET: usize = LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE;
const UPPER_OFFSET_SIZE: usize = 4;
const HEADER_SIZE: usize = PAGE_TYPE_SIZE
    + PAGE_ID_SIZE
    + LSN_SIZE
    + PARENT_PAGE_ID_SIZE
    + LOWER_OFFSET_SIZE
    + UPPER_OFFSET_SIZE;
const LINE_POINTER_OFFSET_SIZE: usize = 4;
const LINE_POINTER_SIZE_SIZE: usize = 4;
const LINE_POINTER_SIZE: usize = LINE_POINTER_OFFSET_SIZE + LINE_POINTER_SIZE_SIZE;

// page_id
const VALUE_SIZE: usize = 4;

pub struct BPlusTreeInternalPage {
    pub data: Box<[u8]>,
}

impl BPlusTreeInternalPage {
    pub fn new(
        page_id: PageID,
        parent_page_id: PageID,
        left_page_id: PageID,
        right_page_id: PageID,
        risen_key: Tuple,
    ) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];
        data[PAGE_TYPE_OFFSET..(PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE)]
            .copy_from_slice(&B_PLUS_TREE_INTERNAL_PAGE_PAGE_TYPE.0.to_le_bytes());
        data[PAGE_ID_OFFSET..(PAGE_ID_OFFSET + PAGE_ID_SIZE)]
            .copy_from_slice(&page_id.0.to_le_bytes());
        data[PARENT_PAGE_ID_OFFSET..(PARENT_PAGE_ID_OFFSET + PARENT_PAGE_ID_SIZE)]
            .copy_from_slice(&parent_page_id.0.to_le_bytes());

        let mut lower_offset = HEADER_SIZE;
        let mut upper_offset = PAGE_SIZE;
        data[(upper_offset - VALUE_SIZE)..upper_offset]
            .copy_from_slice(&left_page_id.0.to_le_bytes());
        upper_offset -= VALUE_SIZE;
        data[lower_offset..(lower_offset + LINE_POINTER_OFFSET_SIZE)]
            .copy_from_slice(&(upper_offset as u32).to_le_bytes());
        data[(lower_offset + LINE_POINTER_OFFSET_SIZE)..(lower_offset + LINE_POINTER_SIZE)]
            .copy_from_slice(&(VALUE_SIZE as u32).to_le_bytes());
        lower_offset += LINE_POINTER_SIZE;
        let entry_bytes = [Box::new(right_page_id.0.to_le_bytes()), risen_key.data].concat();
        let entry_size = entry_bytes.len();
        data[(upper_offset - entry_size)..upper_offset].copy_from_slice(&entry_bytes);
        upper_offset -= entry_size;
        data[lower_offset..(lower_offset + LINE_POINTER_OFFSET_SIZE)]
            .copy_from_slice(&(upper_offset as u32).to_le_bytes());
        data[(lower_offset + LINE_POINTER_OFFSET_SIZE)..(lower_offset + LINE_POINTER_SIZE)]
            .copy_from_slice(&(entry_size as u32).to_le_bytes());
        lower_offset += LINE_POINTER_SIZE;
        data[LOWER_OFFSET_OFFSET..(LOWER_OFFSET_OFFSET + LOWER_OFFSET_SIZE)]
            .copy_from_slice(&(lower_offset as u32).to_le_bytes());
        data[UPPER_OFFSET_OFFSET..(UPPER_OFFSET_OFFSET + UPPER_OFFSET_SIZE)]
            .copy_from_slice(&(upper_offset as u32).to_le_bytes());

        BPlusTreeInternalPage { data: data.into() }
    }
    pub fn from_data(data: &[u8]) -> Self {
        BPlusTreeInternalPage { data: data.into() }
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

    pub fn key_at(&self, index: usize, schema: &Schema) -> Option<Vec<Value>> {
        if index == 0 {
            return None;
        }
        let key_offset = self.line_pointer_offset(index) as usize + VALUE_SIZE;
        let key_size = self.line_pointer_size(index) as usize - VALUE_SIZE;
        let bytes = &self.data[key_offset..(key_offset + key_size)];
        Some(Tuple::new(None, bytes).values(schema))
    }
    pub fn value_at(&self, index: usize) -> PageID {
        let value_offset = self.line_pointer_offset(index) as usize;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[value_offset..(value_offset + VALUE_SIZE)]);
        PageID(u32::from_le_bytes(bytes))
    }
    pub fn key_index(&self, key: &[Value], schema: &Schema) -> usize {
        let mut ng = self.num_line_pointers() as i32;
        let mut ok = 0;
        while (ok - ng).abs() > 1 {
            let mid = (ok + ng) / 2;
            if let Some(mid_key) = self.key_at(mid as usize, schema) {
                let order = Value::compare_values(&mid_key, key).unwrap();
                if order.is_gt() {
                    ng = mid;
                } else {
                    ok = mid;
                }
            } else {
                ok = mid;
            }
        }
        ok as usize
    }
    pub fn value_index(&self, value: PageID) -> usize {
        for i in 0..self.num_line_pointers() {
            if self.value_at(i as usize) == value {
                return i as usize;
            }
        }
        unreachable!()
    }
    pub fn lookup(&self, key: &[Value], schema: &Schema) -> PageID {
        let index = self.key_index(key, schema);
        if let Some(k) = self.key_at(index, schema) {
            let order = Value::compare_values(&k, key).unwrap();
            if !order.is_eq() {
                return self.value_at(index);
            }
        }

        for i in (0..index).rev() {
            if let Some(prev_key) = self.key_at(i, schema) {
                let order = Value::compare_values(&prev_key, key).unwrap();
                if !order.is_eq() {
                    return self.value_at(i + 1);
                }
            } else {
                return self.value_at(i + 1);
            }
        }
        self.value_at(0)
    }
    pub fn insert_after(&mut self, prev_value: PageID, key: &[Value], value: PageID) {
        let index = self.value_index(prev_value) + 1;

        let key_bytes = Tuple::temp_tuple(key).data;
        let value_bytes = Box::new(value.0.to_le_bytes());
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
            let offset_byttes = ((current_offset as u32) - size).to_le_bytes();
            let size_bytes = size.to_le_bytes();
            self.data[(HEADER_SIZE + i * LINE_POINTER_SIZE)
                ..(HEADER_SIZE + i * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE)]
                .copy_from_slice(&offset_byttes);
            self.data[(HEADER_SIZE + i * LINE_POINTER_SIZE + LINE_POINTER_OFFSET_SIZE)
                ..(HEADER_SIZE
                    + i * LINE_POINTER_SIZE
                    + LINE_POINTER_OFFSET_SIZE
                    + LINE_POINTER_SIZE_SIZE)]
                .copy_from_slice(&size_bytes);
            self.data[(current_offset - size as usize)..current_offset].copy_from_slice(entry);
            current_offset -= size as usize;
        }
        self.set_lower_offset((HEADER_SIZE + entries.len() * LINE_POINTER_SIZE) as u32);
        self.set_upper_offset(current_offset as u32);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::{Column, DataType},
        common::INVALID_PAGE_ID,
        value::{integer::IntegerValue, varchar::VarcharValue},
    };

    use super::*;
    use anyhow::Result;

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
                2,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("bar".to_string())),
                ],
                3,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(1)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                4,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(3)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                5,
            ),
            (
                vec![
                    Value::Integer(IntegerValue(4)),
                    Value::Varchar(VarcharValue("foo".to_string())),
                ],
                6,
            ),
        ];

        let mut page = BPlusTreeInternalPage::new(
            PageID(0),
            INVALID_PAGE_ID,
            PageID(1),
            PageID(values_list[0].1 as u32),
            Tuple::temp_tuple(&values_list[0].0),
        );
        for i in 1..5 {
            page.insert_after(
                PageID(values_list[i - 1].1 as u32),
                &values_list[i].0,
                PageID(values_list[i].1 as u32),
            );
        }

        let page_id = page.lookup(
            &[
                Value::Integer(IntegerValue(1)),
                Value::Varchar(VarcharValue("bar".to_string())),
            ],
            &schema,
        );
        assert_eq!(page_id, PageID(2));

        let page_id = page.lookup(
            &[
                Value::Integer(IntegerValue(1)),
                Value::Varchar(VarcharValue("aaa".to_string())),
            ],
            &schema,
        );
        assert_eq!(page_id, PageID(1));

        let page_id = page.lookup(
            &[
                Value::Integer(IntegerValue(0)),
                Value::Varchar(VarcharValue("bar".to_string())),
            ],
            &schema,
        );
        assert_eq!(page_id, PageID(1));

        let page_id = page.lookup(
            &[
                Value::Integer(IntegerValue(5)),
                Value::Varchar(VarcharValue("bar".to_string())),
            ],
            &schema,
        );
        assert_eq!(page_id, PageID(6));

        let page_id = page.lookup(
            &[
                Value::Integer(IntegerValue(3)),
                Value::Varchar(VarcharValue("aaa".to_string())),
            ],
            &schema,
        );
        assert_eq!(page_id, PageID(4));

        Ok(())
    }
}

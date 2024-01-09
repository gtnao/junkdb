use crate::{
    catalog::Schema,
    common::{TransactionID, RID},
    value::Value,
};

pub struct Tuple {
    pub rid: Option<RID>,
    pub data: Box<[u8]>,
}

const XMIN_OFFSET: usize = 0;
const XMIN_SIZE: usize = 8;
const XMAX_OFFSET: usize = XMIN_OFFSET + XMIN_SIZE;
const XMAX_SIZE: usize = 8;
const HEADER_SIZE: usize = XMAX_OFFSET + XMAX_SIZE;

impl Tuple {
    pub fn new(rid: Option<RID>, data: &[u8]) -> Tuple {
        Tuple {
            rid,
            data: data.into(),
        }
    }

    pub fn temp_tuple(values: &[Value]) -> Tuple {
        let bytes = Self::serialize(TransactionID(0), TransactionID(0), values);
        Tuple::new(None, &bytes)
    }

    pub fn xmin(&self) -> TransactionID {
        let mut bytes = [0; XMIN_SIZE];
        bytes.copy_from_slice(&self.data[XMIN_OFFSET..XMIN_OFFSET + XMIN_SIZE]);
        TransactionID(u64::from_le_bytes(bytes))
    }

    pub fn xmax(&self) -> TransactionID {
        let mut bytes = [0; XMAX_SIZE];
        bytes.copy_from_slice(&self.data[XMAX_OFFSET..XMAX_OFFSET + XMAX_SIZE]);
        TransactionID(u64::from_le_bytes(bytes))
    }

    pub fn set_xmax(&mut self, xmax: TransactionID) {
        self.data[XMAX_OFFSET..XMAX_OFFSET + XMAX_SIZE].copy_from_slice(&xmax.0.to_le_bytes());
    }

    pub fn values(&self, schema: &Schema) -> Vec<Value> {
        let mut values = vec![];
        let column_count = schema.columns.len();
        let mut offset = HEADER_SIZE + Self::null_bitmap_size(column_count);
        for (column, is_null) in schema.columns.iter().zip(self.null_bitmap(column_count)) {
            if is_null {
                values.push(Value::Null);
                continue;
            }
            let value = Value::deserialize(&column.data_type, &self.data[offset..]);
            offset += value.size();
            values.push(value);
        }
        values
    }

    pub fn serialize(xmin: TransactionID, xmax: TransactionID, values: &[Value]) -> Box<[u8]> {
        let values_bytes = values
            .iter()
            .map(|v| v.serialize().to_vec())
            .flatten()
            .collect::<Vec<u8>>();
        let mut bytes = Vec::with_capacity(
            HEADER_SIZE + Self::null_bitmap_size(values.len()) + values_bytes.len(),
        );
        bytes.extend_from_slice(&xmin.0.to_le_bytes());
        bytes.extend_from_slice(&xmax.0.to_le_bytes());
        bytes.extend_from_slice(&Self::serialize_null_bitmap(values));
        bytes.extend_from_slice(&values_bytes);
        bytes.into()
    }

    fn null_bitmap_size(column_count: usize) -> usize {
        (column_count + 7) / 8
    }

    fn null_bitmap(&self, column_count: usize) -> Vec<bool> {
        let mut res = vec![];
        let mut offset = HEADER_SIZE;
        for _ in 0..Self::null_bitmap_size(column_count) {
            let byte = self.data[offset];
            for i in 0..8 {
                res.push((byte & (1 << i)) != 0);
            }
            offset += 1;
        }
        res
    }

    fn serialize_null_bitmap(values: &[Value]) -> Box<[u8]> {
        let column_count = values.len();
        let mut res = vec![0; Self::null_bitmap_size(column_count)];
        for (i, value) in values.iter().enumerate() {
            if Value::Null == *value {
                let byte_index = i / 8;
                let bit_index = i % 8;
                res[byte_index] |= 1 << bit_index;
            }
        }
        res.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{Column, DataType},
        value::{IntegerValue, VarcharValue},
    };

    #[test]
    fn test_tuple() {
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
                Column {
                    name: "nullable".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                },
            ],
        };
        let values = vec![
            Value::Integer(IntegerValue(1)),
            Value::Varchar(VarcharValue("foo".to_string())),
            Value::Null,
            Value::Integer(IntegerValue(20)),
        ];
        let tuple_data = Tuple::serialize(TransactionID(1), TransactionID(2), &values);
        let tuple = Tuple::new(None, &tuple_data);
        assert_eq!(tuple.xmin(), TransactionID(1));
        assert_eq!(tuple.xmax(), TransactionID(2));
        assert_eq!(
            tuple.values(&schema),
            vec![
                Value::Integer(IntegerValue(1)),
                Value::Varchar(VarcharValue("foo".to_string())),
                Value::Null,
                Value::Integer(IntegerValue(20))
            ]
        );
    }

    #[test]
    fn test_ten_columns_with_nulls() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "c1".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c2".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c3".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c4".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c5".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c6".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c7".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c8".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c9".to_string(),
                    data_type: DataType::Integer,
                },
                Column {
                    name: "c10".to_string(),
                    data_type: DataType::Integer,
                },
            ],
        };
        let values = vec![
            Value::Integer(IntegerValue(1)),
            Value::Null,
            Value::Null,
            Value::Integer(IntegerValue(1)),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(IntegerValue(1)),
        ];
        let tuple_data = Tuple::serialize(TransactionID(1), TransactionID(2), &values);
        let tuple = Tuple::new(None, &tuple_data);
        assert_eq!(tuple.xmin(), TransactionID(1));
        assert_eq!(tuple.xmax(), TransactionID(2));
        assert_eq!(
            tuple.values(&schema),
            vec![
                Value::Integer(IntegerValue(1)),
                Value::Null,
                Value::Null,
                Value::Integer(IntegerValue(1)),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Integer(IntegerValue(1)),
            ]
        );
    }
}

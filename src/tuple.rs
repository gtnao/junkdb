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
        let mut offset = HEADER_SIZE;
        for column in &schema.columns {
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
        let mut bytes = Vec::with_capacity(HEADER_SIZE + values_bytes.len());
        bytes.extend_from_slice(&xmin.0.to_le_bytes());
        bytes.extend_from_slice(&xmax.0.to_le_bytes());
        bytes.extend_from_slice(&values_bytes);
        bytes.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{Column, DataType},
        value::{IntValue, VarcharValue},
    };

    #[test]
    fn test_tuple() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "age".to_string(),
                    data_type: DataType::Int,
                },
            ],
        };
        let values = vec![
            Value::Int(IntValue(1)),
            Value::Varchar(VarcharValue("foo".to_string())),
            Value::Int(IntValue(20)),
        ];
        let tuple_data = Tuple::serialize(TransactionID(1), TransactionID(2), &values);
        let tuple = Tuple::new(None, &tuple_data);
        assert_eq!(tuple.xmin(), TransactionID(1));
        assert_eq!(tuple.xmax(), TransactionID(2));
        assert_eq!(
            tuple.values(&schema),
            vec![
                Value::Int(IntValue(1)),
                Value::Varchar(VarcharValue("foo".to_string())),
                Value::Int(IntValue(20))
            ]
        );
    }
}

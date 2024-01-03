use crate::catalog::{DataType, Schema};

#[derive(Debug, PartialEq)]
pub enum Value {
    Int(IntValue),
    Varchar(VarcharValue),
}

impl Value {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Value::Int(v) => v.0.to_be_bytes().to_vec(),
            Value::Varchar(v) => {
                let bytes = v.0.as_bytes().to_vec();
                let size = bytes.len() as u32;
                let mut size_bytes = size.to_be_bytes().to_vec();
                size_bytes.extend(bytes);
                size_bytes
            }
        }
    }
    pub fn deserialize(data_type: &DataType, bytes: &[u8]) -> Self {
        match data_type {
            DataType::Int => {
                let mut buffer = [0u8; 4];
                buffer.copy_from_slice(bytes);
                Value::Int(IntValue(i32::from_be_bytes(buffer)))
            }
            DataType::Varchar => {
                let mut buffer = [0u8; 4];
                buffer.copy_from_slice(&bytes[0..4]);
                let size = u32::from_be_bytes(buffer) as usize;
                let mut buffer = vec![0u8; size];
                buffer.copy_from_slice(&bytes[4..(4 + size)]);
                // TODO: remove unwrap
                Value::Varchar(VarcharValue(String::from_utf8(buffer).unwrap()))
            }
        }
    }
    pub fn deserialize_values(schema: &Schema, bytes: &[u8]) -> Vec<Self> {
        let mut values = vec![];
        let mut offset = 0;
        for column in &schema.columns {
            let size = match column.data_type {
                DataType::Int => 4,
                DataType::Varchar => {
                    let mut buffer = [0u8; 4];
                    buffer.copy_from_slice(&bytes[offset..(offset + 4)]);
                    u32::from_be_bytes(buffer) as usize + 4
                }
            };
            let value = Value::deserialize(&column.data_type, &bytes[offset..(offset + size)]);
            values.push(value);
            offset += size;
        }
        values
    }
}

#[derive(Debug, PartialEq)]
pub struct IntValue(pub i32);

#[derive(Debug, PartialEq)]
pub struct VarcharValue(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_int() {
        let value = Value::Int(IntValue(123));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 123]);
    }

    #[test]
    fn test_serialize_varchar() {
        let value = Value::Varchar(VarcharValue(String::from("foobar")));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 6, 102, 111, 111, 98, 97, 114]);
    }

    #[test]
    fn test_deserialize_int() {
        let bytes = vec![0, 0, 0, 123];
        let value = Value::deserialize(&DataType::Int, &bytes);
        assert_eq!(value, Value::Int(IntValue(123)));
    }

    #[test]
    fn test_deserialize_varchar() {
        let bytes = vec![0, 0, 0, 6, 102, 111, 111, 98, 97, 114];
        let value = Value::deserialize(&DataType::Varchar, &bytes);
        assert_eq!(value, Value::Varchar(VarcharValue(String::from("foobar"))));
    }
}

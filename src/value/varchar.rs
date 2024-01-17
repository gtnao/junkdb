use anyhow::Result;

use crate::catalog::DataType;

use super::{boolean::BooleanValue, integer::IntegerValue, Value};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct VarcharValue(pub String);

impl From<&[u8]> for VarcharValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 4);
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        let size = u32::from_be_bytes(buffer) as usize;
        let mut buffer = vec![0u8; size];
        buffer.copy_from_slice(&bytes[4..(4 + size)]);
        if let Ok(string) = String::from_utf8(buffer) {
            VarcharValue(string)
        } else {
            panic!("Invalid UTF-8 sequence")
        }
    }
}

impl VarcharValue {
    pub fn serialize(&self) -> Box<[u8]> {
        let bytes = self.0.as_bytes().to_vec();
        let size = bytes.len() as u32;
        let mut size_bytes = size.to_be_bytes().to_vec();
        size_bytes.extend(bytes);
        size_bytes.into()
    }
    pub fn size(&self) -> usize {
        4 + self.0.len()
    }

    pub fn convert_to(&self, data_type: &DataType) -> Result<Value> {
        match data_type {
            DataType::Integer => Ok(Value::Integer(IntegerValue(0))),
            DataType::Varchar => Ok(Value::Varchar(self.clone())),
            DataType::Boolean => Ok(Value::Boolean(BooleanValue(false))),
        }
    }

    pub fn perform_equal(&self, other: &VarcharValue) -> BooleanValue {
        BooleanValue(self.0 == other.0)
    }
    pub fn perform_not_equal(&self, other: &VarcharValue) -> BooleanValue {
        BooleanValue(self.0 != other.0)
    }
}

use crate::catalog::DataType;

use super::{
    big_integer::BigIntegerValue, boolean::BooleanValue, integer::IntegerValue,
    unsigned_big_integer::UnsignedBigIntegerValue, unsigned_integer::UnsignedIntegerValue, Value,
};

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

    pub fn convert_to(&self, data_type: &DataType) -> Option<Value> {
        match data_type {
            DataType::Integer => self
                .0
                .parse::<i32>()
                .ok()
                .map(|v| Value::Integer(IntegerValue(v))),
            DataType::UnsignedInteger => self
                .0
                .parse::<u32>()
                .ok()
                .map(|v| Value::UnsignedInteger(UnsignedIntegerValue(v))),
            DataType::BigInteger => self
                .0
                .parse::<i64>()
                .ok()
                .map(|v| Value::BigInteger(BigIntegerValue(v))),
            DataType::UnsignedBigInteger => self
                .0
                .parse::<u64>()
                .ok()
                .map(|v| Value::UnsignedBigInteger(UnsignedBigIntegerValue(v))),
            DataType::Varchar => Some(Value::Varchar(self.clone())),
            DataType::Boolean => self
                .0
                .parse::<bool>()
                .ok()
                .map(|v| Value::Boolean(BooleanValue(v))),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> bool {
        match other {
            Value::Varchar(other) => self.0 == other.0,
            _ => false,
        }
    }
}

use crate::catalog::DataType;

use super::{
    big_integer::BigIntegerValue, boolean::BooleanValue, integer::IntegerValue,
    unsigned_integer::UnsignedIntegerValue, varchar::VarcharValue, Value,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct UnsignedBigIntegerValue(pub u64);

impl From<&[u8]> for UnsignedBigIntegerValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 8);
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(&bytes[0..8]);
        UnsignedBigIntegerValue(u64::from_be_bytes(buffer))
    }
}

impl UnsignedBigIntegerValue {
    pub fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    pub fn size(&self) -> usize {
        8
    }

    pub fn convert_to(&self, data_type: &DataType) -> Option<Value> {
        match data_type {
            DataType::Integer => {
                if self.0 <= i32::MAX as u64 {
                    Some(Value::Integer(IntegerValue(self.0 as i32)))
                } else {
                    None
                }
            }
            DataType::UnsignedInteger => {
                if self.0 <= u32::MAX as u64 {
                    Some(Value::UnsignedInteger(UnsignedIntegerValue(self.0 as u32)))
                } else {
                    None
                }
            }
            DataType::BigInteger => Some(Value::BigInteger(BigIntegerValue(self.0 as i64))),
            DataType::UnsignedBigInteger => {
                Some(Value::UnsignedBigInteger(UnsignedBigIntegerValue(self.0)))
            }
            DataType::Varchar => Some(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Some(Value::Boolean(BooleanValue(self.0 != 0))),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => other.0 >= 0 && self.0 == other.0 as u64,
            Value::UnsignedInteger(other) => self.0 == other.0 as u64,
            Value::BigInteger(other) => other.0 >= 0 && self.0 == other.0 as u64,
            Value::UnsignedBigInteger(other) => self.0 == other.0,
            _ => false,
        }
    }
}

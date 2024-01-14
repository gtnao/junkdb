use crate::catalog::DataType;

use super::{
    big_integer::BigIntegerValue, boolean::BooleanValue, integer::IntegerValue,
    unsigned_big_integer::UnsignedBigIntegerValue, varchar::VarcharValue, Value,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct UnsignedIntegerValue(pub u32);

impl From<&[u8]> for UnsignedIntegerValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 4);
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        UnsignedIntegerValue(u32::from_be_bytes(buffer))
    }
}

impl UnsignedIntegerValue {
    pub fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    pub fn size(&self) -> usize {
        4
    }

    pub fn convert_to(&self, data_type: &DataType) -> Option<Value> {
        match data_type {
            DataType::Integer => {
                if self.0 <= i32::MAX as u32 {
                    Some(Value::Integer(IntegerValue(self.0 as i32)))
                } else {
                    None
                }
            }
            DataType::UnsignedInteger => Some(Value::UnsignedInteger(UnsignedIntegerValue(self.0))),
            DataType::BigInteger => Some(Value::BigInteger(BigIntegerValue(self.0 as i64))),
            DataType::UnsignedBigInteger => Some(Value::UnsignedBigInteger(
                UnsignedBigIntegerValue(self.0 as u64),
            )),
            DataType::Varchar => Some(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Some(Value::Boolean(BooleanValue(self.0 != 0))),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => other.0 >= 0 && self.0 == other.0 as u32,
            Value::UnsignedInteger(other) => self.0 == other.0,
            Value::BigInteger(other) => other.0 >= 0 && self.0 as i64 == other.0,
            Value::UnsignedBigInteger(other) => self.0 as u64 == other.0,
            _ => false,
        }
    }
}

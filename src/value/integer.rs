use crate::catalog::DataType;

use super::{
    big_integer::BigIntegerValue, boolean::BooleanValue,
    unsigned_big_integer::UnsignedBigIntegerValue, unsigned_integer::UnsignedIntegerValue,
    varchar::VarcharValue, Value,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct IntegerValue(pub i32);

impl From<&[u8]> for IntegerValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 4);
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        IntegerValue(i32::from_be_bytes(buffer))
    }
}

impl IntegerValue {
    pub fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    pub fn size(&self) -> usize {
        4
    }

    pub fn convert_to(&self, data_type: &DataType) -> Option<Value> {
        match data_type {
            DataType::Integer => Some(Value::Integer(IntegerValue(self.0))),
            DataType::UnsignedInteger => {
                if self.0 >= 0 {
                    Some(Value::UnsignedInteger(UnsignedIntegerValue(self.0 as u32)))
                } else {
                    None
                }
            }
            DataType::BigInteger => Some(Value::BigInteger(BigIntegerValue(self.0 as i64))),
            DataType::UnsignedBigInteger => {
                if self.0 >= 0 {
                    Some(Value::UnsignedBigInteger(UnsignedBigIntegerValue(
                        self.0 as u64,
                    )))
                } else {
                    None
                }
            }
            DataType::Varchar => Some(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Some(Value::Boolean(BooleanValue(self.0 != 0))),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 == other.0,
            Value::UnsignedInteger(other) => self.0 >= 0 && self.0 as u32 == other.0,
            Value::BigInteger(other) => self.0 as i64 == other.0,
            Value::UnsignedBigInteger(other) => self.0 >= 0 && self.0 as u64 == other.0,
            _ => false,
        }
    }
    pub fn perform_not_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 != other.0,
            Value::UnsignedInteger(other) => self.0 < 0 || self.0 as u32 != other.0,
            Value::BigInteger(other) => self.0 as i64 != other.0,
            Value::UnsignedBigInteger(other) => self.0 < 0 || self.0 as u64 != other.0,
            _ => true,
        }
    }
    pub fn perform_less_than(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 < other.0,
            Value::UnsignedInteger(other) => self.0 < 0 || (self.0 as u32) < other.0,
            Value::BigInteger(other) => (self.0 as i64) < other.0,
            Value::UnsignedBigInteger(other) => self.0 < 0 || (self.0 as u64) < other.0,
            _ => false,
        }
    }
    pub fn perform_less_than_or_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 <= other.0,
            Value::UnsignedInteger(other) => self.0 < 0 || (self.0 as u32) <= other.0,
            Value::BigInteger(other) => (self.0 as i64) <= other.0,
            Value::UnsignedBigInteger(other) => self.0 < 0 || (self.0 as u64) <= other.0,
            _ => false,
        }
    }
    pub fn perform_greater_than(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 > other.0,
            Value::UnsignedInteger(other) => self.0 >= 0 && (self.0 as u32) > other.0,
            Value::BigInteger(other) => (self.0 as i64) > other.0,
            Value::UnsignedBigInteger(other) => self.0 >= 0 && (self.0 as u64) > other.0,
            _ => false,
        }
    }
    pub fn perform_greater_than_or_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 >= other.0,
            Value::UnsignedInteger(other) => self.0 >= 0 && (self.0 as u32) >= other.0,
            Value::BigInteger(other) => (self.0 as i64) >= other.0,
            Value::UnsignedBigInteger(other) => self.0 >= 0 && (self.0 as u64) >= other.0,
            _ => false,
        }
    }
}

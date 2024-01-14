use crate::catalog::DataType;

use super::{
    big_integer::BigIntegerValue, integer::IntegerValue,
    unsigned_big_integer::UnsignedBigIntegerValue, unsigned_integer::UnsignedIntegerValue,
    varchar::VarcharValue, Value,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BooleanValue(pub bool);

impl From<&[u8]> for BooleanValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 1);
        BooleanValue(bytes[0] != 0)
    }
}

impl BooleanValue {
    pub fn serialize(&self) -> Box<[u8]> {
        let bytes = vec![if self.0 { 1 } else { 0 }];
        bytes.into()
    }
    pub fn size(&self) -> usize {
        1
    }

    pub fn convert_to(&self, data_type: &DataType) -> Option<Value> {
        match data_type {
            DataType::Integer => Some(Value::Integer(IntegerValue(if self.0 { 1 } else { 0 }))),
            DataType::UnsignedInteger => {
                Some(Value::UnsignedInteger(UnsignedIntegerValue(if self.0 {
                    1
                } else {
                    0
                })))
            }
            DataType::BigInteger => Some(Value::BigInteger(BigIntegerValue(if self.0 {
                1
            } else {
                0
            }))),
            DataType::UnsignedBigInteger => Some(Value::UnsignedBigInteger(
                UnsignedBigIntegerValue(if self.0 { 1 } else { 0 }),
            )),
            DataType::Varchar => Some(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Some(Value::Boolean(BooleanValue(self.0))),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> bool {
        match other {
            Value::Integer(other) => self.0 == (other.0 != 0),
            Value::UnsignedInteger(other) => self.0 == (other.0 != 0),
            Value::BigInteger(other) => self.0 == (other.0 != 0),
            Value::UnsignedBigInteger(other) => self.0 == (other.0 != 0),
            Value::Boolean(other) => self.0 == other.0,
            _ => false,
        }
    }
}

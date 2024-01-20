use anyhow::Result;

use crate::catalog::DataType;

use super::{integer::IntegerValue, varchar::VarcharValue, Value};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BooleanValue(pub bool);

impl From<&[u8]> for BooleanValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(!bytes.is_empty());
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

    pub fn convert_to(&self, data_type: &DataType) -> Result<Value> {
        match data_type {
            DataType::Integer => Ok(Value::Integer(IntegerValue(if self.0 { 1 } else { 0 }))),
            DataType::Varchar => Ok(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Ok(Value::Boolean(BooleanValue(self.0))),
        }
    }

    pub fn perform_not(&self) -> BooleanValue {
        BooleanValue(!self.0)
    }
    pub fn perform_and(&self, other: &BooleanValue) -> BooleanValue {
        BooleanValue(self.0 && other.0)
    }
    pub fn perform_or(&self, other: &BooleanValue) -> BooleanValue {
        BooleanValue(self.0 || other.0)
    }
}

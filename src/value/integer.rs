use anyhow::Result;

use crate::catalog::DataType;

use super::{boolean::BooleanValue, varchar::VarcharValue, Value};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct IntegerValue(pub i64);

impl From<&[u8]> for IntegerValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 8);
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(&bytes[0..8]);
        IntegerValue(i64::from_be_bytes(buffer))
    }
}

impl IntegerValue {
    pub fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    pub fn size(&self) -> usize {
        8
    }

    pub fn convert_to(&self, data_type: &DataType) -> Result<Value> {
        match data_type {
            DataType::Integer => Ok(Value::Integer(IntegerValue(self.0))),
            DataType::Varchar => Ok(Value::Varchar(VarcharValue(self.0.to_string()))),
            DataType::Boolean => Ok(Value::Boolean(BooleanValue(self.0 != 0))),
        }
    }

    pub fn perform_negate(&self) -> Result<IntegerValue> {
        self.0.checked_neg().map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_add(&self, other: &IntegerValue) -> Result<IntegerValue> {
        self.0.checked_add(other.0).map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_subtract(&self, other: &IntegerValue) -> Result<IntegerValue> {
        self.0.checked_sub(other.0).map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_multiply(&self, other: &IntegerValue) -> Result<IntegerValue> {
        self.0.checked_mul(other.0).map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_divide(&self, other: &IntegerValue) -> Result<IntegerValue> {
        self.0.checked_div(other.0).map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_modulo(&self, other: &IntegerValue) -> Result<IntegerValue> {
        self.0.checked_rem(other.0).map_or_else(
            || Err(anyhow::anyhow!("Integer overflow")),
            |value| Ok(IntegerValue(value)),
        )
    }
    pub fn perform_equal(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 == other.0)
    }
    pub fn perform_not_equal(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 != other.0)
    }
    pub fn perform_less_than(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 < other.0)
    }
    pub fn perform_less_than_or_equal(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 <= other.0)
    }
    pub fn perform_greater_than(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 > other.0)
    }
    pub fn perform_greater_than_or_equal(&self, other: &IntegerValue) -> BooleanValue {
        BooleanValue(self.0 >= other.0)
    }
}

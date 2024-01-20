use std::fmt::{Display, Formatter};

use anyhow::{anyhow, Result};

use crate::catalog::DataType;

use self::{boolean::BooleanValue, integer::IntegerValue, varchar::VarcharValue};

pub mod boolean;
pub mod integer;
pub mod varchar;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Value {
    Integer(IntegerValue),
    Varchar(VarcharValue),
    Boolean(BooleanValue),
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(value) => write!(f, "{}", value.0),
            Value::Varchar(value) => write!(f, "{}", value.0),
            Value::Boolean(value) => write!(f, "{}", value.0),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Value {
    pub fn serialize(&self) -> Box<[u8]> {
        match self {
            Value::Integer(value) => value.serialize(),
            Value::Varchar(value) => value.serialize(),
            Value::Boolean(value) => value.serialize(),
            Value::Null => vec![].into(),
        }
    }
    pub fn size(&self) -> usize {
        match self {
            Value::Integer(value) => value.size(),
            Value::Varchar(value) => value.size(),
            Value::Boolean(value) => value.size(),
            Value::Null => 0,
        }
    }
    pub fn deserialize(data_type: &DataType, bytes: &[u8]) -> Self {
        match data_type {
            DataType::Integer => Value::Integer(IntegerValue::from(bytes)),
            DataType::Varchar => Value::Varchar(VarcharValue::from(bytes)),
            DataType::Boolean => Value::Boolean(BooleanValue::from(bytes)),
        }
    }

    pub fn convert_to(&self, data_type: &DataType) -> Result<Self> {
        match self {
            Value::Integer(value) => value.convert_to(data_type),
            Value::Varchar(value) => value.convert_to(data_type),
            Value::Boolean(value) => value.convert_to(data_type),
            Value::Null => Err(anyhow!("Cannot convert NULL to {:?}", data_type)),
        }
    }

    // unary operators
    pub fn perform_is_null(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Boolean(BooleanValue(true))),
            _ => Ok(Value::Boolean(BooleanValue(false))),
        }
    }
    pub fn perform_is_not_null(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Boolean(BooleanValue(false))),
            _ => Ok(Value::Boolean(BooleanValue(true))),
        }
    }
    pub fn perform_not(&self) -> Result<Value> {
        if let Value::Null = self {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Boolean)? {
            Value::Boolean(value) => Ok(Value::Boolean(value.perform_not())),
            _ => unreachable!(),
        }
    }
    pub fn perform_negate(&self) -> Result<Value> {
        if self.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => Ok(Value::Integer(value.perform_negate()?)),
            _ => unreachable!(),
        }
    }

    pub fn perform_equal(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match (self, other) {
            (Value::Varchar(value), Value::Varchar(other_value)) => {
                Ok(Value::Boolean(value.perform_equal(other_value)))
            }
            _ => match self.convert_to(&DataType::Integer)? {
                Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                    Value::Integer(other_value) => {
                        Ok(Value::Boolean(value.perform_equal(&other_value)))
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
        }
    }
    pub fn perform_not_equal(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match (self, other) {
            (Value::Varchar(value), Value::Varchar(other_value)) => {
                Ok(Value::Boolean(value.perform_not_equal(other_value)))
            }
            _ => match self.convert_to(&DataType::Integer)? {
                Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                    Value::Integer(other_value) => {
                        Ok(Value::Boolean(value.perform_not_equal(&other_value)))
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
        }
    }
    pub fn perform_less_than(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Boolean(value.perform_less_than(&other_value)))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_less_than_or_equal(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => Ok(Value::Boolean(
                    value.perform_less_than_or_equal(&other_value),
                )),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_greater_than(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Boolean(value.perform_greater_than(&other_value)))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_greater_than_or_equal(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => Ok(Value::Boolean(
                    value.perform_greater_than_or_equal(&other_value),
                )),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    // binary operators
    pub fn perform_add(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => Ok(Value::Integer(value.perform_add(&other_value)?)),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_subtract(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Integer(value.perform_subtract(&other_value)?))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_multiply(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Integer(value.perform_multiply(&other_value)?))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_divide(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Integer(value.perform_divide(&other_value)?))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_modulo(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Integer)? {
            Value::Integer(value) => match other.convert_to(&DataType::Integer)? {
                Value::Integer(other_value) => {
                    Ok(Value::Integer(value.perform_modulo(&other_value)?))
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_and(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Boolean)? {
            Value::Boolean(value) => match other.convert_to(&DataType::Boolean)? {
                Value::Boolean(other_value) => Ok(Value::Boolean(value.perform_and(&other_value))),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
    pub fn perform_or(&self, other: &Value) -> Result<Value> {
        if self.is_null_value() || other.is_null_value() {
            return Ok(Value::Null);
        }
        match self.convert_to(&DataType::Boolean)? {
            Value::Boolean(value) => match other.convert_to(&DataType::Boolean)? {
                Value::Boolean(other_value) => Ok(Value::Boolean(value.perform_or(&other_value))),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn is_null_value(&self) -> bool {
        matches!(self, Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use crate::value::integer::IntegerValue;

    use super::*;

    #[test]
    fn test_serialize_integer() {
        let value = Value::Integer(IntegerValue(-123));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![255, 255, 255, 255, 255, 255, 255, 133].into());
    }

    #[test]
    fn test_serialize_varchar() {
        let value = Value::Varchar(VarcharValue(String::from("foobar")));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 6, 102, 111, 111, 98, 97, 114].into());
    }

    #[test]
    fn test_serialize_boolean() {
        let value = Value::Boolean(BooleanValue(true));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![1].into());
        let value = Value::Boolean(BooleanValue(false));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0].into());
    }

    #[test]
    fn test_deserialize_integer() {
        let bytes = vec![255, 255, 255, 255, 255, 255, 255, 133];
        let value = Value::deserialize(&DataType::Integer, &bytes);
        assert_eq!(value, Value::Integer(IntegerValue(-123)));
    }

    #[test]
    fn test_deserialize_varchar() {
        let bytes = vec![0, 0, 0, 6, 102, 111, 111, 98, 97, 114];
        let value = Value::deserialize(&DataType::Varchar, &bytes);
        assert_eq!(value, Value::Varchar(VarcharValue(String::from("foobar"))));
    }

    #[test]
    fn test_deserialize_boolean() {
        let bytes = vec![1];
        let value = Value::deserialize(&DataType::Boolean, &bytes);
        assert_eq!(value, Value::Boolean(BooleanValue(true)));
        let bytes = vec![0];
        let value = Value::deserialize(&DataType::Boolean, &bytes);
        assert_eq!(value, Value::Boolean(BooleanValue(false)));
    }

    #[test]
    fn test_size_integer() {
        let value = Value::Integer(IntegerValue(123));
        assert_eq!(value.size(), 8);
    }

    #[test]
    fn test_size_varchar() {
        let value = Value::Varchar(VarcharValue(String::from("foobar")));
        assert_eq!(value.size(), 10);
    }

    #[test]
    fn test_size_boolean() {
        let value = Value::Boolean(BooleanValue(true));
        assert_eq!(value.size(), 1);
    }

    #[test]
    fn test_display() {
        let value = Value::Integer(IntegerValue(-123));
        assert_eq!(value.to_string(), "-123");
        let value = Value::Varchar(VarcharValue(String::from("foobar")));
        assert_eq!(value.to_string(), "foobar");
        let value = Value::Boolean(BooleanValue(true));
        assert_eq!(value.to_string(), "true");
        let value = Value::Boolean(BooleanValue(false));
        assert_eq!(value.to_string(), "false");
        let value = Value::Null;
        assert_eq!(value.to_string(), "NULL");
    }
}

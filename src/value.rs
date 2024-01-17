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
    pub fn perform_eq(&self, other: &Self) -> bool {
        match self {
            Value::Integer(value) => value.perform_equal(other),
            Value::Varchar(value) => value.perform_equal(other),
            Value::Boolean(value) => value.perform_equal(other),
            Value::Null => false,
        }
    }
    pub fn convert_to(&self, data_type: &DataType) -> Result<Self> {
        match self {
            Value::Integer(value) => value.convert_to(data_type),
            Value::Varchar(value) => value.convert_to(data_type),
            Value::Boolean(value) => value.convert_to(data_type),
            Value::Null => Some(Value::Null),
        }
        .ok_or(anyhow!("Cannot convert {:?} to {:?}", self, data_type))
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

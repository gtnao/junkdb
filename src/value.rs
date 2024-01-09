use std::fmt::{Display, Formatter};

use anyhow::{anyhow, Result};

use crate::catalog::DataType;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Value {
    Integer(IntegerValue),
    UnsignedInteger(UnsignedIntegerValue),
    BigInteger(BigIntegerValue),
    UnsignedBigInteger(UnsignedBigIntegerValue),
    Varchar(VarcharValue),
    Boolean(BooleanValue),
    Null,
}
impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(value) => write!(f, "{}", value.0),
            Value::UnsignedInteger(value) => write!(f, "{}", value.0),
            Value::BigInteger(value) => write!(f, "{}", value.0),
            Value::UnsignedBigInteger(value) => write!(f, "{}", value.0),
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
            Value::UnsignedInteger(value) => value.serialize(),
            Value::BigInteger(value) => value.serialize(),
            Value::UnsignedBigInteger(value) => value.serialize(),
            Value::Varchar(value) => value.serialize(),
            Value::Boolean(value) => value.serialize(),
            Value::Null => vec![].into(),
        }
    }
    pub fn size(&self) -> usize {
        match self {
            Value::Integer(value) => value.size(),
            Value::UnsignedInteger(value) => value.size(),
            Value::BigInteger(value) => value.size(),
            Value::UnsignedBigInteger(value) => value.size(),
            Value::Varchar(value) => value.size(),
            Value::Boolean(value) => value.size(),
            Value::Null => 0,
        }
    }
    pub fn deserialize(data_type: &DataType, bytes: &[u8]) -> Self {
        match data_type {
            DataType::Integer => Value::Integer(IntegerValue::from(bytes)),
            DataType::UnsignedInteger => Value::UnsignedInteger(UnsignedIntegerValue::from(bytes)),
            DataType::BigInteger => Value::BigInteger(BigIntegerValue::from(bytes)),
            DataType::UnsignedBigInteger => {
                Value::UnsignedBigInteger(UnsignedBigIntegerValue::from(bytes))
            }
            DataType::Varchar => Value::Varchar(VarcharValue::from(bytes)),
            DataType::Boolean => Value::Boolean(BooleanValue::from(bytes)),
        }
    }
    pub fn perform_eq(&self, other: &Self) -> bool {
        if self == &Value::Null || other == &Value::Null {
            return false;
        }
        match (self, other) {
            (Value::Integer(value), Value::Integer(other)) => value == other,
            (Value::Integer(value), Value::UnsignedInteger(other)) => {
                value.0 >= 0 && value.0 as u32 == other.0
            }
            (Value::Integer(value), Value::BigInteger(other)) => value.0 as i64 == other.0,
            (Value::Integer(value), Value::UnsignedBigInteger(other)) => {
                value.0 >= 0 && value.0 as u64 == other.0
            }
            (Value::UnsignedInteger(value), Value::UnsignedInteger(other)) => value == other,
            (Value::UnsignedInteger(value), Value::Integer(other)) => {
                other.0 >= 0 && value.0 == other.0 as u32
            }
            (Value::UnsignedInteger(value), Value::BigInteger(other)) => value.0 as i64 == other.0,
            (Value::UnsignedInteger(value), Value::UnsignedBigInteger(other)) => {
                value.0 as u64 == other.0
            }
            (Value::BigInteger(value), Value::BigInteger(other)) => value == other,
            (Value::BigInteger(value), Value::Integer(other)) => value.0 == other.0 as i64,
            (Value::BigInteger(value), Value::UnsignedInteger(other)) => value.0 == other.0 as i64,
            (Value::BigInteger(value), Value::UnsignedBigInteger(other)) => {
                value.0 >= 0 && value.0 as u64 == other.0
            }
            (Value::UnsignedBigInteger(value), Value::UnsignedBigInteger(other)) => value == other,
            (Value::UnsignedBigInteger(value), Value::Integer(other)) => {
                other.0 >= 0 && value.0 == other.0 as u64
            }
            (Value::UnsignedBigInteger(value), Value::UnsignedInteger(other)) => {
                value.0 == other.0 as u64
            }
            (Value::UnsignedBigInteger(value), Value::BigInteger(other)) => {
                other.0 >= 0 && value.0 == other.0 as u64
            }
            (Value::Varchar(value), Value::Varchar(other)) => value == other,
            (Value::Boolean(value), Value::Boolean(other)) => value == other,
            _ => false,
        }
    }
    pub fn convert_to(&self, data_type: &DataType) -> Result<Self> {
        match self {
            Value::Integer(value) => match data_type {
                DataType::Integer => Ok(Value::Integer(value.clone())),
                DataType::UnsignedInteger => {
                    if value.0 >= 0 {
                        Ok(Value::UnsignedInteger(UnsignedIntegerValue(value.0 as u32)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert integer to unsigned integer"
                        ))
                    }
                }
                DataType::BigInteger => Ok(Value::BigInteger(BigIntegerValue(value.0 as i64))),
                DataType::UnsignedBigInteger => Ok(Value::UnsignedBigInteger(if value.0 >= 0 {
                    UnsignedBigIntegerValue(value.0 as u64)
                } else {
                    UnsignedBigIntegerValue((value.0 as i64).wrapping_neg() as u64)
                })),
                _ => Err(anyhow!("Cannot convert integer to {:?}", data_type)),
            },
            Value::UnsignedInteger(value) => match data_type {
                DataType::Integer => {
                    if value.0 <= i32::MAX as u32 {
                        Ok(Value::Integer(IntegerValue(value.0 as i32)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert unsigned integer to integer"
                        ))
                    }
                }
                DataType::UnsignedInteger => Ok(Value::UnsignedInteger(value.clone())),
                DataType::BigInteger => Ok(Value::BigInteger(BigIntegerValue(value.0 as i64))),
                DataType::UnsignedBigInteger => Ok(Value::UnsignedBigInteger(
                    UnsignedBigIntegerValue(value.0 as u64),
                )),
                _ => Err(anyhow!(
                    "Cannot convert unsigned integer to {:?}",
                    data_type
                )),
            },
            Value::BigInteger(value) => match data_type {
                DataType::Integer => {
                    if value.0 >= i32::MIN as i64 && value.0 <= i32::MAX as i64 {
                        Ok(Value::Integer(IntegerValue(value.0 as i32)))
                    } else {
                        Err(anyhow::anyhow!("Cannot convert big integer to integer"))
                    }
                }
                DataType::UnsignedInteger => {
                    if value.0 >= 0 && value.0 <= u32::MAX as i64 {
                        Ok(Value::UnsignedInteger(UnsignedIntegerValue(value.0 as u32)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert big integer to unsigned integer"
                        ))
                    }
                }
                DataType::BigInteger => Ok(Value::BigInteger(value.clone())),
                DataType::UnsignedBigInteger => {
                    if value.0 >= 0 {
                        Ok(Value::UnsignedBigInteger(UnsignedBigIntegerValue(
                            value.0 as u64,
                        )))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert big integer to unsigned big integer"
                        ))
                    }
                }
                _ => Err(anyhow!("Cannot convert big integer to {:?}", data_type)),
            },
            Value::UnsignedBigInteger(value) => match data_type {
                DataType::Integer => {
                    if value.0 <= i32::MAX as u64 {
                        Ok(Value::Integer(IntegerValue(value.0 as i32)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert unsigned big integer to integer"
                        ))
                    }
                }
                DataType::UnsignedInteger => {
                    if value.0 <= u32::MAX as u64 {
                        Ok(Value::UnsignedInteger(UnsignedIntegerValue(value.0 as u32)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert unsigned big integer to unsigned integer"
                        ))
                    }
                }
                DataType::BigInteger => {
                    if value.0 <= i64::MAX as u64 {
                        Ok(Value::BigInteger(BigIntegerValue(value.0 as i64)))
                    } else {
                        Err(anyhow::anyhow!(
                            "Cannot convert unsigned big integer to big integer"
                        ))
                    }
                }
                DataType::UnsignedBigInteger => Ok(Value::UnsignedBigInteger(value.clone())),
                _ => Err(anyhow!(
                    "Cannot convert unsigned big integer to {:?}",
                    data_type
                )),
            },
            Value::Varchar(value) => match data_type {
                DataType::Varchar => Ok(Value::Varchar(value.clone())),
                _ => Err(anyhow!("Cannot convert varchar to {:?}", data_type)),
            },
            Value::Boolean(value) => match data_type {
                DataType::Boolean => Ok(Value::Boolean(value.clone())),
                _ => Err(anyhow!("Cannot convert boolean to {:?}", data_type)),
            },
            Value::Null => Ok(Value::Null),
        }
    }
}

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
    fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    fn size(&self) -> usize {
        4
    }
}

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
    fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    fn size(&self) -> usize {
        4
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BigIntegerValue(pub i64);
impl From<&[u8]> for BigIntegerValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 8);
        let mut buffer = [0u8; 8];
        buffer.copy_from_slice(&bytes[0..8]);
        BigIntegerValue(i64::from_be_bytes(buffer))
    }
}
impl BigIntegerValue {
    fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    fn size(&self) -> usize {
        8
    }
}

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
    fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    fn size(&self) -> usize {
        8
    }
}

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
    fn serialize(&self) -> Box<[u8]> {
        let bytes = self.0.as_bytes().to_vec();
        let size = bytes.len() as u32;
        let mut size_bytes = size.to_be_bytes().to_vec();
        size_bytes.extend(bytes);
        size_bytes.into()
    }
    fn size(&self) -> usize {
        4 + self.0.len()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BooleanValue(pub bool);
impl From<&[u8]> for BooleanValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 1);
        BooleanValue(bytes[0] != 0)
    }
}
impl BooleanValue {
    fn serialize(&self) -> Box<[u8]> {
        let bytes = vec![if self.0 { 1 } else { 0 }];
        bytes.into()
    }
    fn size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_integer() {
        let value = Value::Integer(IntegerValue(-123));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![255, 255, 255, 133].into());
    }

    #[test]
    fn test_serialize_unsigned_integer() {
        let value = Value::UnsignedInteger(UnsignedIntegerValue(123));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 123].into());
    }

    #[test]
    fn test_serialize_big_integer() {
        let value = Value::BigInteger(BigIntegerValue(-3_000_000_000));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![255, 255, 255, 255, 77, 47, 162, 0].into());
    }

    #[test]
    fn test_serialize_unsigned_big_integer() {
        let value = Value::UnsignedBigInteger(UnsignedBigIntegerValue(5_000_000_000));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 1, 42, 5, 242, 0].into());
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
        let bytes = vec![255, 255, 255, 133];
        let value = Value::deserialize(&DataType::Integer, &bytes);
        assert_eq!(value, Value::Integer(IntegerValue(-123)));
    }

    #[test]
    fn test_deserialize_unsigned_integer() {
        let bytes = vec![0, 0, 0, 123];
        let value = Value::deserialize(&DataType::UnsignedInteger, &bytes);
        assert_eq!(value, Value::UnsignedInteger(UnsignedIntegerValue(123)));
    }

    #[test]
    fn test_deserialize_big_integer() {
        let bytes = vec![255, 255, 255, 255, 77, 47, 162, 0];
        let value = Value::deserialize(&DataType::BigInteger, &bytes);
        assert_eq!(value, Value::BigInteger(BigIntegerValue(-3_000_000_000)));
    }

    #[test]
    fn test_deserialize_unsigned_big_integer() {
        let bytes = vec![0, 0, 0, 1, 42, 5, 242, 0];
        let value = Value::deserialize(&DataType::UnsignedBigInteger, &bytes);
        assert_eq!(
            value,
            Value::UnsignedBigInteger(UnsignedBigIntegerValue(5_000_000_000))
        );
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
        assert_eq!(value.size(), 4);
    }

    #[test]
    fn test_size_unsigned_integer() {
        let value = Value::UnsignedInteger(UnsignedIntegerValue(123));
        assert_eq!(value.size(), 4);
    }

    #[test]
    fn test_size_big_integer() {
        let value = Value::BigInteger(BigIntegerValue(123));
        assert_eq!(value.size(), 8);
    }

    #[test]
    fn test_size_unsigned_big_integer() {
        let value = Value::UnsignedBigInteger(UnsignedBigIntegerValue(123));
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
        let value = Value::UnsignedInteger(UnsignedIntegerValue(123));
        assert_eq!(value.to_string(), "123");
        let value = Value::BigInteger(BigIntegerValue(-3_000_000_000));
        assert_eq!(value.to_string(), "-3000000000");
        let value = Value::UnsignedBigInteger(UnsignedBigIntegerValue(5_000_000_000));
        assert_eq!(value.to_string(), "5000000000");
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

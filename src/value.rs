use crate::catalog::DataType;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Value {
    Int(IntValue),
    Varchar(VarcharValue),
    Boolean(BooleanValue),
}
impl Value {
    pub fn serialize(&self) -> Box<[u8]> {
        match self {
            Value::Int(value) => value.serialize(),
            Value::Varchar(value) => value.serialize(),
            Value::Boolean(value) => value.serialize(),
        }
    }
    pub fn size(&self) -> usize {
        match self {
            Value::Int(value) => value.size(),
            Value::Varchar(value) => value.size(),
            Value::Boolean(value) => value.size(),
        }
    }
    pub fn deserialize(data_type: &DataType, bytes: &[u8]) -> Self {
        match data_type {
            DataType::Int => Value::Int(IntValue::from(bytes)),
            DataType::Varchar => Value::Varchar(VarcharValue::from(bytes)),
            DataType::Boolean => Value::Boolean(BooleanValue::from(bytes)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct IntValue(pub i32);
impl From<&[u8]> for IntValue {
    fn from(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 4);
        let mut buffer = [0u8; 4];
        buffer.copy_from_slice(&bytes[0..4]);
        IntValue(i32::from_be_bytes(buffer))
    }
}
impl IntValue {
    fn serialize(&self) -> Box<[u8]> {
        self.0.to_be_bytes().into()
    }
    fn size(&self) -> usize {
        4
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
        // TODO: remove unwrap
        VarcharValue(String::from_utf8(buffer).unwrap())
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
        let buffer = bytes[0];
        BooleanValue(buffer != 0)
    }
}
impl BooleanValue {
    fn serialize(&self) -> Box<[u8]> {
        let buffer = if self.0 { 1 } else { 0 };
        vec![buffer].into()
    }
    fn size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_int() {
        let value = Value::Int(IntValue(123));
        let bytes = value.serialize();
        assert_eq!(bytes, vec![0, 0, 0, 123].into());
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
    fn test_deserialize_int() {
        let bytes = vec![0, 0, 0, 123];
        let value = Value::deserialize(&DataType::Int, &bytes);
        assert_eq!(value, Value::Int(IntValue(123)));
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
    fn test_size_int() {
        let value = Value::Int(IntValue(123));
        assert_eq!(value.size(), 4);
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
}

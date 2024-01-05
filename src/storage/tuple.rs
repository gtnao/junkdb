use crate::common::TransactionID;

pub struct Tuple {
    pub data: Box<[u8]>,
}

const XMIN_OFFSET: usize = 0;
const XMIN_SIZE: usize = 8;
const XMAX_OFFSET: usize = XMIN_OFFSET + XMIN_SIZE;
const XMAX_SIZE: usize = 8;
const TUPLE_HEADER_SIZE: usize = XMAX_OFFSET + XMAX_SIZE;

impl Tuple {
    pub fn new(data: Box<[u8]>) -> Tuple {
        Tuple { data }
    }

    pub fn xmin(&self) -> TransactionID {
        let mut bytes = [0; XMIN_SIZE];
        bytes.copy_from_slice(&self.data[XMIN_OFFSET..XMIN_OFFSET + XMIN_SIZE]);
        TransactionID(u64::from_le_bytes(bytes))
    }

    pub fn xmax(&self) -> TransactionID {
        let mut bytes = [0; XMAX_SIZE];
        bytes.copy_from_slice(&self.data[XMAX_OFFSET..XMAX_OFFSET + XMAX_SIZE]);
        TransactionID(u64::from_le_bytes(bytes))
    }

    pub fn serialize(xmin: TransactionID, xmax: TransactionID, values_data: &[u8]) -> Box<[u8]> {
        let mut bytes = Vec::with_capacity(TUPLE_HEADER_SIZE + values_data.len());
        bytes.extend_from_slice(&xmin.0.to_le_bytes());
        bytes.extend_from_slice(&xmax.0.to_le_bytes());
        bytes.extend_from_slice(values_data);
        bytes.into_boxed_slice()
    }

    pub fn values_data(&self) -> &[u8] {
        &self.data[TUPLE_HEADER_SIZE..]
    }
}

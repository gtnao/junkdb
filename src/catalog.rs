pub struct Schema {
    pub columns: Vec<Column>,
}
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}
pub enum DataType {
    Int,
    Varchar,
}

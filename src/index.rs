use anyhow::Result;

use crate::{common::PageID, value::Value};

#[derive(Debug)]
pub struct Index {
    pub id: i64,
    pub name: String,
    pub table_name: String,
    pub first_page_id: PageID,
    pub columns: Vec<String>,
}

impl Index {
    pub fn from_system_table(values: Vec<Value>) -> Result<Self> {
        let id = if let Value::Integer(id) = &values[0] {
            id.0
        } else {
            return Err(anyhow::anyhow!("Invalid id"));
        };
        let name = if let Value::Varchar(name) = &values[1] {
            name.0.clone()
        } else {
            return Err(anyhow::anyhow!("Invalid name"));
        };
        let table_name = if let Value::Varchar(table_name) = &values[2] {
            table_name.0.clone()
        } else {
            return Err(anyhow::anyhow!("Invalid table_name"));
        };
        let first_page_id = if let Value::Integer(first_page_id) = &values[3] {
            PageID(first_page_id.0 as u32)
        } else {
            return Err(anyhow::anyhow!("Invalid first_page_id"));
        };
        Ok(Self {
            id,
            name,
            table_name,
            first_page_id,
            columns: Vec::new(),
        })
    }
    pub fn add_columns(&mut self, column: String) {
        self.columns.push(column);
    }
}

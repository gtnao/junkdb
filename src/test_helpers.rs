use anyhow::Result;
use tempfile::tempdir;

use crate::{
    catalog::DataType,
    instance::Instance,
    parser::{CreateTableStatementAST, TableElementAST},
};

pub fn setup_test_database() -> Result<Instance> {
    let dir = tempdir()?;
    let data_dir = dir.path().join("data");
    let instance = Instance::new(data_dir.to_str().unwrap(), true)?;
    let txn_id = instance.begin(None)?;
    let create_table_ast = CreateTableStatementAST {
        table_name: "t1".to_string(),
        elements: vec![
            TableElementAST {
                column_name: "c1".to_string(),
                data_type: DataType::Integer,
            },
            TableElementAST {
                column_name: "c2".to_string(),
                data_type: DataType::Varchar,
            },
        ],
    };
    instance.create_table(&create_table_ast, txn_id)?;
    instance.commit(txn_id)?;
    Ok(instance)
}

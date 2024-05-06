use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    buffer::BufferPoolManager,
    catalog::{Catalog, Schema},
    common::{PageID, RID},
    value::Value,
};

#[derive(Debug)]
pub struct Index {
    pub id: i64,
    pub name: String,
    pub table_name: String,
    pub first_page_id: PageID,
    pub columns: Vec<String>,
    pub schema: Schema,
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
            schema: Schema {
                columns: Vec::new(),
            },
        })
    }
    // TODO: set by catalog
    pub fn add_columns(&mut self, column: String) {
        self.columns.push(column);
    }
    // TODO: set by catalog
    pub fn set_schema(&mut self, schema: Schema) {
        let columns = self.columns.iter().map(|column| {
            schema
                .columns
                .iter()
                .find(|c| c.name == *column)
                .expect("column not found")
                .clone()
        });
        self.schema = Schema {
            columns: columns.collect(),
        };
    }
}

pub struct IndexManager {
    index: Index,
    catalog: Arc<Mutex<Catalog>>,
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
}
impl IndexManager {
    pub fn new(
        index: Index,
        catalog: Arc<Mutex<Catalog>>,
        buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    ) -> Self {
        Self {
            index,
            catalog,
            buffer_pool_manager,
        }
    }
    pub fn lookup(&self, key: &Value) -> Result<Option<Vec<RID>>> {
        let leaf_page_id = self.find_leaf_page(key)?;
        let page = self
            .buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .fetch_page(leaf_page_id)?;
        let rid = page
            .read()
            .map_err(|_| anyhow::anyhow!("read error"))?
            .with_b_plus_tree_leaf_page(|leaf_page| {
                leaf_page.lookup(&[key.clone()], &self.index.schema)
            });
        Ok(rid)
    }
    pub fn insert(&self, key: &Value, rid: RID) -> Result<()> {
        self.insert_into_leaf_page(key, rid)?;
        Ok(())
    }
    fn insert_into_leaf_page(&self, key: &Value, rid: RID) -> Result<()> {
        let leaf_page_id = self.find_leaf_page(key)?;
        let page = self
            .buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .fetch_page(leaf_page_id)?;
        // TODO: write WAL
        page.write()
            .map_err(|_| anyhow::anyhow!("write error"))?
            .with_b_plus_tree_leaf_page_mut(|leaf_page| {
                // TODO: full check
                leaf_page.insert(&[key.clone()], rid, &self.index.schema)
            });
        self.buffer_pool_manager
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .unpin_page(leaf_page_id, true)?;
        Ok(())
    }
    fn find_leaf_page(&self, key: &Value) -> Result<PageID> {
        let page_id = self.index.first_page_id;
        loop {
            let page = self
                .buffer_pool_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .fetch_page(page_id)?;
            if page
                .read()
                .map_err(|_| anyhow::anyhow!("read error"))?
                .is_b_plus_tree_leaf()
            {
                self.buffer_pool_manager
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lock error"))?
                    .unpin_page(page_id, false)?;
                return Ok(page_id);
            }
            // TODO: lookup internal page
            self.buffer_pool_manager
                .lock()
                .map_err(|_| anyhow::anyhow!("lock error"))?
                .unpin_page(page_id, false)?;
        }
    }
}

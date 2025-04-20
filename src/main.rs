use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::Result;

struct PageManager {
    file: File,
}

impl PageManager {
    fn load<T: AsRef<Path>>(file_path: T) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_path.as_ref())?,
        })
    }
    fn bootstrap<T: AsRef<Path>>(file_path: T) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_path.as_ref())?,
        })
    }
}

fn main() -> Result<()> {
    let page_manager = PageManager::bootstrap("example.db")?;
    let page_manager2 = PageManager::load("example.db")?;
    Ok(())
}

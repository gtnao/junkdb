use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

use anyhow::Result;

const PAGE_SIZE: u32 = 4096;

pub struct DiskManager {
    data_file: File,
    next_page_id: u32,
}

impl DiskManager {
    pub fn new(data_file_path: &str) -> Result<Self> {
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path)?;
        let size = data_file.metadata()?.len();
        let next_page_id = (size / PAGE_SIZE as u64) as u32;
        Ok(Self {
            data_file,
            next_page_id,
        })
    }
    pub fn read_page(&mut self, page_id: u32, data: &mut [u8]) -> Result<()> {
        assert!(data.len() == PAGE_SIZE as usize);
        let offset = page_id * PAGE_SIZE;
        self.data_file.seek(SeekFrom::Start(offset as u64))?;
        self.data_file.read_exact(data)?;
        Ok(())
    }
    pub fn write_page(&mut self, page_id: u32, data: &[u8]) -> Result<()> {
        assert!(data.len() == PAGE_SIZE as usize);
        let offset = page_id * PAGE_SIZE;
        self.data_file.seek(SeekFrom::Start(offset as u64))?;
        self.data_file.write_all(data)?;
        self.data_file.sync_all()?;
        Ok(())
    }
    pub fn allocate_page(&mut self) -> Result<u32> {
        self.data_file.seek(SeekFrom::End(0))?;
        self.data_file.write_all(&[0; PAGE_SIZE as usize])?;
        self.data_file.sync_all()?;
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read() -> Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;

        let write_data1 = vec![1; PAGE_SIZE as usize];
        disk_manager.write_page(0, &write_data1)?;
        let write_data2 = vec![2; PAGE_SIZE as usize];
        disk_manager.write_page(1, &write_data2)?;

        let mut read_data1 = vec![0; PAGE_SIZE as usize];
        disk_manager.read_page(0, &mut read_data1)?;
        let mut read_data2 = vec![0; PAGE_SIZE as usize];
        disk_manager.read_page(1, &mut read_data2)?;
        assert_eq!(read_data1, vec![1; PAGE_SIZE as usize]);
        assert_eq!(read_data2, vec![2; PAGE_SIZE as usize]);

        Ok(())
    }

    #[test]
    fn test_fail_read() -> Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;

        let mut read_data = vec![0; PAGE_SIZE as usize];
        let result = disk_manager.read_page(0, &mut read_data);
        assert!(result.is_err());

        let result = disk_manager.read_page(1, &mut read_data);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_file_exists() -> Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;

        let write_data = vec![1; PAGE_SIZE as usize];
        disk_manager.write_page(0, &write_data)?;

        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let mut read_data = vec![0; PAGE_SIZE as usize];
        disk_manager.read_page(0, &mut read_data)?;
        assert_eq!(read_data, vec![1; PAGE_SIZE as usize]);

        Ok(())
    }

    #[test]
    fn test_allocate_page() -> Result<()> {
        let dir = tempdir()?;
        let data_file_path = dir.path().join("data");
        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;

        let page_id1 = disk_manager.allocate_page()?;
        let page_id2 = disk_manager.allocate_page()?;
        assert_eq!(page_id1, 0);
        assert_eq!(page_id2, 1);

        let mut disk_manager = DiskManager::new(data_file_path.to_str().unwrap())?;
        let page_id3 = disk_manager.allocate_page()?;
        assert_eq!(page_id3, 2);

        Ok(())
    }
}

use anyhow::Result;
use std::{
    collections::{hash_map::Entry, HashMap},
    fs::{create_dir_all, read_dir, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::common::{PageID, PAGE_SIZE};

type SegmentNo = u64;
struct PageAddr {
    segment_no: SegmentNo,
    offset: u64,
}

#[cfg(not(test))]
const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB
#[cfg(test)]
const SEGMENT_SIZE: usize = 8 * PAGE_SIZE; // 8 KiB
const PAGES_PER_SEGMENT: u64 = (SEGMENT_SIZE / PAGE_SIZE) as u64;

pub struct SegmentManager {
    base_dir: PathBuf,
    fd_table: HashMap<SegmentNo, File>,
}

impl SegmentManager {
    pub fn new<T: AsRef<Path>>(base_dir: T) -> Result<Self> {
        Ok(Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            fd_table: HashMap::new(),
        })
    }
    pub fn bootstrap<T: AsRef<Path>>(base_dir: T) -> Result<Self> {
        create_dir_all(&base_dir)?;
        Self::new(base_dir)
    }
    pub fn read_into(&mut self, page_id: PageID, output: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let addr = Self::addr(page_id);
        self.open_segment(addr.segment_no)?
            .seek(SeekFrom::Start(addr.offset))?;
        self.open_segment(addr.segment_no)?.read_exact(output)?;
        Ok(())
    }
    pub fn write_from(&mut self, page_id: PageID, input: &[u8; PAGE_SIZE]) -> Result<()> {
        let addr = Self::addr(page_id);
        self.open_segment(addr.segment_no)?
            .seek(SeekFrom::Start(addr.offset))?;
        self.open_segment(addr.segment_no)?.write_all(input)?;
        Ok(())
    }
    pub fn allocate_page(&mut self) -> Result<PageID> {
        let page_id = self.next_page_id()?;
        self.write_from(page_id, &[0; PAGE_SIZE])?;
        Ok(page_id)
    }

    fn addr(page_id: PageID) -> PageAddr {
        let segment_no = page_id / PAGES_PER_SEGMENT;
        let offset = (page_id % PAGES_PER_SEGMENT) * PAGE_SIZE as u64;
        PageAddr { segment_no, offset }
    }
    fn open_segment(&mut self, segment_no: SegmentNo) -> Result<&mut File> {
        match self.fd_table.entry(segment_no) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(v) => {
                let path = self.base_dir.join(format!("seg_{segment_no:06}"));
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)?;
                Ok(v.insert(file))
            }
        }
    }
    fn next_page_id(&mut self) -> Result<PageID> {
        let mut segment_no = 0;
        read_dir(&self.base_dir)?.for_each(|entry| {
            if let Ok(e) = entry {
                if let Ok(name) = e.file_name().into_string() {
                    if name.starts_with("seg_") {
                        let n: u64 = name[4..10].parse().unwrap_or(0);
                        segment_no = segment_no.max(n);
                    }
                }
            }
        });
        let file = self.open_segment(segment_no)?;
        let len = file.metadata()?.len();
        let used_pages = len / PAGE_SIZE as u64;
        Ok(segment_no * PAGES_PER_SEGMENT + used_pages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read_dir;
    use tempfile::tempdir;
    #[test]
    fn write_then_read_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let mut sm = SegmentManager::bootstrap(dir.path())?;

        let page_id = sm.allocate_page()?;

        let mut write_buf = [0u8; PAGE_SIZE];
        for (i, b) in write_buf.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        sm.write_from(page_id, &write_buf)?;

        let mut read_buf = [0u8; PAGE_SIZE];
        sm.read_into(page_id, &mut read_buf)?;
        assert_eq!(write_buf, read_buf);

        Ok(())
    }
    #[test]
    fn allocate_returns_zeroed_page() -> Result<()> {
        let dir = tempdir()?;
        let mut sm = SegmentManager::bootstrap(dir.path())?;

        let pid = sm.allocate_page()?;
        let mut buf = [0u8; PAGE_SIZE];
        sm.read_into(pid, &mut buf)?;

        assert!(buf.iter().all(|b| *b == 0));
        Ok(())
    }
    #[test]
    fn next_page_id_monotonic() -> Result<()> {
        let dir = tempdir()?;
        let mut sm = SegmentManager::bootstrap(dir.path())?;

        let first = sm.allocate_page()?;
        let second = sm.allocate_page()?;
        let third = sm.allocate_page()?;

        assert_eq!(second, first + 1);
        assert_eq!(third, first + 2);
        Ok(())
    }
    #[test]
    fn allocate_crosses_segment_boundary() -> Result<()> {
        let dir = tempdir()?;
        let mut sm = SegmentManager::bootstrap(dir.path())?;

        let first_pid = sm.allocate_page()?;
        assert_eq!(first_pid, 0);
        for _ in 1..PAGES_PER_SEGMENT {
            sm.allocate_page()?;
        }

        let cross_pid = sm.allocate_page()?;
        assert_eq!(cross_pid, PAGES_PER_SEGMENT);

        let seg_files = read_dir(dir.path())?
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|d| d.file_name().into_string().ok())
                    .map(|s| s.starts_with("seg_"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(seg_files, 2);

        Ok(())
    }
    #[test]
    fn read_write_across_segments() -> Result<()> {
        let dir = tempdir()?;
        let mut sm = SegmentManager::bootstrap(dir.path())?;

        let last_pid = PAGES_PER_SEGMENT - 1;
        let first_pid2 = PAGES_PER_SEGMENT;

        let buf_a = [0xAAu8; PAGE_SIZE];
        let buf_b = [0xBBu8; PAGE_SIZE];

        sm.write_from(last_pid, &buf_a)?;
        sm.write_from(first_pid2, &buf_b)?;

        let mut read_a = [0u8; PAGE_SIZE];
        let mut read_b = [0u8; PAGE_SIZE];

        sm.read_into(last_pid, &mut read_a)?;
        sm.read_into(first_pid2, &mut read_b)?;

        assert_eq!(buf_a, read_a);
        assert_eq!(buf_b, read_b);

        Ok(())
    }
}

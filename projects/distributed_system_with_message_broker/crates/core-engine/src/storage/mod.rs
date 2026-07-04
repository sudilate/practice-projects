use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    pub offset: u64,
    pub length: u32,
}

pub struct Wal {
    file: File,
    index: Vec<LogPosition>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        Ok(Self {
            file,
            index: Vec::new(),
        })
    }

    pub fn append(&mut self, bytes: &[u8]) -> io::Result<LogPosition> {
        let length = u32::try_from(bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "WAL record exceeds u32 length")
        })?;
        let offset = self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&length.to_be_bytes())?;
        self.file.write_all(bytes)?;
        self.file.sync_data()?;

        let position = LogPosition { offset, length };
        self.index.push(position);
        Ok(position)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

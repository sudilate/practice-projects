use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    pub offset: u64,
    pub length: u32,
}

#[derive(Debug)]
pub struct Wal {
    file: File,
    index: Vec<LogPosition>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let index = rebuild_index(&mut file)?;

        Ok(Self { file, index })
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

    pub fn get(&mut self, index: usize) -> io::Result<Option<Vec<u8>>> {
        let Some(position) = self.index.get(index) else {
            return Ok(None);
        };

        self.file.seek(SeekFrom::Start(position.offset + 4))?;
        let mut payload = vec![0; position.length as usize];
        self.file.read_exact(&mut payload)?;
        Ok(Some(payload))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

fn rebuild_index(file: &mut File) -> io::Result<Vec<LogPosition>> {
    let mut index = Vec::new();
    let file_len = file.metadata()?.len();
    let mut offset = 0;

    while offset < file_len {
        if file_len - offset < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL contains truncated record header",
            ));
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut length_bytes = [0; 4];
        file.read_exact(&mut length_bytes)?;
        let length = u32::from_be_bytes(length_bytes);
        let next_offset = offset + 4 + u64::from(length);

        if next_offset > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL contains truncated record payload",
            ));
        }

        index.push(LogPosition { offset, length });
        offset = next_offset;
    }

    file.seek(SeekFrom::End(0))?;
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::Wal;
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn creates_empty_wal() {
        let path = test_wal_path("empty");
        let wal = Wal::open(&path).expect("WAL opens");

        assert!(wal.is_empty());
        assert_eq!(wal.len(), 0);

        cleanup(path);
    }

    #[test]
    fn appends_one_record() {
        let path = test_wal_path("one-record");
        let mut wal = Wal::open(&path).expect("WAL opens");

        let position = wal.append(b"first").expect("record appends");

        assert_eq!(position.offset, 0);
        assert_eq!(position.length, 5);
        assert_eq!(wal.len(), 1);
        assert_eq!(wal.get(0).expect("read succeeds"), Some(b"first".to_vec()));

        cleanup(path);
    }

    #[test]
    fn appends_multiple_records() {
        let path = test_wal_path("multiple-records");
        let mut wal = Wal::open(&path).expect("WAL opens");

        let first = wal.append(b"first").expect("first appends");
        let second = wal.append(b"second").expect("second appends");

        assert_eq!(first.offset, 0);
        assert_eq!(second.offset, 9);
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(0).expect("read succeeds"), Some(b"first".to_vec()));
        assert_eq!(wal.get(1).expect("read succeeds"), Some(b"second".to_vec()));
        assert_eq!(wal.get(2).expect("read succeeds"), None);

        cleanup(path);
    }

    #[test]
    fn rebuilds_index_when_reopened() {
        let path = test_wal_path("reopen");
        {
            let mut wal = Wal::open(&path).expect("WAL opens");
            wal.append(b"first").expect("first appends");
            wal.append(b"second").expect("second appends");
        }

        let mut reopened = Wal::open(&path).expect("WAL reopens");

        assert_eq!(reopened.len(), 2);
        assert_eq!(
            reopened.get(0).expect("read succeeds"),
            Some(b"first".to_vec())
        );
        assert_eq!(
            reopened.get(1).expect("read succeeds"),
            Some(b"second".to_vec())
        );

        cleanup(path);
    }

    #[test]
    fn rejects_truncated_record_header() {
        let path = test_wal_path("truncated-header");
        let mut file = File::create(&path).expect("file creates");
        file.write_all(&[0, 0]).expect("truncated header writes");

        let error = Wal::open(&path).expect_err("truncated WAL is rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        cleanup(path);
    }

    #[test]
    fn rejects_truncated_record_payload() {
        let path = test_wal_path("truncated-payload");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .expect("file opens");
        file.write_all(&5_u32.to_be_bytes()).expect("length writes");
        file.write_all(b"abc").expect("partial payload writes");

        let error = Wal::open(&path).expect_err("truncated WAL is rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        cleanup(path);
    }

    fn test_wal_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is valid")
            .as_nanos();
        std::env::temp_dir().join(format!("core-engine-{name}-{nanos}.log"))
    }

    fn cleanup(path: PathBuf) {
        let _ = fs::remove_file(path);
    }
}

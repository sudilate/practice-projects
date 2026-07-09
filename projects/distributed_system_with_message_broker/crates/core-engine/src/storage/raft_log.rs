use crate::raft::RaftLogEntry;
use crate::storage::Wal;
use std::io;
use std::path::Path;

/// Durable Raft log backed by the existing WAL. Each WAL record is a
/// serialized `RaftLogEntry`:
///
/// ```text
/// [term:u64_be][index:u64_be][task_id_len:u16_be][task_id:N][payload_len:u32_be][payload:M]
/// ```
#[derive(Debug)]
pub struct RaftLog {
    wal: Wal,
}

impl RaftLog {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            wal: Wal::open(path)?,
        })
    }

    pub fn append(&mut self, entry: &RaftLogEntry) -> io::Result<()> {
        let mut record = Vec::new();
        record.extend_from_slice(&entry.term.to_be_bytes());
        record.extend_from_slice(&entry.index.to_be_bytes());
        let task_id_len = u16::try_from(entry.task_id.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "task id too long"))?;
        record.extend_from_slice(&task_id_len.to_be_bytes());
        record.extend_from_slice(entry.task_id.as_bytes());
        let payload_len = u32::try_from(entry.payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too long"))?;
        record.extend_from_slice(&payload_len.to_be_bytes());
        record.extend_from_slice(&entry.payload);
        self.wal.append(&record)?;
        Ok(())
    }

    pub fn iter(&mut self) -> io::Result<RaftLogIter<'_>> {
        Ok(RaftLogIter {
            wal: &mut self.wal,
            next_index: 0,
        })
    }

    pub fn last_index(&mut self) -> io::Result<Option<u64>> {
        let len = self.wal.len();
        if len == 0 {
            return Ok(None);
        }
        let Some(bytes) = self.wal.get(len - 1)? else {
            return Ok(None);
        };
        let entry = decode_entry(&bytes).map_err(|message| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt raft log: {message}"),
            )
        })?;
        Ok(Some(entry.index))
    }

    pub fn is_empty(&self) -> bool {
        self.wal.is_empty()
    }

    pub fn len(&self) -> usize {
        self.wal.len()
    }
}

pub struct RaftLogIter<'a> {
    wal: &'a mut Wal,
    next_index: usize,
}

impl Iterator for RaftLogIter<'_> {
    type Item = io::Result<RaftLogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.wal.get(self.next_index).ok()?;
        let Some(bytes) = bytes else {
            return None;
        };
        self.next_index += 1;
        Some(decode_entry(&bytes).map_err(|message| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt raft log: {message}"),
            )
        }))
    }
}

fn decode_entry(bytes: &[u8]) -> Result<RaftLogEntry, &'static str> {
    if bytes.len() < 18 {
        return Err("record too short");
    }
    let term = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let index = u64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let task_id_len = u16::from_be_bytes([bytes[16], bytes[17]]) as usize;
    let task_id_start = 18;
    let task_id_end = task_id_start + task_id_len;
    if bytes.len() < task_id_end + 4 {
        return Err("truncated task id or payload length");
    }
    let task_id = std::str::from_utf8(&bytes[task_id_start..task_id_end])
        .map_err(|_| "invalid task id utf-8")?
        .to_string();
    let payload_len = u32::from_be_bytes([
        bytes[task_id_end],
        bytes[task_id_end + 1],
        bytes[task_id_end + 2],
        bytes[task_id_end + 3],
    ]) as usize;
    let payload_start = task_id_end + 4;
    let payload_end = payload_start + payload_len;
    if bytes.len() != payload_end {
        return Err("trailing bytes or truncated payload");
    }
    Ok(RaftLogEntry {
        term,
        index,
        task_id,
        payload: bytes[payload_start..payload_end].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::RaftLog;
    use crate::raft::RaftLogEntry;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn append_and_replay_entries() {
        let path = test_path("append-replay");
        let mut log = RaftLog::open(&path).expect("raft log opens");

        log.append(&RaftLogEntry {
            term: 1,
            index: 1,
            task_id: "task-1".to_string(),
            payload: b"one".to_vec(),
        })
        .expect("appends");
        log.append(&RaftLogEntry {
            term: 1,
            index: 2,
            task_id: "task-2".to_string(),
            payload: b"two".to_vec(),
        })
        .expect("appends");

        let entries: Vec<_> = log
            .iter()
            .expect("iterates")
            .map(|e| e.expect("decodes"))
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].task_id, "task-2");

        fs::remove_file(path).ok();
    }

    #[test]
    fn reopen_replays_existing_entries() {
        let path = test_path("reopen");
        {
            let mut log = RaftLog::open(&path).expect("raft log opens");
            log.append(&RaftLogEntry {
                term: 2,
                index: 5,
                task_id: "task-a".to_string(),
                payload: b"payload".to_vec(),
            })
            .expect("appends");
        }

        let mut log = RaftLog::open(&path).expect("raft log reopens");
        let entries: Vec<_> = log
            .iter()
            .expect("iterates")
            .map(|e| e.expect("decodes"))
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].term, 2);
        assert_eq!(entries[0].index, 5);
        assert_eq!(log.last_index().expect("last index"), Some(5));

        fs::remove_file(path).ok();
    }

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("core-engine-raft-log-{name}-{nanos}.log"))
    }
}

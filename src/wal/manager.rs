#![allow(dead_code, unused_variables)]

use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use crc32fast::hash;
use parking_lot::{FairMutex, FairMutexGuard};
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::txn_manager::TxnId;
use crate::wal::record::{LogRecord, Record};
use crate::wal::Lsn;

#[cfg(test)]
use std::path::PathBuf;

const LEN_FIELD_SIZE: u64 = 8;
const CHECKSUM_FIELD_SIZE: u64 = 4;

/// 7 name bytes + 1 format-version byte. Bump the last byte on any
/// change to the record wire format that isn't an appended enum variant.
const MAGIC: [u8; 8] = *b"NIWIDDB\x01";

/// First byte past the file header: where the first record lives
pub(crate) const LOG_START: Lsn = MAGIC.len() as Lsn;

pub type ArcLogManager = Arc<LogManagerHandle>;

/// Wraps the manager so the recovery flag is readable without taking the
/// lock: replay holds the mutex while call sites must check the flag first.
pub struct LogManagerHandle {
    inner: FairMutex<LogManager>,
    recovering: AtomicBool,
}

impl LogManagerHandle {
    pub fn lock(&self) -> FairMutexGuard<'_, LogManager> {
        self.inner.lock()
    }

    pub fn recovering(&self) -> bool {
        self.recovering.load(Ordering::Relaxed)
    }

    pub fn set_recovering(&self, on: bool) {
        self.recovering.store(on, Ordering::Relaxed);
    }
}

pub struct LogManager {
    handle: File,
    prev_lsn: HashMap<TxnId, Lsn>,
    next_lsn: Lsn,
    last_synced_lsn: Lsn,
    /// iterator cursor
    cursor: Lsn,

    #[cfg(test)]
    path: PathBuf,
}

impl LogManagerHandle {
    pub fn new(data_dir: &str) -> ArcLogManager {
        Arc::new(LogManagerHandle {
            inner: FairMutex::new(LogManager::open(data_dir)),
            recovering: AtomicBool::new(false),
        })
    }
}

impl LogManager {
    fn open(data_dir: &str) -> Self {
        let dir = Path::new(data_dir);

        create_dir_all(dir).unwrap_or_else(|e| panic!("Failed to write to {}: {e}", dir.display()));

        let path = dir.join("wal.log");

        let mut handle = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .expect("Failed to open WAL file");

        let len = handle.metadata().expect("Failed to stat WAL file").len();

        let next_lsn = if len == 0 {
            handle
                .write_all(&MAGIC)
                .expect("Failed to write WAL header");
            handle.sync_all().expect("Failed to sync WAL header");
            MAGIC.len() as Lsn
        } else {
            let mut header = [0u8; MAGIC.len()];
            handle.seek(SeekFrom::Start(0)).expect("Failed to seek WAL");
            handle
                .read_exact(&mut header)
                .expect("WAL file shorter than its header");
            assert_eq!(
                header, MAGIC,
                "WAL header mismatch: not a WAL file or unsupported format version"
            );
            len
        };

        LogManager {
            handle,
            prev_lsn: HashMap::new(),
            next_lsn,
            // bytes already in the file survived the last process; they
            // need no fsync, so the watermark starts fully caught up
            last_synced_lsn: next_lsn,
            cursor: MAGIC.len() as Lsn,
            #[cfg(test)]
            path,
        }
    }

    pub fn truncate(&mut self, checkpoint: Lsn) -> Result<()> {
        todo!()
    }

    pub fn append(&mut self, txn_id: TxnId, record_type: Record) -> Lsn {
        let lsn = self.next_lsn;
        let prev_lsn = self.prev_lsn.get(&txn_id).copied().unwrap_or(0);

        match record_type {
            // txn is over; drop its chain head so the map only tracks live txns
            Record::Commit | Record::Abort => {
                self.prev_lsn.remove(&txn_id);
            }
            Record::Operation(_)
            | Record::CreateTable(_, _)
            | Record::DropTable(_)
            | Record::Truncate(_) => {
                self.prev_lsn.insert(txn_id, lsn);
            }
        }

        let record = LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            record_type,
        };

        let serialized = serialize(&record).unwrap();
        let len = serialized.len() as u64;
        let checksum = hash(&serialized);

        let buf: Vec<u8> = len
            .to_be_bytes()
            .into_iter()
            .chain(checksum.to_be_bytes())
            .chain(serialized)
            .collect();

        // file was created with append(true) -> every write lands at end-of-file, which next_lsn mirrors
        self.handle
            .write_all(&buf)
            .expect("Failed to append to WAL");

        // len field (u64) + checksum field (u32) + payload
        self.next_lsn += LEN_FIELD_SIZE + CHECKSUM_FIELD_SIZE + len;

        debug_assert_eq!(
            self.next_lsn,
            self.handle.metadata().unwrap().len(),
            "next_lsn drifted from the real file length"
        );

        lsn
    }

    pub fn commit(&mut self, lsn: Lsn) -> Result<Lsn> {
        if lsn <= self.last_synced_lsn {
            return Ok(self.last_synced_lsn);
        }

        self.handle
            .sync_all()
            .context("Failed to sync WAL to disk")?;

        // everything appended so far is now durable
        self.last_synced_lsn = self.next_lsn;

        Ok(self.last_synced_lsn)
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    fn seek_from(&mut self, lsn: Lsn) {
        self.cursor = lsn;
    }

    /// None means end of log: clean EOF or a torn tail from a crash
    /// mid-append; either way, nothing at or past the cursor is trusted
    fn next_record(&mut self) -> Option<LogRecord> {
        let record_len = &mut [0; LEN_FIELD_SIZE as usize];
        self.handle.read_exact_at(record_len, self.cursor).ok()?;
        let record_len = u64::from_be_bytes(*record_len);

        self.cursor += LEN_FIELD_SIZE;

        let checksum = &mut [0; CHECKSUM_FIELD_SIZE as usize];
        self.handle.read_exact_at(checksum, self.cursor).ok()?;
        let checksum = u32::from_be_bytes(*checksum);

        self.cursor += CHECKSUM_FIELD_SIZE;

        let mut record = vec![0u8; record_len as usize];
        self.handle.read_exact_at(&mut record, self.cursor).ok()?;

        self.cursor += record_len;

        let record_checksum = hash(&record);
        if record_checksum != checksum {
            // Skip broken record
            return None;
        }

        // checksum was checked, we don't expect this to fail
        let record = deserialize(&record).expect("Record deserialization somehow failed");

        Some(record)
    }

    /// Stops at the first torn or corrupt record
    pub(crate) fn iter_from(&mut self, from: Lsn) -> impl Iterator<Item = LogRecord> + '_ {
        self.seek_from(from);
        std::iter::from_fn(|| self.next_record())
    }
}

#[cfg(test)]
impl LogManager {
    /// Create a second LogManager to similuate recovery after crash
    fn duplicate(&self) -> LogManager {
        let path = self.path.clone();
        let dir = path.parent().unwrap().to_str().unwrap();
        LogManager::open(dir)
    }
}

#[cfg(test)]
impl Drop for LogManager {
    fn drop(&mut self) {
        // delete only the log file, the dir is shared with the disk
        // manager's pages; remove_dir only succeeds once the dir is empty
        use std::fs::{remove_dir, remove_file};

        remove_file(&self.path).unwrap_or_default();
        remove_dir(self.path.parent().unwrap()).unwrap_or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_manager::test_path;
    use crate::lit;
    use crate::types::Types;
    use crate::types::ValueFactory;
    use crate::wal::record::RowOperation;

    pub fn test_log_manager() -> LogManager {
        LogManager::open(&test_path())
    }

    fn dummy_record(int: i32) -> Record {
        Record::Operation(RowOperation::Insert(
            "test".into(),
            vec![lit!(Int, format!("{int}")).unwrap()],
        ))
    }

    #[test]
    fn test_append_advances_next_lsn() -> Result<()> {
        let mut lm = test_log_manager();
        let next_lsn = lm.next_lsn;
        lm.append(0, dummy_record(30));

        assert_ne!(next_lsn, lm.next_lsn);

        Ok(())
    }

    #[test]
    fn one_record_roundtrip() -> Result<()> {
        let mut lm = test_log_manager();
        let record_type =
            Record::Operation(RowOperation::Insert("test".into(), vec![lit!(Int, "30")?]));
        let lsn = lm.append(10, record_type.clone());
        lm.commit(lm.next_lsn)?;

        let mut new_lm = lm.duplicate();

        let expected = LogRecord {
            lsn,
            prev_lsn: 0,
            txn_id: 10,
            record_type,
        };

        assert_eq!(
            serialize(
                &new_lm
                    .next_record()
                    .expect("expected LM to produce a record")
            )
            .unwrap(),
            serialize(&expected).unwrap(),
        );

        Ok(())
    }

    #[test]
    fn torn_record() -> Result<()> {
        let mut lm = test_log_manager();

        let r1 = dummy_record(10);
        let r2 = dummy_record(20);
        let lsn1 = lm.append(1, r1.clone());
        let lsn2 = lm.append(2, r2.clone());

        lm.handle.write_all(b"garbage").unwrap();

        let mut new_lm = lm.duplicate();

        let e1 = LogRecord {
            lsn: lsn1,
            prev_lsn: 0,
            txn_id: 1,
            record_type: r1,
        };

        let e2 = LogRecord {
            lsn: lsn2,
            prev_lsn: 0,
            txn_id: 2,
            record_type: r2,
        };

        assert_eq!(
            serialize(
                &new_lm
                    .next_record()
                    .expect("expected LM to produce a record")
            )
            .unwrap(),
            serialize(&e1).unwrap()
        );

        assert_eq!(
            serialize(
                &new_lm
                    .next_record()
                    .expect("expected LM to produce a record")
            )
            .unwrap(),
            serialize(&e2).unwrap()
        );

        Ok(())
    }
}

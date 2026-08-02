#![allow(dead_code, unused_variables)]

use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use crc32fast::hash;
use parking_lot::{FairMutex, FairMutexGuard};
use std::collections::HashMap;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::txn_manager::TxnId;
use crate::wal::record::{LogRecord, Record};
use crate::wal::Lsn;

const LEN_FIELD_SIZE: u64 = 8;
const CHECKSUM_FIELD_SIZE: u64 = 4;

/// 7 name bytes + 1 format-version byte. Bump the last byte on any
/// change to the record wire format that isn't an appended enum variant.
const MAGIC: [u8; 8] = *b"NIWIDDB\x01";

/// magic + the lsn the first record in the file carries
const HEADER_SIZE: u64 = MAGIC.len() as u64 + 8;

/// The lsn of the very first record ever written
pub(crate) const LOG_START: Lsn = HEADER_SIZE;

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
    dir: PathBuf,
    handle: File,
    /// lsn of the first record in the file; truncation moves it forward
    base: Lsn,
    prev_lsn: HashMap<TxnId, Lsn>,
    next_lsn: Lsn,
    last_synced_lsn: Lsn,
    /// iterator cursor
    cursor: Lsn,
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

        let mut handle = Self::open_file(dir);
        let len = handle.metadata().expect("Failed to stat WAL").len();

        let base = if len == 0 {
            Self::write_header(&mut handle, LOG_START);
            LOG_START
        } else {
            Self::read_header(&mut handle)
        };

        LogManager {
            dir: dir.to_path_buf(),
            handle,
            base,
            prev_lsn: HashMap::new(),
            next_lsn: base + len.saturating_sub(HEADER_SIZE),
            // bytes already in the file survived the last process; they
            // need no fsync, so the watermark starts fully caught up
            last_synced_lsn: base + len.saturating_sub(HEADER_SIZE),
            cursor: base,
        }
    }

    fn path(dir: &Path) -> PathBuf {
        dir.join("wal.log")
    }

    fn open_file(dir: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(Self::path(dir))
            .expect("Failed to open WAL file")
    }

    fn write_header(file: &mut File, base: Lsn) {
        file.write_all(&MAGIC).expect("Failed to write WAL header");
        file.write_all(&base.to_be_bytes())
            .expect("Failed to write WAL base");
        file.sync_all().expect("Failed to sync WAL header");
    }

    fn read_header(file: &mut File) -> Lsn {
        let mut header = [0u8; HEADER_SIZE as usize];

        file.seek(SeekFrom::Start(0)).expect("Failed to seek WAL");
        file.read_exact(&mut header)
            .expect("WAL shorter than its header");

        assert_eq!(
            header[..MAGIC.len()],
            MAGIC,
            "WAL header mismatch: not a WAL file or unsupported format version"
        );

        Lsn::from_be_bytes(header[MAGIC.len()..].try_into().unwrap())
    }

    /// Throws the log away and starts a new one at `covered`. The caller holds
    /// the lock across the checkpoint, so there is never anything past it.
    pub fn truncate(&mut self, covered: Lsn) -> Result<()> {
        debug_assert_eq!(covered, self.next_lsn, "records would be lost");

        remove_file(Self::path(&self.dir))?;

        self.handle = Self::open_file(&self.dir);
        Self::write_header(&mut self.handle, covered);

        self.base = covered;
        self.cursor = covered;

        Ok(())
    }

    /// The oldest lsn still on disk; anything before it has been truncated
    pub fn oldest_lsn(&self) -> Lsn {
        self.base
    }

    fn offset(&self, lsn: Lsn) -> u64 {
        HEADER_SIZE + (lsn - self.base)
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
            self.offset(self.next_lsn),
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
        let mut offset = self.offset(self.cursor);

        let record_len = &mut [0; LEN_FIELD_SIZE as usize];
        self.handle.read_exact_at(record_len, offset).ok()?;
        let record_len = u64::from_be_bytes(*record_len);
        offset += LEN_FIELD_SIZE;

        let checksum = &mut [0; CHECKSUM_FIELD_SIZE as usize];
        self.handle.read_exact_at(checksum, offset).ok()?;
        let checksum = u32::from_be_bytes(*checksum);
        offset += CHECKSUM_FIELD_SIZE;

        let mut record = vec![0u8; record_len as usize];
        self.handle.read_exact_at(&mut record, offset).ok()?;

        if hash(&record) != checksum {
            // torn or corrupt: nothing past here is trusted
            return None;
        }

        self.cursor += LEN_FIELD_SIZE + CHECKSUM_FIELD_SIZE + record_len;

        // checksum was checked, we don't expect this to fail
        Some(deserialize(&record).expect("Record deserialization somehow failed"))
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
        LogManager::open(self.dir.to_str().unwrap())
    }
}

#[cfg(test)]
impl Drop for LogManager {
    fn drop(&mut self) {
        // delete only the log, the dir is shared with the disk manager's
        // pages; remove_dir only succeeds once the dir is empty
        remove_file(Self::path(&self.dir)).unwrap_or_default();
        std::fs::remove_dir(&self.dir).unwrap_or_default();
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

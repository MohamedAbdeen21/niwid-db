#![allow(dead_code, unused_variables)]

use anyhow::{Context, Result};
use lazy_static::lazy_static;
use parking_lot::FairMutex;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::{empty, Iterator};
use std::path::Path;
use std::sync::Arc;

use crate::disk_manager::DISK_STORAGE;
use crate::txn_manager::TxnId;
use crate::wal::record::{LogRecord, Record};
use crate::wal::Lsn;

const LOG_NAME: &str = "_WAL";

/// 7 name bytes + 1 format-version byte. Bump the last byte on any
/// change to the record wire format that isn't an appended enum variant.
const MAGIC: [u8; 8] = *b"NIWIDDB\x01";

pub type ArcLogManager = Arc<FairMutex<LogManager>>;

lazy_static! {
    static ref LM: ArcLogManager = Arc::new(FairMutex::new(LogManager::new(DISK_STORAGE)));
}

pub struct LogManager {
    handle: File,
    prev_lsn: HashMap<TxnId, Lsn>,
    next_lsn: Lsn,
    last_synced_lsn: Lsn,
}

impl LogManager {
    pub fn get() -> ArcLogManager {
        LM.clone()
    }

    fn new(data_dir: &str) -> Self {
        let path = Path::new(data_dir);

        create_dir_all(path).unwrap_or_else(|_| panic!("Failed to write to {}", path.display()));

        let mut handle = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.join("wal.log"))
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
        }
    }

    pub fn truncate(&mut self, checkpoint: Lsn) -> Result<()> {
        todo!()
    }

    pub fn append(&mut self, mut record: LogRecord) -> Lsn {
        record.lsn = self.next_lsn;
        record.prev_lsn = self.prev_lsn.get(&record.txn_id).copied().unwrap_or(0);

        match record.record_type {
            // txn is over; drop its chain head so the map only tracks live txns
            Record::Commit | Record::Abort => {
                self.prev_lsn.remove(&record.txn_id);
            }
            Record::Operation(_) => {
                self.prev_lsn.insert(record.txn_id, record.lsn);
            }
        }

        let serialized = bincode::serialize(&record).unwrap();
        let len = serialized.len() as u64;
        let checksum = crc32fast::hash(&serialized);

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
        self.next_lsn += 8 + 4 + len;

        debug_assert_eq!(
            self.next_lsn,
            self.handle.metadata().unwrap().len(),
            "next_lsn drifted from the real file length"
        );

        record.lsn
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

    // Repeat all operations starting from a given LSN
    // used in recovery and adding nodes
    fn iter_from(&mut self, lsn: Lsn) -> impl Iterator<Item = Record> {
        empty()
    }
}

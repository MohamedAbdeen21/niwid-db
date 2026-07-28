#![allow(dead_code)]

use crate::pages::traits::Serialize;
use crate::txn_manager::TxnId;
use crate::types::Value;
use crate::wal::Lsn;

type TableName = String;

pub enum RowOperation {
    Insert(TableName, Vec<Value>),
    Delete(TableName, Vec<Value>),
    Update {
        table: TableName,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    },
}

#[repr(C)]
struct LogRecord {
    lsn: Lsn,
    prev_lsn: Lsn,
    txn_id: TxnId,
    checksum: u32,
    length: u32,
    record_type: Record,
}

pub enum Record {
    Commit,
    Abort,
    Operation(RowOperation),
}

impl LogRecord {
    pub fn new(_txn_id: TxnId, _record: Record) -> Self {
        todo!()
    }
}

impl Serialize for LogRecord {
    fn to_bytes(&self) -> &[u8] {
        todo!()
    }

    fn from_bytes(_bytes: &[u8]) -> Self {
        todo!()
    }
}

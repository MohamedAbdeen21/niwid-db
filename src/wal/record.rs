#![allow(dead_code)]

use crate::types::Value;
use crate::wal::Lsn;
use crate::{tuple::schema::Schema, txn_manager::TxnId};

use serde::{Deserialize, Serialize};

type TableName = String;

#[derive(Serialize, Deserialize)]
pub enum RowOperation {
    Insert(TableName, Vec<Value>),
    Delete(TableName, Vec<Value>),
    // Tables currently do update by calling delete then insert
    Update {
        table: TableName,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Record {
    Commit,
    Abort,
    Operation(RowOperation),
    CreateTable(TableName, Schema),
    DropTable(TableName),
    Truncate(TableName),
}

#[derive(Serialize, Deserialize)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: Record,
}

impl LogRecord {
    pub fn new(txn_id: TxnId, record: Record) -> Self {
        LogRecord {
            // All filed later
            lsn: 0,
            prev_lsn: 0,
            txn_id,
            record_type: record,
        }
    }
}

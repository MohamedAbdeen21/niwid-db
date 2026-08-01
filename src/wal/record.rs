#![allow(dead_code)]

use std::fmt::Display;

use crate::types::Value;
use crate::wal::Lsn;
use crate::{tuple::schema::Schema, txn_manager::TxnId};

use serde::{Deserialize, Serialize};

type TableName = String;

#[derive(Serialize, Deserialize, Clone)]
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

#[derive(Serialize, Deserialize, Clone)]
pub enum Record {
    Commit,
    Abort,
    Operation(RowOperation),
    CreateTable(TableName, Schema),
    DropTable(TableName),
    Truncate(TableName),
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let join = |values: &[Value]| {
            values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };

        match self {
            Record::Commit => write!(f, "COMMIT"),
            Record::Abort => write!(f, "ABORT"),
            Record::Operation(RowOperation::Insert(table, values)) => {
                write!(f, "INSERT {table} ({})", join(values))
            }
            Record::Operation(RowOperation::Delete(table, values)) => {
                write!(f, "DELETE {table} ({})", join(values))
            }
            Record::Operation(RowOperation::Update {
                table,
                old_values,
                new_values,
            }) => {
                write!(
                    f,
                    "UPDATE {table} ({}) -> ({})",
                    join(old_values),
                    join(new_values)
                )
            }
            Record::CreateTable(table, schema) => write!(f, "CREATE {table} {}", schema.to_sql()),
            Record::DropTable(table) => write!(f, "DROP {table}"),
            Record::Truncate(table) => write!(f, "TRUNCATE {table}"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct LogRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: TxnId,
    pub record_type: Record,
}

impl Display for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lsn: {}, prev_lsn: {}, txn_id: {}, record_type: {}",
            self.lsn, self.prev_lsn, self.txn_id, self.record_type,
        )
    }
}

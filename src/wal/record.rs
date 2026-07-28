use crate::pages::traits::Serialize;
use crate::txn_manager::TxnId;
use crate::types::Value;

type LSN = u64;
type Table = String;

#[allow(dead_code)]
pub enum RowOperation {
    Insert(Table, Vec<Value>),
    Delete(Table, Vec<Value>),
    Update {
        table: Table,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    },
}

#[repr(C)]
#[allow(dead_code)]
struct LogRecord {
    lsn: LSN,
    prev_lsn: LSN,
    txn_id: TxnId,
    checksum: u32,
    length: u32,
    record_type: Record,
}

#[allow(dead_code)]
pub enum Record {
    COMMIT,
    ABORT,
    OP(RowOperation),
}

impl LogRecord {
    #[allow(dead_code)]
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

use crate::types::Types;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    TableExists(String),
    TupleExists,
    TupleNotFound,
    TableNotFound(String),
    Unimplemented(String),
    Unsupported(String),
    Expected(String, String),
    TypeMismatch(Vec<Types>, Vec<Types>),
    TransactionActive,
    NoActiveTransaction,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Internal(context) => write!(f, "Internal Error: {context}."),
            Error::TableExists(table) => write!(f, "Table {table} already exists."),
            Error::TupleExists => write!(f, "Tuple already exists"),
            Error::Unimplemented(object) => write!(f, "Not yet implemented: {object}."),
            Error::Unsupported(context) => write!(f, "Unsupported: {context}."),
            Error::Expected(expected, actual) => {
                write!(f, "Expected {expected}, but got {actual}.")
            }
            Error::TableNotFound(table) => write!(f, "Table {table} not found."),
            Error::TupleNotFound => write!(f, "Tuple not found."),
            Error::TransactionActive => write!(f, "Writing transaction already active."),
            Error::NoActiveTransaction => write!(f, "No active transaction."),
            Error::TypeMismatch(expected, actual) => {
                write!(
                    f,
                    "Type mismatch: Expected {expected:?}, but got {actual:?}."
                )
            }
        }
    }
}

impl std::error::Error for Error {}

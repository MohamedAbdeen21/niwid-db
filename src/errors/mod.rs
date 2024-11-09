#[derive(Debug)]
pub enum Error {
    Internal(String),
    TableExists(String),
    TupleExists,
    Unimplemented(String),
    Unsupported(String),
    Expected(String, String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Internal(context) => write!(f, "Internal Error: {context}."),
            Error::TableExists(table) => write!(f, "Table {table} already exists."),
            Error::TupleExists => write!(f, "Tuple already exists"),
            Error::Unimplemented(object) => write!(f, "Not yet implemented: {object}."),
            Error::Unsupported(context) => write!(f, "Unsupported: {context}."),
            Error::Expected(actual, expected) => {
                write!(f, "Expected {expected}, but got {actual}.")
            }
        }
    }
}

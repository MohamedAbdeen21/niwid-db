use anyhow::Result;
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

pub fn parse(sql: impl Into<String>) -> Result<Statement> {
    Ok(Parser::new(&GenericDialect)
        .try_with_sql(&sql.into())?
        .parse_statement()?)
}

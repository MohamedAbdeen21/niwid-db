mod buffer_pool;
mod catalog;
mod disk_manager;
mod executor;
mod pages;
mod table;
mod tuple;
mod txn_manager;
mod types;

use anyhow::Result;
use executor::Executor;
use tuple::schema::{Field, Schema};
use types::Types;

fn main() -> Result<()> {
    let mut ctx = Executor::new()?;

    ctx.start_txn()?;

    // TODO: Use fields for handling nullability
    let schema = Schema::new(vec![
        Field::new("id", Types::U8, true),
        Field::new("age", Types::Str, false),
    ]);

    let _ = ctx.add_table("users", &schema, true)?;

    ctx.execute_sql("INSERT INTO users VALUES (2, 'Hello')")?;
    ctx.execute_sql("INSERT INTO users VALUES (null, 'World!')")?;
    ctx.execute_sql("SELECT *, id FROM users")?.show();

    ctx.commit_txn()?;

    Ok(())
}

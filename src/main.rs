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
use tuple::schema::Schema;
use types::Types;

fn main() -> Result<()> {
    let mut ctx = Executor::new()?;

    ctx.start_txn()?;

    // TODO: Use fields for handling nullability
    let schema = Schema::new(vec!["id", "age"], vec![Types::U8, Types::Str]);
    let _ = ctx.add_table("users", &schema, true)?;

    ctx.execute_sql("INSERT INTO users VALUES (2, 'Hello')")?;
    ctx.execute_sql("INSERT INTO users VALUES (null, 'World!')")?;
    ctx.execute_sql("SELECT id, age FROM users")?;

    ctx.commit_txn()?;

    Ok(())
}

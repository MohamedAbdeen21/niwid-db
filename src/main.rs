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

    let schema = Schema::new(vec!["id", "age"], vec![Types::U8, Types::U16]);
    let _ = ctx.add_table("users", &schema, true)?;

    ctx.execute_sql("INSERT INTO users VALUES (2, 3)")?;
    ctx.execute_sql("INSERT INTO users VALUES (null, 6)")?;
    ctx.execute_sql("SELECT id, age FROM users")?;
    ctx.commit_txn()?;

    Ok(())
}

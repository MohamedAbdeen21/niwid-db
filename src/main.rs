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
        Field::new("msg", Types::Str, false),
    ]);

    let _ = ctx.add_table("users", &schema, true)?;

    ctx.execute_sql("INSERT INTO users VALUES (1, 'foo')")?;
    ctx.execute_sql("INSERT INTO users VALUES (2, 'bar')")?;
    ctx.execute_sql("INSERT INTO users VALUES (3, 'baz')")?;

    ctx.execute_sql("UPDATE users SET msg='baz2' WHERE id=3")?;
    ctx.execute_sql("UPDATE users SET id=4 WHERE msg='foo'")?;

    ctx.execute_sql("SELECT *, id FROM users")?.show();

    ctx.commit_txn()?;

    Ok(())
}

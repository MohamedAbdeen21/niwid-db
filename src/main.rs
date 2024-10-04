use anyhow::Result;
use idk::context::Context;
use idk::tuple::schema::{Field, Schema};
use idk::types::Types;

fn main() -> Result<()> {
    let mut ctx = Context::new()?;

    ctx.start_txn()?;

    // TODO: Use fields for handling nullability
    let schema = Schema::new(vec![
        Field::new("id", Types::U8, true),
        Field::new("msg", Types::Str, false),
    ]);

    ctx.execute_sql(format!(
        "CREATE TABLE IF NOT EXISTS users (
                {}
        )",
        schema.to_sql(),
    ))?;

    ctx.execute_sql("INSERT INTO users VALUES (1, 'foo')")?;
    ctx.execute_sql("INSERT INTO users VALUES (2, 'bar')")?;
    ctx.execute_sql("INSERT INTO users VALUES (3, 'baz')")?;
    ctx.execute_sql("INSERT INTO users VALUES (4, null)")?;

    ctx.execute_sql("UPDATE users SET msg='baz2' WHERE id=3")?;
    ctx.execute_sql("UPDATE users SET id=4 WHERE msg='foo'")?;
    ctx.execute_sql("UPDATE users SET id=5 WHERE msg=null")?;

    ctx.execute_sql("SELECT *, id FROM users")?.show();

    ctx.commit_txn()?;

    Ok(())
}

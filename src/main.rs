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

    ctx.add_table("users", &schema, true)?;

    // ctx.execute_sql(format!(
    //     "CREATE TABLE IF NOT EXISTS users (
    //             {}
    //     )",
    //     schema.to_sql(),
    // ))?;

    ctx.execute_sql("SELECT *, id FROM users WHERE id = 2")?
        .show();

    ctx.commit_txn()?;

    Ok(())
}

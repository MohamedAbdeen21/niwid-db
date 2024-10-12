use anyhow::Result;
use idk::context::Context;

fn main() -> Result<()> {
    let mut ctx = Context::new()?;

    ctx.start_txn()?;

    // let schema = Schema::new(vec![
    //     Field::new("id", Types::U8, true),
    //     Field::new("num", Types::U8, true),
    //     Field::new("msg", Types::Str, false),
    // ]);

    ctx.execute_sql("CREATE TABLE IF NOT EXISTS users (id SMALLINT, num SMALLINT, msg VARCHAR)")?;

    // ctx.execute_sql(format!(
    //     "CREATE TABLE IF NOT EXISTS users (
    //             {}
    //     )",
    //     schema.to_sql(),
    // ))?;

    ctx.execute_sql("EXPLAIN SELECT msg, id FROM users WHERE num > 10")?;
    ctx.execute_sql("SELECT msg, id FROM users WHERE num > 10")?;

    ctx.commit_txn()?;

    Ok(())
}

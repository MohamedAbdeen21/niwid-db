use anyhow::Result;
use idk::context::Context;

fn main() -> Result<()> {
    let mut ctx = Context::new()?;

    ctx.start_txn()?;

    // ctx.execute_sql("DROP TABLE IF EXISTS users;")?;

    ctx.execute_sql(
        "CREATE TABLE IF NOT EXISTS users
        (id UINT, num INT, msg VARCHAR)",
    )?;

    ctx.execute_sql("TRUNCATE TABLE users;")?;

    ctx.execute_sql(
        "EXPLAIN ANALYZE INSERT INTO users
        VALUES (1,1,'hello'), (2,3,'world');",
    )?;

    ctx.execute_sql(
        "EXPLAIN ANALYZE SELECT num, id, msg
        FROM users;",
    )?
    .show();

    ctx.execute_sql(
        "EXPLAIN ANALYZE SELECT num, id, msg
        FROM users WHERE id = num;",
    )?
    .show();

    ctx.execute_sql(
        "EXPLAIN ANALYZE SELECT num, id, msg
        FROM users WHERE num > 2;",
    )?
    .show();

    ctx.commit_txn()?;

    Ok(())
}

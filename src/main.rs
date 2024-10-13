use anyhow::Result;
use idk::context::Context;

fn main() -> Result<()> {
    let mut ctx = Context::new()?;

    ctx.start_txn()?;

    ctx.execute_sql("CREATE TABLE IF NOT EXISTS users (id UINT, num INT, msg VARCHAR)")?;

    ctx.execute_sql("INSERT INTO users SELECT (1,2,'hello')")?;
    ctx.execute_sql("EXPLAIN SELECT num, id, msg FROM users")?;
    ctx.execute_sql("SELECT num, id, msg FROM users")?.show();

    ctx.commit_txn()?;

    Ok(())
}

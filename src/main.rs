mod buffer_pool;
mod catalog;
mod disk_manager;
mod executor;
mod pages;
mod table;
mod tuple;
mod txn_manager;
mod types;

use std::{thread, time::Duration};

use anyhow::Result;
use executor::Executor;
use parking_lot::deadlock;
use tuple::schema::Schema;
use types::Types;

fn main() -> Result<()> {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(2));
        let deadlocks = deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{}", i);
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });

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

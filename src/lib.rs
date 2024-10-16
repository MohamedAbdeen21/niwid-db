mod buffer_pool;
mod catalog;
pub mod context;
mod disk_manager;
mod execution;
mod pages;
mod sql;
mod table;
pub mod tuple;
mod txn_manager;
pub mod types;
mod versioned_map;

#[macro_export]
macro_rules! printdbg {
    ($val: expr $(, $args: expr)*) => {
        #[cfg(debug_assertions)]
        println!($val $(, $args)*)
    };
}

#[macro_export]
macro_rules! get_caller_name {
    () => {{
        let mut bt = backtrace::Backtrace::new_unresolved();
        bt.resolve();
        let frames = bt.frames();
        if frames.len() > 1 {
            let caller_frame = &frames[1]; // Caller frame
            if let Some(symbol) = caller_frame.symbols().first() {
                if let Some(name) = symbol.name() {
                    name.to_string() // Return the caller's name as a string
                } else {
                    "Unknown Caller".to_string() // Fallback if name resolution fails
                }
            } else {
                "Unknown Caller".to_string()
            }
        } else {
            "No Caller Found".to_string() // In case the stack isn't deep enough
        }
    }};
}

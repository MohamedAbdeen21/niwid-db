mod buffer_pool;
mod catalog;
mod disk_manager;
mod errors;
mod execution;
mod indexes;
mod latch;
mod pages;
mod sql;
mod table;
mod tuple;
mod txn_manager;
mod types;

#[cfg(test)]
mod sqllogictest;

pub mod context;
pub mod server;

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
            let caller_frame = &frames[1];
            if let Some(symbol) = caller_frame.symbols().first() {
                if let Some(name) = symbol.name() {
                    name.to_string()
                } else {
                    "Unknown Caller".to_string()
                }
            } else {
                "Unknown Caller".to_string()
            }
        } else {
            "No Caller Found".to_string() // In case the stack isn't deep enough
        }
    }};
}

#[macro_export]
macro_rules! is_boolean_op {
    ($op:ident) => {
        matches!(
            $op,
            BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq
        )
    };
}

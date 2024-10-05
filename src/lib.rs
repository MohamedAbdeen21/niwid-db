mod buffer_pool;
mod catalog;
pub mod context;
mod disk_manager;
mod pages;
mod table;
pub mod tuple;
mod txn_manager;
pub mod types;

#[macro_export]
macro_rules! printdbg {
    ($val: expr $(, $args: expr)*) => {
        #[cfg(debug_assertions)]
        println!($val $(, $args)*)
    };
}

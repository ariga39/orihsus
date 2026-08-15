#[cfg(all(feature = "loadtest-insecure-upstream", not(debug_assertions)))]
compile_error!("loadtest-insecure-upstream is forbidden in release builds");

pub mod app;
pub mod audit;
pub mod config;
pub mod gateway;
pub mod hot_reload;
pub mod pool;
pub mod queue;
pub mod server;
pub mod usage;

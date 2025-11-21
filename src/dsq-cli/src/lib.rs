//! dsq-cli library
//!
//! Provides Config and Executor for programmatic use.

#[cfg(all(not(target_arch = "wasm32"), feature = "cli"))]
mod cli;
mod config;
mod executor;

pub use config::Config;
pub use executor::Executor;

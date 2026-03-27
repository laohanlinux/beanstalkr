//! Beanstalkr - Beanstalkd server implementation in Rust.
//!
//! When the `client` feature is enabled, this crate also provides an async beanstalk client.

#![recursion_limit = "512"]

#[macro_use]
extern crate lazy_static;
extern crate strum;
#[macro_use]
extern crate strum_macros;

pub mod architecture;
pub mod backend;
pub mod backup;
pub mod operation;
pub mod util;

#[cfg(feature = "client")]
pub mod client;

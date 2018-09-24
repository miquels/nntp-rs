//! A bunch of helper and utility functions.

#[macro_use] extern crate lazy_static;
extern crate chrono;
extern crate libc;
extern crate regex;
extern crate serde;

use std::time::SystemTime;

mod dateparser;
mod de;
mod hostname;
mod wildmat_fn;
mod wildmat;

pub use dateparser::*;
pub use de::*;
pub use hostname::*;
pub use wildmat_fn::*;
pub use wildmat::*;

pub fn unixtime() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

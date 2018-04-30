#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate log;
extern crate libc;
extern crate time;
extern crate memchr;
extern crate linked_hash_map;

pub mod history;
pub mod spool;
pub mod clock;

pub use history::{History,HistEnt,HistStatus};
pub use spool::{MetaSpoolCfg,SpoolCfg,Spool,ArtPart,ArtLoc};

pub(crate) fn u16_to_b2(b: &mut [u8], offset: usize, val: u16) {
    b[offset] = ((val >> 8) & 0xff) as u8;
    b[offset + 1] = (val & 0xff) as u8;
}

pub(crate) fn u32_to_b4(b: &mut [u8], offset: usize, val: u32) {
    b[offset] = ((val >> 24) & 0xff) as u8;
    b[offset + 1] = ((val >> 16) & 0xff) as u8;
    b[offset + 2] = ((val >> 8) & 0xff) as u8;
    b[offset + 3] = (val & 0xff) as u8;
}

pub(crate) fn b2_to_u16(b: &[u8]) -> u16 {
    (b[0] as u16) << 8 | b[1] as u16
}

pub(crate) fn b4_to_u32(b: &[u8]) -> u32 {
    (b[0] as u32) << 24 | (b[1] as u32) << 16 | (b[2] as u32) << 8 | b[3] as u32
}


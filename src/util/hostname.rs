use libc::{self, gethostname};
use std::ffi::CStr;

/// Rust interface to the libc gethostname function.
///
/// If unqualified, returns "hostname.local".
/// If unset, returns "unconfigured.local".
pub fn hostname() -> String {
    let len = 255;
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr() as *mut libc::c_char;

    unsafe {
        if gethostname(ptr, len as libc::size_t) != 0 {
            return String::from("unconfigured.local");
        }
        let mut h = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        if !h.contains(".") {
            h += ".local";
        }
        h
    }
}

use std::ffi::CStr;
use libc::{self,gethostname};

/// Rust interface to the libc gethostname function.
pub fn hostname() -> Option<String> {
    let len = 255;
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr() as *mut libc::c_char;

    unsafe {
        if gethostname(ptr, len as libc::size_t) != 0 {
            return None;
        }
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}


use std::ffi::CStr;

/// Rust interface to the libc gethostname function.
///
/// If unqualified, returns "hostname.local".
/// If unset, returns "unconfigured.local".
pub fn hostname() -> String {
    let len = 255;
    let mut buf = Vec::<u8>::new();
    buf.resize(len + 1, 0);
    let ptr = buf.as_mut_ptr() as *mut libc::c_char;

    unsafe {
        // this is safe, `ptr` points to buf which has been initialized.
        if libc::gethostname(ptr, len as libc::size_t) != 0 {
            return String::from("unconfigured.local");
        }
        // this is also safe. since `buf` was initialized with zeroes and
        // is actually one byte longer than what we told `gethostname`,
        // it's guaranteed to be zero-terminated.
        let mut h = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        if !h.contains(".") {
            h += ".local";
        }
        h
    }
}

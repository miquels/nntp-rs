
pub fn getpid() -> u32 {
    // safe: systemcall wrapper, no pointers.
    unsafe { libc::getpid() as u32 }
}


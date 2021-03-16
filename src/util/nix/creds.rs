use std::io;

pub fn setgroups(gids: &[u32]) -> io::Result<()> {
    let mut g = Vec::<libc::gid_t>::new();
    g.extend(gids.iter().map(|&g| g as libc::gid_t));
    if unsafe { libc::setgroups(g.len(), g.as_ptr() as _) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}


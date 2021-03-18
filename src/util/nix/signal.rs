
pub use libc::SIGINT;
pub use libc::SIGHUP;
pub use libc::SIGTERM;
pub use libc::SIGQUIT;
pub use libc::SIGUSR1;
pub use libc::SIGUSR2;

pub fn ignore_signal(signal: libc::c_int) {
    unsafe {
        libc::signal(signal, libc::SIG_IGN);
    }
}

pub fn ignore_most_signals() {
    ignore_signal(SIGINT);
    ignore_signal(SIGHUP);
    ignore_signal(SIGTERM);
    ignore_signal(SIGQUIT);
    ignore_signal(SIGUSR1);
    ignore_signal(SIGUSR2);
}

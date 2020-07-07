use std::io;
use std::net::{SocketAddr, TcpListener};

use net2::unix::UnixTcpBuilderExt;

/// A set of TcpListeners (so a set of sets).
pub struct TcpListenerSets {
    sets: Vec<Vec<TcpListener>>,
}

impl TcpListenerSets {
    /// Start listening an the addresses in `addrs` on multiple sockets
    /// at the same time (using SO_REUSEADDR).
    pub fn new(addrs: &[SocketAddr], num_sets: usize) -> io::Result<TcpListenerSets> {
        let mut sets = Vec::new();
        for _ in 0..num_sets {
            let mut listeners = Vec::new();
            for addr in addrs.iter() {
                let listener = bind_socket(&addr)?;
                listeners.push(listener);
            }
            sets.push(listeners);
        }
        Ok(TcpListenerSets { sets })
    }

    pub fn pop(&mut self) -> Option<Vec<TcpListener>> {
        self.sets.pop()
    }

    pub fn len(&self) -> usize {
        self.sets.len()
    }
}

/// Create a socket, set SO_REUSEPORT on it, bind it to an address,
/// and start listening for connections.
pub fn bind_socket(addr: &SocketAddr) -> io::Result<TcpListener> {
    let builder = if addr.is_ipv6() {
        // create IPv6 socket and make it v6-only.
        let b = net2::TcpBuilder::new_v6().map_err(|e| ioerr!(Other, "creating IPv6 socket: {}", e))?;
        b.only_v6(true)
            .map_err(|e| ioerr!(e.kind(), "setting socket to only_v6: {}", e))?;
        b
    } else {
        net2::TcpBuilder::new_v4().map_err(|e| ioerr!(e.kind(), "creating IPv4 socket: {}", e))?
    };
    // reuse_addr to make sure we can restart quickly.
    let builder = builder
        .reuse_address(true)
        .map_err(|e| ioerr!(e.kind(), "setting SO_REUSEADDR on socket: {}", e))?;
    // reuse_port to be able to have multiple sockets listening on the same port.
    let builder = builder
        .reuse_port(true)
        .map_err(|e| ioerr!(e.kind(), "setting SO_REUSEPORT on socket: {}", e))?;
    let builder = builder
        .bind(addr)
        .map_err(|e| ioerr!(e.kind(), "binding socket to {}: {}", addr, e))?;
    let listener = builder
        .listen(128)
        .map_err(|e| ioerr!(e.kind(), "listening on socket: {}", e))?;
    Ok(listener)
}

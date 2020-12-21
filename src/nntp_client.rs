//! Outgoing feeds.
//!
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::net::TcpStream;

use crate::dns;
use crate::nntp_codec::NntpCodec;
use crate::util::{self, hostname, CongestionControl};

static HOSTNAME: Lazy<String> = Lazy::new(|| hostname());

/// Connect to remote server.
///
/// Returns a codec, the address we connected to, and the
/// initial welcome message. Or an error ofcourse.
pub async fn nntp_connect(
    hostname: &str,
    port: u16,
    cmd: &str,
    respcode: u32,
    bindaddr: Option<IpAddr>,
    sendbuf_size: Option<usize>,
    congestion_control: Option<CongestionControl>,
    max_pacing_rate: Option<u32>,
) -> io::Result<(NntpCodec, IpAddr, String)>
{
    use rand::seq::SliceRandom;

    // A lookup of the hostname might return multiple addresses.
    // Shuffle IPv4 and IPv6 addresses separately, then return
    // the lot with the IPv6 addresses in front.
    let addrs = match dns::RESOLVER.lookup_ip(hostname).await {
        Ok(lookupip) => {
            let addrs: Vec<SocketAddr> = lookupip.iter().map(|a| SocketAddr::new(a, port)).collect();
            let mut v6 = addrs.iter().filter(|a| a.is_ipv6()).cloned().collect::<Vec<_>>();
            let mut v4 = addrs.iter().filter(|a| a.is_ipv4()).cloned().collect::<Vec<_>>();
            let mut rng = rand::thread_rng();
            v6.shuffle(&mut rng);
            v4.shuffle(&mut rng);
            v6.extend(v4.drain(..));
            v6
        },
        Err(e) => return Err(ioerr!(Other, e)),
    };

    // Try to connect to the peer.
    let mut last_err = None;
    for addr in &addrs {
        log::debug!("Trying to connect to {}", addr);
        let result = async move {
            // Create socket.
            let is_ipv6 = bindaddr.map(|ref a| a.is_ipv6()).unwrap_or(addr.is_ipv6());
            let domain = if is_ipv6 {
                socket2::Domain::ipv6()
            } else {
                socket2::Domain::ipv4()
            };
            let socket = socket2::Socket::new(domain, socket2::Type::stream(), None).map_err(|e| {
                log::trace!("Connection::connect: Socket::new({:?}): {}", domain, e);
                e
            })?;

            // Set IPV6_V6ONLY if this is going to be an IPv6 connection.
            if is_ipv6 {
                socket.set_only_v6(true).map_err(|_| {
                    log::trace!("Connection::connect: Socket.set_only_v6() failed");
                    ioerr!(AddrNotAvailable, "socket.set_only_v6() failed")
                })?;
            }

            // Bind local address.
            if let Some(ref bindaddr) = bindaddr {
                let sa = SocketAddr::new(bindaddr.to_owned(), 0);
                socket.bind(&sa.clone().into()).map_err(|e| {
                    log::trace!("Connection::connect: Socket::bind({:?}): {}", sa, e);
                    ioerr!(e.kind(), "bind {}: {}", sa, e)
                })?;
            }

            // Set (max) outbuf buffer size.
            if let Some(size) = sendbuf_size {
                let _ = socket.set_send_buffer_size(size);
            }

            /*
                        // Now this sucks, having to run it on a threadpool.
                        //
                        // See below, turns out that tokio _does_ have a method
                        // for this, but it's undocumented. I'm leaving this
                        // here in case it gets removed without a replacement.
                        //
                        log::trace!("Trying to connect to {}", addr);
                        let addr2: socket2::SockAddr = addr.to_owned().into();
                        let res = task::spawn_blocking(move || {
                            // 10 second timeout for a connect is more than enough.
                            socket.connect_timeout(&addr2, Duration::new(10, 0))?;
                            Ok(socket)
                        })
                        .await
                        .unwrap_or_else(|e| Err(ioerr!(Other, "spawn_blocking: {}", e)));
                        let socket = res.map_err(|e| {
                            log::trace!("Connection::connect({}): {}", addr, e);
                            ioerr!(e.kind(), "{}: {}", addr, e)
                        })?;

                        // Now turn it into a tokio::net::TcpStream.
                        let socket = TcpStream::from_std(socket.into()).unwrap();
            */
            let socket = tokio::select! {
                _ = tokio::time::delay_for(Duration::new(10, 0)) => {
                    log::trace!("Connection::connect({}): timed out", addr);
                    return Err(ioerr!(TimedOut, "{}: connection timed out", addr));
                }
                res = TcpStream::connect_std(socket.into(), addr) => {
                    res.map_err(|e| {
                        log::trace!("Connection::connect({}): {}", addr, e);
                        ioerr!(e.kind(), "{}: {}", addr, e)
                    })?
                }
            };

            // set congestion control algorithm (if the OS supports it).
            if let Some(cc) = congestion_control {
                util::set_congestion_control(&socket, cc)
                    .map_err(|e| ioerr!(e.kind(), "set_congestion_control {:?}: {}", cc, e))?;
            }

            // set max pacing rate (if the OS supports it).
            if let Some(rate) = max_pacing_rate {
                util::set_max_pacing_rate(&socket, rate)
                    .map_err(|e| ioerr!(e.kind(), "set_max_pacing_rate {:?}: {}", rate, e))?;
            }

            // Create codec from socket.
            let mut codec = NntpCodec::builder(socket)
                .read_timeout(30)
                .write_timeout(60)
                .build();

            // Read initial response code.
            let resp = codec.read_response().await.map_err(|e| {
                log::trace!("{:?} read_response: {}", addr, e);
                ioerr!(e.kind(), "{}: {}", addr, e)
            })?;
            log::trace!("<< {}", resp.short());
            if resp.code / 100 != 2 {
                Err(ioerr!(
                    InvalidData,
                    "{}: initial response {}, expected 2xx",
                    addr,
                    resp.code
                ))?;
            }
            let connect_msg = resp.short().to_string();

            if cmd != "" {
                // Send command (e.g. MODE STREAM).
                log::trace!(">> {}", cmd);
                let resp = codec
                    .command(cmd)
                    .await
                    .map_err(|e| ioerr!(e.kind(), "{}: {}", addr, e))?;
                log::trace!("<< {}", resp.short());
                if resp.code != respcode {
                    Err(ioerr!(
                        InvalidData,
                        "{}: {} response {}, expected {}",
                        addr,
                        cmd,
                        resp.code,
                        respcode,
                    ))?;
                }
            }

            Ok((codec, connect_msg))
        }
        .await;

        // On success, return. Otherwise, save the error.
        match result {
            Ok((codec, connect_msg)) => return Ok((codec, addr.ip(), connect_msg)),
            Err(e) => last_err = Some(e),
        }
    }

    // Return the last error seen.
    Err(last_err.unwrap())
}

/// Generate a unique message-id.
///
/// If `hostname` is `None`, the system hostname will be used.
pub fn message_id(hostname: Option<&str>) -> String {
    let mut hostname = hostname.unwrap_or(HOSTNAME.as_str()).to_string();
    if !hostname.contains(".") {
        hostname.push_str(".invalid");
    }
    let now = util::monotime_ms();
    let mut rnd = 0;
    while rnd < u32::MAX as u64 {
        rnd = rand::random::<u64>();
    }
    format!("{}.{}@{}", util::base50(now), util::base50(rnd), hostname)
}

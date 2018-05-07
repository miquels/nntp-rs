//! NNTP wire protocol implementation.
use std;
use std::io;
use std::io::{Read,Write};

use memchr;

/// A NntpStream wraps a TcpStream and adds buffering, timeout,
/// command-line reading, etc.
pub struct NntpStream<S> {
    inner:      S,
    rbuffer:    Vec<u8>,
    rbufpos:    usize,
}

impl<S: Read + Write> NntpStream<S> {

    /// Return a new NntpStream.
    pub fn new(inner: S) -> NntpStream<S> {
        NntpStream{
            inner:  inner,
            rbuffer: Vec::with_capacity(1024),
            rbufpos: 0,
        }
    }

    fn shift_rbuffer(&mut self) {
        let todo = self.rbuffer.len() - self.rbufpos;
        if todo == 0 {
            self.rbuffer.truncate(0);
            self.rbufpos = 0;
        } else {
            unsafe {
                use std::ptr::copy;
                copy(self.rbuffer.as_ptr().offset(self.rbufpos as isize), self.rbuffer.as_mut_ptr(), todo);
                self.rbuffer.set_len(todo);
            }
            self.rbufpos = 0;
        }
    }

    // read function that first copies in whatever we have left in our
    // internal buffer, then (if space is left) calls inner.read.
    pub fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {

        if self.rbufpos == self.rbuffer.len() {
            return self.inner.read(&mut buf);
        }

        // paste in buffered data.
        let bufleft = &self.rbuffer[self.rbufpos..];
        let m = std::cmp::min(buf.len(), bufleft.len());
        self.rbufpos += m;
        let (left, mut right) = buf.split_at_mut(m);
        left.clone_from_slice(&bufleft[..m]);

        // if incoming buffer is full now return
        if right.len() == 0 {
            return Ok(left.len());
        }

        // there's some more space in the buffer to read into
        match self.inner.read(&mut right) {
            Ok(n) => Ok(n + m),
            Err(_) => Ok(m),
        }
    }

    /// Put data back into the buffer.
    pub fn unread(&mut self, buf: &[u8]) {
        self.shift_rbuffer();
        self.rbuffer.extend_from_slice(buf);
    }

    /// Read one line.
    ///
    /// Returns normal IO errors, plus possibly:
    /// - io::ErrorKind::UnexpectedEof: underlying stream has reached EOF
    /// - io::ErrorKind::InvalidData: input overflow (> 1000 bytes).
    ///
    pub fn read_line(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {

        let mut done = 0;
        let mut nlseen = false;

        while !nlseen {

            // fill buffer if needed.
            if self.rbufpos == self.rbuffer.len() {
                self.rbufpos = 0;
                let mut cap = self.rbuffer.capacity();
                if cap > 8192 {
                    cap = 8192;
                }
                unsafe { self.rbuffer.set_len(cap); }
                let n = match self.inner.read(&mut self.rbuffer[..]) {
                    Ok(n) => n,
                    Err(e) => {
                        self.rbuffer.truncate(0);
                        return Err(e);
                    },
                };
                self.rbuffer.truncate(n);
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UnexpectedEof"));
                }
            }

            // find newline.
            let n = match memchr::memchr(b'\n', &self.rbuffer[self.rbufpos..]) {
                Some(z) => {
                    nlseen = true;
                    z + 1
                },
                None => self.rbuffer.len() - self.rbufpos,
            };
            done += n;
            if done <= 1000 {
                buf.extend_from_slice(&self.rbuffer[self.rbufpos .. self.rbufpos + n]);
            }
            self.rbufpos += n;
        }

        if done <= 1000 {
            Ok(done)
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Overflow"))
        }
    }

    /// Like read_cmd, but into a String.
    pub fn read_line_string(&mut self, s: &mut String) -> io::Result<usize> {
        let mut buf = unsafe { s.as_mut_vec() };
        let pos = buf.len();
        match self.read_line(&mut buf) {
            Err(e) => {
                debug!("read_cmd_string: Err({})", e);
                buf.truncate(pos);
                Err(e)
            },
            Ok(n) => {
                match std::str::from_utf8(&buf[pos..]) {
                    Ok(_) => {
                        debug!("read_line_string: {}",
                               std::str::from_utf8(&buf[pos..]).unwrap().trim_right());
                        Ok(n)
                    },
                    Err(_) => {
                        debug!("read_line_string: Err(InvalidUTF8)");
                        buf.truncate(pos);
                        Err(io::Error::new(io::ErrorKind::InvalidData,
                                       "stream did not contain valid UTF-8"))
                    }
                }
            }
        }
    }
}

impl<S: Read + Write> Write for NntpStream<S> {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
/*
impl<'a, S: Read + Write> Write for &'a mut NntpStream<S> {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
*/

/// Reads data into a set of fized-size buffers.
/// You can also set a maximum size, read_all() returns an io::Error of
/// kind InvalidData if there is an overflow.
pub struct DataReader<R> {
    inner:      R,
    maxsize:    usize,
    firstchunk: usize,
    nextchunks: usize,
}

impl<R: Read> DataReader<R> {

    /// Constructor.
    pub fn new(r: R, maxsize: usize, firstchunk: usize, nextchunks: usize) -> DataReader<R> {
        DataReader{
            inner:  r,
            maxsize:    maxsize,
            firstchunk: firstchunk,
            nextchunks: nextchunks,
        }
    }

    /// Read data into a set of buffers.
    pub fn read_all(&mut self, bufs: &mut Vec<Vec<u8>>) -> io::Result<usize> {

        let mut done = 0;
        let mut curbuf = 0;
        let mut bufpos = 0;

        if bufs.len() == 0 {
            bufs.push(Vec::with_capacity(self.firstchunk));
            unsafe { bufs[0].set_len(self.firstchunk); }
        } else {
            bufpos = bufs[0].len();
            let cap = bufs[0].capacity();
            unsafe { bufs[0].set_len(cap); }
        }

        loop {
            if bufpos == bufs[curbuf].len() {
                if done < self.maxsize {
                    bufs.push(Vec::with_capacity(self.nextchunks));
                    curbuf = bufs.len() - 1;
                    unsafe { bufs[curbuf].set_len(self.nextchunks); }
                }
                bufpos = 0;
            }
            let n = match self.inner.read(&mut bufs[curbuf][bufpos..]) {
                Err(e) => {
                    bufs[curbuf].truncate(bufpos);
                    return Err(e);
                },
                Ok(n) => n,
            };
            if n == 0 {
                bufs[curbuf].truncate(bufpos);
                break;
            }
            done += n;
            bufpos += n;
        }

        if done > self.maxsize && self.maxsize != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Overflow"));
        }
        Ok(done)
    }
}


/// Read adapter that reads until DOT CRLF.
pub struct DotReader<'a, S: Read + Write + 'a> {
    inner:      &'a mut NntpStream<S>,
    state:      RState,
}

#[derive(Debug,PartialEq,Eq)]
enum RState {
    Data,
    Cr1Seen,
    Lf1Seen,
    DotSeen,
    Cr2Seen,
    Lf2Seen,
}

impl<'a, S: Read + Write> DotReader<'a, S> {
    /// Constructor.
    pub fn new(s: &'a mut NntpStream<S>) -> DotReader<S> {
        DotReader{
            inner:      s,
            state:      RState::Lf1Seen,
        }
    }
}

impl<'a, S: Read + Write> Read for DotReader<'a, S> {

    /// Reads from the underlying NntpStream until DOT CRLF is seen.
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {

        // If we're done return 0, which indicates EOF.
        if self.state == RState::Lf2Seen {
            return Ok(0);
        }

        // Read data.
        let n = self.inner.read(&mut buf)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UnexpectedEof"));
        }
        let mut i = 0;

        // State machine.
        while i < n {
            match self.state {
                RState::Data => {
                    match memchr::memchr(b'\r', &buf[i..n]) {
                        Some(z) => {
                            self.state = RState::Cr1Seen;
                            i += z + 1;
                        },
                        None => {
                            i = n;
                        }
                    }
                },
                RState::Cr1Seen => {
                    self.state = match buf[i] {
                        b'\n' => RState::Lf1Seen,
                        b'\r' => RState::Cr1Seen,
                        _ => RState::Data,
                    };
                    i += 1;
                },
                RState::Lf1Seen => {
                    self.state = match buf[i] {
                        b'.' => RState::DotSeen,
                        b'\r' => RState::Cr1Seen,
                        _ => RState::Data,
                    };
                    i += 1;
                },
                RState::DotSeen => {
                    self.state = match buf[i] {
                        b'\r' => RState::Cr2Seen,
                        _ => RState::Data,
                    };
                    i += 1;
                },
                RState::Cr2Seen => {
                    self.state = match buf[i] {
                        b'\n' => RState::Lf2Seen,
                        b'\r' => RState::Cr1Seen,
                        _ => RState::Data,
                    };
                    i += 1;
                },
                RState::Lf2Seen => {
                    break;
                },
            }
        }

        if i < n {
            // still some data left, buffer it.
            self.inner.unread(&buf[i..n]);
        }
        Ok(i)
    }
}

/// Writer that does dotstuffing.
pub struct DotWriter<W: Write> {
    inner:      W,
    state:      WState,
}

#[derive(Debug,PartialEq,Eq)]
enum WState {
    Data,
    LfSeen,
    Eof,
}

impl<W: Write> DotWriter<W> {

    /// Return a new DotWriter.
    pub fn new(inner: W) -> DotWriter<W> {
        DotWriter{
            inner:  inner,
            state:  WState::LfSeen,
        }
    }

    /// Call this after being done writing, it writes a final DOT CRLF
    /// If you don't call this first it will be called on drop.
    pub fn done(&mut self) -> io::Result<(usize)> {
        let d = match self.state {
            WState::Data => &b"\r\n.\r\n"[..],
            WState::LfSeen => &b".\r\n"[..],
            WState::Eof => return Ok(0),
        };
        self.state = WState::Eof;
        self.inner.write_all(d)?;
        self.flush()?;
        Ok(d.len())
    }
}

impl<W: Write> Drop for DotWriter<W> {
    fn drop(&mut self) {
		self.done().ok();
	}
}

impl<W: Write> Write for DotWriter<W> {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {

        let mut written = 0;
        let mut i = 0;
        let n = buf.len();

        while i < n {
            match self.state {
                WState::Data => {
                    match memchr::memchr(b'\n', &buf[i..n]) {
                        Some(z) => {
                            self.inner.write_all(&buf[i..i+z-1])?;
                            self.inner.write_all(b"\r\n")?;
                            i += z;
                            written += z + 1;
                            self.state = WState::LfSeen;
                        },
                        None => {
                            self.inner.write_all(&buf[i..n])?;
                            let w = n - i;
                            written += w;
                            i += w;
                        },
                    }
                },
                WState::LfSeen => {
                    if buf[i] == b'.' {
                        self.inner.write_all(b"..")?;
                        i += 1;
                        written += 2;
                    }
                    self.state = WState::Data;
                },
                WState::Eof => break,
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}


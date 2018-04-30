//! NNTP wire protocol implementation.
use std;
use std::io;
use std::io::Read;

use memchr;

/// a reader with 2 modes: line and data.
pub struct NntpReader<R> {
    inner:      R,
    mode:       Mode,
    state:      State,
    buffer:     Vec<u8>,
    bufpos:     usize,
    maxsz:      usize,
    done:       usize,
}

#[derive(Debug,PartialEq,Eq)]
enum State {
    Data,
    Cr1Seen,
    Lf1Seen,
    DotSeen,
    Cr2Seen,
    Lf2Seen,
}

#[derive(Debug,PartialEq,Eq)]
enum Mode {
    Normal,
    Cmd,
    Data,
}

impl<R: Read> NntpReader<R> {

    /// Return a new NntpReader. Initial mode is command mode.
    pub fn new(inner: R) -> NntpReader<R> {
        NntpReader{
            inner:  inner,
            mode:   Mode::Normal,
            state:  State::Lf1Seen,
            buffer: Vec::new(),
            bufpos: 0,
            maxsz:  1000,
            done:   0,
        }
    }

    /// Switch to data mode.
    pub fn mode_data(&mut self, maxsz: usize) {
        self.mode = Mode::Data;
        self.state = State::Lf1Seen;
        self.maxsz = if maxsz > 0 { maxsz } else { 64000 };
        self.done = 0;
    }

    /// Switch to command mode.
    pub fn mode_cmd(&mut self) {
        self.mode = Mode::Cmd;
        self.state = State::Data;
        self.maxsz = 1000;
        self.done = 0;
    }

    /// Switch to normal mode.
    pub fn mode_normal(&mut self) {
        self.mode = Mode::Normal;
    }

    // read function that first copies in whatever we have left in our
    // internal buffer, then (if space is left) calls inner.read.
    fn bread(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {

        if self.bufpos == self.buffer.len() {
            return self.inner.read(&mut buf);
        }

        // paste in buffered data.
        let bufleft = &self.buffer[self.bufpos..];
        let m = std::cmp::min(buf.len(), bufleft.len());
        self.bufpos += m;
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

    /// Read a command aka one line. Line has to end with CRLF.
    /// The final CRLF is not included in the return buffer.
    ///
    /// Returns normal IO errors, plus possibly:
    /// - io::ErrorKind::UnexpectedEof: underlying stream has reached EOF
    /// - io::ErrorKind::InvalidData: input line > 1000 bytes.
    ///
    pub fn read_cmd(&mut self, mut buf: &mut Vec<u8>) -> io::Result<usize> {
        self.mode_cmd();
        buf.truncate(0);
        let mut n = self.read_to_end(&mut buf)?;
        if buf.ends_with(b"\r\n") {
            let len = buf.len();
            buf.truncate(len - 2);
            n -= 2;
        }
        Ok(n)
    }

    /// Like read_cmd, but into a String.
    ///
    /// Returns IO errors like read_cmd, plus possibly:
    /// - io::ErrorKind::InvalidData: non-utf8 data found in input
    ///
    pub fn read_cmd_string(&mut self, s: &mut String) -> io::Result<usize> {
        let mut buf = unsafe { s.as_mut_vec() };
        let pos = buf.len();
        match self.read_cmd(&mut buf) {
            Err(e) => {
                debug!("read_cmd_string: Err({})", e);
                buf.truncate(pos);
                Err(e)
            },
            Ok(n) => {
                if std::str::from_utf8(&buf[pos..]).is_err() {
                    debug!("read_cmd_string: Err(InvalidUTF8)");
                    buf.truncate(pos);
                    Err(io::Error::new(io::ErrorKind::InvalidData,
                                       "stream did not contain valid UTF-8"))
                } else {
                    buf.truncate(pos + n);
                    debug!("read_cmd_string: {}", std::str::from_utf8(&buf[pos..]).unwrap());
                    Ok(n)
                }
            },
        }
    }

    /// Read data into a databuffer, until CRLF.CRLF Does not do dot-escaping.
    ///
    /// If bufs[0] is already present, it will be used for the first chunk.
    /// As soon as the current buffer is full, a new buffer of 128KB will be
    /// allocated.
    ///
    /// Returns normal IO errors, plus possibly:
    /// - io::ErrorKind::UnexpectedEof: underlying stream has reached EOF
    /// - io::ErrorKind::InvalidData: input line > 1000 bytes.
    ///
    pub fn read_data(&mut self, bufs: &mut Vec<Vec<u8>>, maxsz: usize) -> io::Result<usize> {

        self.mode_data(maxsz);

        let mut done = 0;
        let mut curbuf = 0;
        let mut bufpos = 0;

        if bufs.len() > 0 {
            bufpos = bufs[0].len();
            let cap = bufs[0].capacity();
            unsafe { bufs[0].set_len(cap); }
        }

        loop {
            if bufs.len() == 0 || bufpos == bufs[curbuf].len() {
                let cap = 128*1024;
                bufs.push(Vec::with_capacity(cap));
                curbuf = bufs.len() - 1;
                bufpos = 0;
                unsafe { bufs[curbuf].set_len(cap); }
            }
            let n = match self.read(&mut bufs[curbuf][bufpos..]) {
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
        Ok(done)
    }

    /// Reads a chunk of data from the NNTP stream. Behaviour depends on the mode:
    /// - mode_normal():  normal read.
    /// - mode_data(): reads until CRLF . CRLF
    /// - mode_cmd(): reads until CRLF
    ///
    /// In data or cmd mode, returns Ok(0) aka EOF when done. At that point the
    /// NntpReader needs to be reset for the next round using mode_normal,
    /// mode_cmd() or mode_data().
    ///
    /// So, one command can be read by using reader.read_to_end(&mut Vec<u8>)
    ///
    /// Returns normal IO errors, plus a few extra of kind:
    /// - io::ErrorKind::UnexpectedEof: underlying stream has reached EOF
    /// - io::ErrorKind::InvalidData: input too large (overflow)
    ///
    pub fn nntp_read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {

        if self.mode == Mode::Normal {
            return self.inner.read(&mut buf);
        }

        // If we're done return 0, which indicates EOF.
        if self.state == State::Lf2Seen {
            if self.done > self.maxsz {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "overflow"));
            }
            return Ok(0);
        }

        // Read data.
        let n = self.bread(&mut buf)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UnexpectedEof"));
        }
        let mut i = 0;

        // State machine.
        while i < n {
            match self.state {
                State::Data => {
                    match memchr::memchr(b'\r', &buf[i..n]) {
                        Some(z) => {
                            self.state = State::Cr1Seen;
                            i += z + 1;
                        },
                        None => {
                            i = n;
                        }
                    }
                },
                State::Cr1Seen => {
                    self.state = match buf[i] {
                        b'\n' => State::Lf1Seen,
                        b'\r' => State::Cr1Seen,
                        _ => State::Data,
                    };
                    i += 1;
                    if self.mode == Mode::Cmd && self.state == State::Lf1Seen {
                        self.state = State::Lf2Seen;
                        break;
                    }
                },
                State::Lf1Seen => {
                    self.state = match buf[i] {
                        b'.' => State::DotSeen,
                        b'\r' => State::Cr1Seen,
                        _ => State::Data,
                    };
                    i += 1;
                },
                State::DotSeen => {
                    self.state = match buf[i] {
                        b'\r' => State::Cr2Seen,
                        _ => State::Data,
                    };
                    i += 1;
                },
                State::Cr2Seen => {
                    self.state = match buf[i] {
                        b'\n' => State::Lf2Seen,
                        b'\r' => State::Cr1Seen,
                        _ => State::Data,
                    };
                    i += 1;
                },
                State::Lf2Seen => {
                    break;
                },
            }
        }

        if i < n && self.bufpos == self.buffer.len() {
            // still some data left, buffer it.
            self.buffer.truncate(0);
            self.bufpos = 0;
            self.buffer.extend_from_slice(&buf[i..n]);
        }

        if self.done > self.maxsz {
            return Ok(1)
        }

        Ok(i)
    }
}

impl<R: Read> Read for NntpReader<R> {
    /// Simply forwards to nntp_read.
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        self.nntp_read(&mut buf)
    }
}


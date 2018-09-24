use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::str;

use bytes::BytesMut;
use memchr::memchr;

// helper macro to build header names and hashmap.
macro_rules! nntp_headers {
    {$(($variant:ident, $name:expr)),*} => {
        /// Well-known header names.
        #[repr(usize)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum HeaderName { $($variant),* }

        fn build_header_enums() -> HashMap<Vec<u8>, HeaderName> {
            let mut hm = HashMap::new();
            $( hm.insert($name.to_lowercase().into_bytes(), HeaderName::$variant); )*
            hm
        }

        fn build_header_names() -> HashMap<HeaderName, &'static [u8]> {
            let mut hm = HashMap::new();
            $( hm.insert(HeaderName::$variant, $name.as_bytes()); )*
            hm
        }
    }
}

nntp_headers! {
    ( Bytes,        "Bytes" ),
    ( Control,      "Control" ),
    ( Date,         "Date" ),
    ( Distribution, "Distribution" ),
    ( From,         "From" ),
    ( Lines,        "Lines" ),
    ( MessageId,    "Message-ID" ),
    ( Newsgroups,   "Newsgroups" ),
    ( Path,         "Path" ),
    ( References,   "References" ),
    ( Subject,      "Subject" ),
    ( Supersedes,   "Supersedes" ),
    ( Xref,         "Xref" ),
    ( Other,        "" )
}

const MANDATORY_HEADERS: [HeaderName; 6] = [
    HeaderName::Date,
    HeaderName::From,
    HeaderName::MessageId,
    HeaderName::Newsgroups,
    HeaderName::Path,
    HeaderName::Subject,
];

// initialize globals.
lazy_static! {
    static ref HEADER_ENUMS: HashMap<Vec<u8>, HeaderName> = build_header_enums();
    static ref HEADER_NAMES: HashMap<HeaderName, &'static [u8]> = build_header_names();
}

#[derive(Debug)]
struct HeaderPos {
    modified:   bool,
    header:     Range<usize>,
    name:       Range<usize>,
    value:      Range<usize>,
}

impl HeaderPos {
    fn new() -> HeaderPos {
        HeaderPos{
            modified:   false,
            header:     Range{ start: 0, end: 0 },
            name:       Range{ start: 0, end: 0 },
            value:      Range{ start: 0, end: 0 },
        }
    }
}

/// Headers parser.
#[derive(Default,Debug)]
pub struct HeadersParser {
    buf:            BytesMut,
    modbuf:         BytesMut,
    hpos:           Vec<HeaderPos>,
    hlen:           usize,
    well_known:     [Option<u32>; HeaderName::Other as usize],
    ok:             bool,
}

/// Complete parsed headers.
#[derive(Debug)]
pub struct Headers(HeadersParser);

impl HeadersParser {

    /// Return a new HeadersParser.
    pub fn new() -> HeadersParser {
        HeadersParser::default()
    }

    /// Parse a &[u8] buffer into header information.
    ///
    /// Returns None if the buffer does not contain a complete header
    /// yet, indicating that perhaps more data needs to be read
    /// from the network.
    ///
    /// Returns Some(Ok(len)) where len is the length of the header, up
    /// to but not including the empty line seperating header and body.
    ///
    /// Returns Some(Err(e)) if there was a parse error.
    pub fn parse(&mut self, buf: &[u8], last: bool) -> Option<io::Result<u64>> {

        // Parse into NL delimited lines.
        let nlines = buf.len() / 30;
        let mut lines : Vec<Range<usize>> = Vec::with_capacity(nlines);
        let mut pos = 0usize;
        loop {
            let nl = match memchr(b'\n', &buf[pos..]) {
                Some(nl) => {
                    // empty lines signals end-of-headers.
                    if nl == 0 || (nl == 1 && buf[pos] == b'\r') {
                        break;
                    }
                    nl
                },
                None => {
                    // Not complete yet? Try again later.
                    if !last {
                        return None;
                    }
                    // End of data here means end-of-header.
                    if pos == buf.len() {
                        break;
                    }
                    // Well this was unexpected.
                    return Some(Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")));
                },
            };

            // end-of-header? break.
            if nl == 0 || (nl == 1 && buf[pos] == b'\r') {
                break;
            }

            lines.push(Range{ start: pos, end: pos + nl + 1});
            pos += nl + 1;
        }

        // take ref only once
        let header_enums = &*HEADER_ENUMS;

        // Parse headers one by one.
        let mut idx = 0;
        let len = lines.len();
        self.hpos.reserve_exact(len);
        while idx < len {

            // might be a multi-line (continued) header.
            let mut header_idx = lines[idx].clone();
            while idx + 1 < len && is_cont(&buf[lines[idx+1].clone()]) {
                idx += 1;
            }
            header_idx.end = lines[idx].end;
            idx += 1;

            // get header name.
            let b = &buf[header_idx.clone()];
            let mut p = match memchr(b':', b) {
                None => {
                    return Some(Err(io::Error::new(io::ErrorKind::InvalidData, "mangled headers")));
                },
                Some(p) => p,
            };
            if p == 0 || b.len() < 4 {
                return Some(Err(io::Error::new(io::ErrorKind::InvalidData, "mangled headers")));
            }
            let hname_idx = Range{ start: header_idx.start, end: header_idx.start + p };
            p += 1;

            // find start of header value.
            while p < b.len() {
                if b[p] != b' ' && b[p] != b'\t' && b[p] != b'\r' && b[p] != b'\n' {
                    break;
                }
                p += 1;
            }
            if p == b.len() {
                return Some(Err(io::Error::new(io::ErrorKind::InvalidData, "mangled headers")));
            }
            let hvalue_idx = Range{ start: header_idx.start + p, end: header_idx.end };

            // is this a well-known header?
            let mut tmpbuf = [0u8; 32];
            let lc = lowercase(&buf[hname_idx.clone()], &mut tmpbuf[..]);
            if let Some(wk) = header_enums.get(lc) {
                if self.well_known[*wk as usize].is_some() {
                    return Some(Err(io::Error::new(io::ErrorKind::InvalidData,
                                        format!("duplicate {} header", header_name(wk)))));
                }
                self.well_known[*wk as usize] = Some(self.hpos.len() as u32);
            }

            // add this header to the list.
            let hpos = HeaderPos{
                modified:   false,
                header:     header_idx,
                name:       hname_idx,
                value:      hvalue_idx,
            };
            self.hpos.push(hpos);
        }
        self.hlen = pos;

        // check for all mandatory headers.
        for wk in &MANDATORY_HEADERS {
            if self.well_known[*wk as usize].is_none() {
                return Some(Err(io::Error::new(io::ErrorKind::InvalidData,
                                        format!("missing {} header", header_name(wk)))));
            }
        }

        self.ok = true;
        Some(Ok(pos as u64))
    }

    /// This method consumes self and the BytesMut with the header data,
    /// and returns a Header and the remaining data (e.g. the body).
    pub fn into_headers(mut self, buffer: BytesMut) -> (Headers, BytesMut) {
        if !self.ok {
            panic!("HeadersParser::parse() returned error, you can't call into_headers()!");
        }
        self.buf = buffer;
        let mut ret = self.buf.split_off(self.hlen);
        // eat header-body separator.
        if ret.len() >= 2 && &ret[..2] == &b"\r\n"[..] {
            ret.advance(2);
        } else if ret.len() >= 1 && ret[0] == b'\n' {
            ret.advance(1);
        }
        (Headers(self), ret)
    }
}

impl Headers {

    // where is this header in our self.hpos vector.
    fn get_hpos_idx(&self, name: HeaderName) -> Option<usize> {
        let idx = match name {
            HeaderName::Other => return None,
            i => i as usize,
        };
        self.0.well_known[idx].map(|i| i as usize)
    }

    // return a reference to the HeaderPos of this header.
    fn get_hpos(&self, name: HeaderName) -> Option<&HeaderPos> {
        self.get_hpos_idx(name).map(|i| &self.0.hpos[i])
    }

    /// Get the value of a header as bytes.
    pub fn get(&self, name: HeaderName) -> Option<&[u8]> {
        let hdr = self.get_hpos(name)?;
        Some(if hdr.modified {
            &self.0.modbuf[hdr.value.clone()]
        } else {
            &self.0.buf[hdr.value.clone()]
        })
    }

    /// Get the value of a header as an UTF-8 string.
    ///
    /// If the header value is not valid utf-8, we translate bytes > 126
    /// into their Unicode code points. That will work as long as the header
    /// value is iso-8859-1, for other encodings you are SOL.
    pub fn get_str<'a>(&'a self, name: HeaderName) -> Option<Cow<'a, str>> {
        let hdr = self.get(name)?;
        Some(match str::from_utf8(hdr) {
            Ok(s) => Cow::from(s),
            Err(_) => {
                let mut s = String::new();
                for b in hdr.iter() {
                    s.push(*b as char);
                }
                Cow::from(s)
            }
        })
    }

    /// Update the value of a header. This means replace-or-append.
    pub fn update(&mut self, name: HeaderName, value: &[u8]) {
        let i = match self.get_hpos_idx(name) {
            Some(i) => i,
            None => {
                let h = HeaderPos::new();
                self.0.hpos.push(h);
                self.0.hpos.len() -1
            },
        };
        let name = HEADER_NAMES[&name];
        let start = self.0.modbuf.len();
        let end = start + name.len() + value.len() + 4;

        self.0.modbuf.reserve(end - start);
        self.0.modbuf.extend_from_slice(name);
        self.0.modbuf.extend_from_slice(&b": "[..]);
        self.0.modbuf.extend_from_slice(value);
        self.0.modbuf.extend_from_slice(&b"\r\n"[..]);

        self.0.hpos[i] = HeaderPos{
            modified:   true,
            header:     Range{ start, end },
            name:       Range{ start: start, end: start + name.len() },
            value:      Range{ start: start + name.len() + 2, end: end },
        };
    }

    // Count, and perhaps write out the header into the supplied buffer.
    fn do_header_bytes(&self, buffer: &mut BytesMut, doit: bool) -> usize {
        let mut size = 0;
        for h in &self.0.hpos {
            size += h.header.end - h.header.start;
        }
        if size < 2 {
            return 0;
        }
        if doit {
            buffer.reserve(size + 3);
        }

        // add all the headers to the buffer.
        for h in &self.0.hpos {
            let v = if h.modified {
                &self.0.modbuf[h.header.clone()]
            } else {
                &self.0.buf[h.header.clone()]
            };
            size += v.len();
            if doit {
                buffer.extend_from_slice(v);
            }

            // This is to fix a bug in the old diablo spool where
            // sometimes a headerline ends in a bare \n
            let len = buffer.len();
            if buffer[len - 2] != b'\r' && buffer[len - 1] == b'\n' {
                if doit {
                    buffer[len - 1] = b'\r';
                    buffer.extend_from_slice(&b"\n"[..]);
                }
                size  += 1;
            }
        }
        size
    }

    /// Write out the header into the supplied buffer.
    pub fn header_bytes(&self, buffer: &mut BytesMut) {
        self.do_header_bytes(buffer, true);
    }

    /// Length of the header section.
    pub fn len(&self) -> usize {
        let mut bm = BytesMut::new();
        self.do_header_bytes(&mut bm, false)
    }

    /// Number of path elements.
    pub fn path_count(&self) -> usize {
        match self.get(HeaderName::Path) {
            None => 0,
            Some(p) => p.iter().filter(|p| *p == &b'!').count() + 1,
        }
    }

    /// Number of newsgroups.
    pub fn newsgroups_count(&self) -> usize {
        match self.get(HeaderName::Newsgroups) {
            None => 0,
            Some(p) => p.iter().filter(|p| *p == &b',').count() + 1,
        }
    }
}

// helper.
#[inline]
fn is_cont(line: &[u8]) -> bool {
    line.len() > 0 && (line[0] == b' ' || line[0] == b'\t')
}

// helper
fn header_name(wk: &HeaderName) -> &str {
    str::from_utf8(HEADER_NAMES[wk]).unwrap()
}

// cheap ASCII lowercasing.
fn lowercase<'a>(b: &'a [u8], buf: &'a mut [u8]) -> &'a [u8] {
    let mut idx = 0;
    for i in 0..b.len() {
        if i == buf.len() {
            return b;
        }
        let mut c = b[idx];
        if c >= b'A' && c <= b'Z' {
            c += 32;
        }
        buf[idx] = c;
        idx += 1;
    }
    &buf[..idx]
}

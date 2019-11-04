/// List of reasons why we didn't accept the article.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtError {
    /// after receiving article we already had it anyway.
    PostDuplicate,
    /// article is incomplete
    ArtIncomplete,
    /// if headers + body < 80 chars
    TooSmall,
    /// too big
    TooBig,
    /// message is too old (checks Date: header)
    TooOld,
    /// header-only feed article is missing Bytes: header.
    HdrOnlyNoBytes,
    /// matched "filter" in dnewsfeeds entry
    GroupFilter,
    /// matched IFILTER label
    IncomingFilter,
    /// hit internal spam filter
    InternalSpamFilter,
    /// hit external spam filter
    ExternalSpamFilter,
    /// rejected by the spool
    RejSpool,
    /// no spool accepted this article
    NoSpool,
    /// error writing file
    FileWriteError,
    /// other I/O error.
    IOError,
    /// Missing header/body separator
    NoHdrEnd,
    /// header too big to be sane.
    HeaderTooBig,
    /// No colon in header or invalid header name.
    BadHdrName,
    /// Duplicate header
    DupHdr,
    /// Empty header
    EmptyHdr,
    /// Invalid utf-8 in header
    BadUtf8Hdr,
    /// Weird Message-ID
    BadMsgId,
    /// nntp and article message-id mismatch
    MsgIdMismatch,
    /// mandatory header is missing
    MissingHeader,
    /// Newsgroups: header present but empty
    NoNewsgroups,
    /// Path: header present but empty or invalid.
    NoPath,
    /// Not in active file XXX TODO
    NotInActive,
    /// Tab found in Path: XXX TODO
    PathTab,
    /// Tab found in Newsgroups: XXX TODO
    NewsgroupsTab,
    /// NUL found in article XXX TODO
    ArticleNul,
    /// Bare CR found in article XXX TODO
    BareCR,
}

pub type ArtResult<T> = Result<T, ArtError>;

macro_rules! _ioerr {
    ($kind:expr, $arg:expr) => (
        std::io::Error::new($kind, $arg)
    );
}
    
macro_rules! ioerr {
    (@NotFound, $arg:expr) => ( _ioerr!(std::io::ErrorKind::NotFound, $arg) );
    (@PermissionDenied, $arg:expr) => ( _ioerr!(std::io::ErrorKind::PermissionDenied, $arg) );
    (@ConnectionRefused, $arg:expr) => ( _ioerr!(std::io::ErrorKind::ConnectionRefused, $arg) );
    (@ConnectionReset, $arg:expr) => ( _ioerr!(std::io::ErrorKind::ConnectionReset, $arg) );
    (@ConnectionAborted, $arg:expr) => ( _ioerr!(std::io::ErrorKind::ConnectionAborted, $arg) );
    (@NotConnected, $arg:expr) => ( _ioerr!(std::io::ErrorKind::NotConnected, $arg) );
    (@AddrInUse, $arg:expr) => ( _ioerr!(std::io::ErrorKind::AddrInUse, $arg) );
    (@AddrNotAvailable, $arg:expr) => ( _ioerr!(std::io::ErrorKind::AddrNotAvailable, $arg) );
    (@BrokenPipe, $arg:expr) => ( _ioerr!(std::io::ErrorKind::BrokenPipe, $arg) );
    (@AlreadyExists, $arg:expr) => ( _ioerr!(std::io::ErrorKind::AlreadyExists, $arg) );
    (@WouldBlock, $arg:expr) => ( _ioerr!(std::io::ErrorKind::WouldBlock, $arg) );
    (@InvalidInput, $arg:expr) => ( _ioerr!(std::io::ErrorKind::InvalidInput, $arg) );
    (@InvalidData, $arg:expr) => ( _ioerr!(std::io::ErrorKind::InvalidData, $arg) );
    (@TimedOut, $arg:expr) => ( _ioerr!(std::io::ErrorKind::TimedOut, $arg) );
    (@WriteZero, $arg:expr) => ( _ioerr!(std::io::ErrorKind::WriteZero, $arg) );
    (@Interrupted, $arg:expr) => ( _ioerr!(std::io::ErrorKind::Interrupted, $arg) );
    (@Other, $arg:expr) => ( _ioerr!(std::io::ErrorKind::Other, $arg) );
    (@UnexpectedEof, $arg:expr) => ( _ioerr!(std::io::ErrorKind::UnexpectedEof, $arg) );
    (@$kind:expr, $arg:expr) => ( _ioerr!($kind, $arg) );

    ($kind:ident, $fmt:expr, $($tt:tt)+) => (
        ioerr!(@$kind, format!($fmt, $($tt)+))
    );
    ($kind:expr, $fmt:expr, $($tt:tt)+) => (
        ioerr!(@$kind, format!($fmt, $($tt)+))
    );
    ($kind:ident, $arg:expr) => (
        ioerr!(@$kind, $arg)
    );
    ($kind:expr, $arg:expr) => (
        ioerr!(@$kind, $arg)
    );
}


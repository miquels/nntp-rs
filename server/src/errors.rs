
/// List of reasons why we didn't accept the article.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum ArtError {
    /// after receiving article we already had it anyway.
    PostDuplicate,
    /// racing another article.
    Deferred,
    /// article is incomplete
    ArtIncomplete,
    /// if headers + body < 80 chars
    TooSmall,
    /// too big
    TooBig,
    /// no matching group found in active file.
    NotInActive,
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
    /// matched a "dontstore" in the spool. this is not an error
    /// really, but we log it in the incoming.log file.
    DontStore,
    /// No colon in header or invalid header name.
    BadHdrName,
    /// Duplicate header
    DupHdr,
    /// Empty header
    EmptyHdr,
    /// Invalid utf-8 in header
    BadUtf8Hdr,
    /// nntp and article message-id mismatch
    MsgIdMismatch,
    /// mandatory header is missing
    MissingHeader,
    /// Newsgroups: header present but empty
    NoNewsgroups,
    /// Path: header present but empty or invalid.
    NoPath,
}

pub type ArtResult<T> = Result<T, ArtError>;


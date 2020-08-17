
## TODO

A list in no particular order.

### Spool

- implement diablo spool expire
- implement cyclic

### History

- new history file format, larger hash, larger offsets, multiple files.
- keep history file in memory option (mmap)

### Feed

- hostname / xrefhost / pathhost / commonpath
- welcome message
- readonly

### Other

- keep active file
- xref generation on/off

### article.rs

- Move Headers into struct ParsedArticle. No separate headers and body buffers anymore.
- Constructors: `ParsedArticle::from_buffer`, `ParsedArticle::from_article` ?

- Maybe 3 different versions / states of an article:
  - ReceivedArticle: internal to `nntp_session.rs`
  - RawArticle: as read from storage, unparsed, maybe some basic info (hdronly, end of hdrs)
  - ParsedArticle


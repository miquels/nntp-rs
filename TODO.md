
## TODO

A list in no particular order.

### Spool

- spool read article: headers and body seperate. headers in r/w buffer,
  body is immutable and can thus optionally be mmmap'ed.
- diablo spool: open files in LRU list
- implement cyclic

### History

- new history file format, larger hash, larger offsets, multiple files.
- keep history file in memory option (mmap)

### General
- add periodic timer infra for hist expire etc

### Feed

- welcome message

### Outgoing
- don't start outgoing feed (or queue!) until first article
- connections: close after timeout (10 mins? not sure)
- close feed itself after idle?

### Stats
- keep global stats per peer
- make available through http, maybe prometheus format as well
- html stats page?

### Other

- keep active file
- xref generation on/off

### article.rs

- Move Headers into struct ParsedArticle. No separate headers and body buffers anymore.
  + => for incoming? outgoing separate headers/body is good, see above
- Constructors: `ParsedArticle::from_buffer`, `ParsedArticle::from_article` ?

- Maybe 3 different versions / states of an article:
  - ReceivedArticle: internal to `nntp_session.rs`
  - RawArticle: as read from storage, unparsed, maybe some basic info (hdronly, end of hdrs)
  - ParsedArticle

DONE:

### Spool

- implement diablo spool expire
  + done, wire it up

### Feed

- hostname / xrefhost / pathhost / commonpath
- welcome message
- readonly

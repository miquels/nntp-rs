
## TODO

A list in no particular order.

### article.rs

- Move Headers into struct ParsedArticle. No separate headers and body buffers anymore.
- Constructors: `ParsedArticle::from_buffer`, `ParsedArticle::from_article` ?

- Maybe 3 different versions / states of an article:
  - ReceivedArticle: internal to `nntp_session.rs`
  - RawArticle: as read from storage, unparsed, maybe some basic info (hdronly, end of hdrs)
  - ParsedArticle

### outfeed.rs

- Finish it


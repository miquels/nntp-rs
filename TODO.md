
# TODO

## Important

### History
- keep history file in memory option (mmap) (options: index only, all)

### General
- add periodic timer infra for hist expire etc

### Active file
- keep active file
- xref generation on/off

## Other in no particular order

### General

- do something on disk I/O errors (writing to spool or queue files)
  + spool/hist/log write error on incoming feed:
    - start to give errors on all connections that are not read-only
  + queue write error on outgoing feed:
    - start to give errors on all incoming connections that are not read-only
    - put outgoing feed that caused the error in paused mode
  + spool/hist read error on outgoing feed:
    - start to give errors on all incoming connections that are not read-only
    - put outgoing feed that caused the error in spool-to-backlog mode

  In all cases, set the global error state (an atomic) if there error has
  higher prio than the previous state. write-EIO > read-EIO > ENOSPC > EDQUOT > NoError

  + run a separate task that does a statfs every .. minute?
    - if any filesystems are 100% full, handle as "spool/hist write error on incoming feed".
    - if the global error state is "ENOSPC" and all filesystems have
      enough space again (define "enough?"), clear the error (cmpxchg)

- build in "inpaths" support.

### Spool
- spool read article: headers and body seperate. headers in r/w buffer,
  body is immutable and can thus optionally be mmmap'ed.
- diablo spool: open files in LRU list
- implement cyclic

### History
- new history file format, larger hash, larger offsets, multiple files.

### Feed
- welcome message

### Incoming
- might be worth it to keep peerfeeds on the multisingle runtime,
  but run readers on the threaded runtime? Need socket handoff
  from multisingle to threaded somehow.
- refactor xclient support.

### Outgoing
- keep "average pending" stats and log actual avpend instead of "1"
- "drop-deferred-large", "no-backlog-large", "feed-delay-large" settings
  that only trigger for articles > N KB (100 KB?)
- finer feed-delay granularity for < 1s ?

### Stats
- http access ?

### Other

- global atomic with an Instant, updated every X seconds, so we have a cheap time source.

### article.rs

- Move Headers into struct ParsedArticle. No separate headers and body buffers anymore.
  + => for incoming? outgoing separate headers/body is good, see above
- Constructors: `ParsedArticle::from_buffer`, `ParsedArticle::from_article` ?

- Maybe 3 different versions / states of an article:
  - ReceivedArticle: internal to `nntp_session.rs`
  - RawArticle: as read from storage, unparsed, maybe some basic info (hdronly, end of hdrs)
  - ParsedArticle

DONE:

### General
- detect changed config and reload (note: included files as well)

### Spool

- implement diablo spool expire
  + done, wire it up

### Feed

- hostname / xrefhost / pathhost / commonpath
- welcome message
- readonly

### Outgoing
- connections: close after timeout (1 mins)
- implement nobatch
- implement drop-deferred
- implement precomreject (as dont-defer)
- implement delayfeed
- don't start outgoing feed (or queue!) until first article
- close feed itself after idle?

### Stats
- keep global stats per peer
- maybe prometheus format as well

### Newsfeeds file
- add "$label" variable that can be used in hostname / inhost / etc
- make label itself meta, bash like, for example: nzspool{1..7}.xs4all.net? (no)
- templates

### Logger
- for syslog logging, do not re-initialize the syslogger every log line.


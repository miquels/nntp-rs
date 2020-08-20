
## nntp-rs

A NNTP server in Rust. Async, built with Tokio. Very much WIP.

Status:
- diablo-format history file
- diablo-format article spool
- uses its own diablo-influenced configuration format,
  but can read diablo "dnewsfeeds", "diablo.hosts" and "dspool.ctl" files.
- NNTP commands for capabilities **mandatory** and most of **READER** have been implemented
  (no active file or article numbering, yet, though).
- incoming and outgoing feeds
- header only feeds

# Runtimes.

The part of the server that handles incoming feeds can use two different
runtimes, based on the Tokio `basic` and `threaded` runtimes.

- **threaded**: based on the Tokio `threaded` runtime. All of the servers `tasks`
  (each task handles one connection, incoming or outgoing) are M:N scheduled
  over a bunch of threads.

- **multisingle**: a mix of multiple Tokio `basic` runtimes and one `threaded`
  runtime. You can configure N incoming threads. Each thread runs one instance
  of the `basic` scheduler, and can be pinned to one specific CPU core.
  Incoming connections are loadbalanced over the incoming threads (using `SO_REUSE_PORT`).
  This combined with ethernet NICs that support multiple IRQs/channels in hardware
  makes the server scale quite nicely. The rest of the server (house-keeping,
  outgoing connections, etc) runs on the `threaded` scheduler.


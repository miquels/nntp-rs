
## nntp-rs

A NNTP server in Rust. Async, built with Tokio.

Work in progress, but it's already a full feeder/peering server that can
handle multiple incoming full feeds (tens of thousands of CHECKs per
second, 2000 articles received per second, 10 Gbit/s incoming traffic)
and multiple full outgoing feeds.

Status:

- diablo-format history file
- diablo-format article spool
- auto-expire for spool: no need to run expire periodically.
- uses its own diablo-influenced configuration format,
  but can read diablo "dnewsfeeds", "diablo.hosts" and "dspool.ctl" files.
- NNTP commands for capabilities **mandatory** and most of **READER** have been
  implemented, as well as CHECK / TAKETHIS.
  (no active file or article numbering, yet, though).
- incoming and outgoing feeds
- header only feeds
- can write `prometheus` metrics files so you can build nice dashboards

Example configuration files: [config.cfg](config.cfg), [newsfeeds.cfg](newsfeeds.cfg).

# Runtimes.

The part of the server that handles incoming feeds can use two different
runtimes, based on the Tokio `basic` and `threaded` runtimes.

- **threaded**: the default, and good for most use-cases.
  It's based on the Tokio `threaded` runtime. All of the servers `tasks`
  (each task handles one connection, incoming or outgoing) are M:N scheduled
  over a bunch of threads.

- **multisingle**: for feeders / peering servers that handle multiple incoming full feeds.
  It's a mix of multiple Tokio `basic` runtimes and one `threaded`
  runtime. You configure N incoming threads. Each thread runs one instance
  of the `basic` (single-threaded) scheduler, and can be pinned to one specific CPU core.
  Incoming connections are loadbalanced over the incoming threads (using `SO_REUSE_PORT`).
  This combined with ethernet NICs that support multiple IRQs/channels in hardware
  makes the server scale quite nicely. The rest of the server (house-keeping,
  outgoing connections, etc) runs on the `threaded` scheduler.



## nntp-rs

A NNTP server in Rust. Very much WIP.

Status:
- diablo-format history file
- diablo-format article spool (read-only)
- NNTP commands for capabilities **mandatory** and most of **READER** have been implemented
  (no active file or article numbering, yet, though).

# Architecture

The server starts a number of threads (by default num_cpus) and on each
thread a tokio current_runtime reactor is started. Each thread has a listening
socket on the same port, shared with SO_REUSEPORT. Load balancing over the
threads is done by the kernel.

Both the history database and the article spool have their own thread pool,
on which futures are spawned from the main nntp handling threads to do
async-io history file lookups and article retrieval.


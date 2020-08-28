# Statistics logging.

`nntp-rs` logs in almost the same format as diablo does. There are a few small
differences though, for consistency reasons.

For syslog logging, one of the main differences is the name of the binary.
With diablo, incoming feeds log `diablo[pid]: ...` and outgoing feeds log
`newslink[pid]: ...`. Since `nntp-rs` does both in a single binary, all
syslog logs start with `nntp-rs-server[pid]: ...`.

So to be able to tell incoming and outgoing feeds apart in a consistent way,
we need to change the log format slightly. But otherwise, all the key/values logged
are the exact same, in the same format.

## Incoming feeds.

- on connect, a field is added at the end, `[label]`.
- when logging stats, `Stats <number>` is inserted before the key/value stats
  themselves. This makes it look more like the Connection / Disconnect log lines,
  and it makes it easier to associate Connection / Stats /Disconnect log lines.

## Outgoing feeds.

- log lines are prefixed with `Feed `.
- log lines in diablo start with `hostname / spoolfile`. This is now changed
  to `label : connection_id`.



Outfeed queueing mechanism.
===========================

## Files in the spooldir.

For each outgoing feed (which may consist of multiple connections)
there are three types of files:

* <peername>.R file
  - Resend. Read-only file.
   - it contains previously backlogged or deferred articles that
     are now being re-sent, or are the first to get re-sent when
     there is time.
  - might do innfeed-style checkpointing? it's small so maybe no.

* <peername>.B.<timestamp> file (peer.B.5db93971)
  - Backlog. Write-only file.
  - tokens of articles that could not be sent are appended here
  - gets re-opened with a new timestamp when it is "full" (10 MB)
    but not more often than once every 5 minutes.

* <peername>.D.<timestamp> file
  - Deferred. Write-only file.
  - tokens of articles that are deferred are appended here.
  - gets re-opened with a new timestamp when it is "full" (10 MB)
    but not more often than once every 5 minutes.

## Terminology: 

- masterfeed: one complete feed from all incoming feeds combined
- peerfeed: the one masterfeed is fanned out to multiple peerfeeds
- connection: one peerfeed is divided over one or more outgoing connections.

Masterfeed and peerfeed are no feeds between machines, but an abstract
description of the data-flow internal to the server. A connection is an
actual outgoing NNTP TCP connection to another server.

## Algorithm:

The masterfeed sends a copy of an article to all peerfeeds that want it.
It does this by adding the article to the peerfeed-queue and waking one
of the connections (if needed). If the peerfeed-queue gets full, half of
it is written to a <peerfeed>.B backlog file.

Whenever a connection is ready to send more articles, it checks its
internal send-queue. If that is empty, it checks the peerfeed-queue.
If the peerfeed queue is empty, it will check if there is a current
<peername>.P file. If there isn't, it will rename the one of the <peername>.B
or <peername>.D to a new current <peername>.P file. If there then is a
<peername>.P file it will fill up to half of the peerfeed-queue with articles
from that file.

It there is an article available to send, it will send it to the remote
peer as either a CHECKTHIS (if it came from the peerfeed-queue) or as
a TAKETHIS (if it was queued internally), and move the article entry to
the tail of the recv-queue.

If a reply is received from the remote peer, the article is popped from
the head of the recv-queue to get the original command and message-id.
For a CHECK reply the article can either be refused (and we're done),
deferred and added to the defer queue, or wanted. In the last case
the article is marked as 'TAKETHIS' and moved to the send-queue.
For a TAKETHIS reply we're always done (either accepted or rejected), but
some servers _might_ reply with "defer". In that case log it and handle
it as a reject.

## Details of the implementation.

- master-task reads items from channel and fans out over peer-connections
  - if peer is not connected, send to .B file right away
  - add to peer-queue tail. if queue is full, send 1/3 of head to .B file
  - wake (at most) one peer-connection.

- peer-connection
  - sender-task:
    - if recv-queue is full, read from wait-channel.
    - if send-queue is not empty, take one item (CHECK/TAKETHIS), move to recv-queue, send it
    - if send-queue is empty:
      - get one item from the peer-queue
      - if peer-queue is empty, see if we can read data from the backlog
        - if backlog is empty, see if we can move an older backlog in to place and use that
      - still empty, try defer-queue in the same way
        - if defer-queue is not old enough, schedule timer to try again later
    - no item: read from wait-channel

  - receiver-task:
    - wait for a reply
    - pop one item off the recv-queue
      - CHECK reply WANT: queue a TAKETHIS on the send-queue, write to wait-channel
      - CHECK reply DEFER: add to global defer queue
      - CHECK reply REFUSE: nothing
      - TAKETHIS reply accepted/rejected: done
      - TAKETHIS reply deferred (431, really a CHECK reply!) - log and handle as reject

  - the sender task and receiver task get polled sequentially from one connection-task
  - might just poll() the connection task every second to keep it going, so backlogs
    get automatically processed.



## History design

Write-through cache.

- read:
  - launch request on threadpool to read entry.
    (might add a mode to do it on the main thread for a history database
     that has its data mlock'ed into memory).

- read through cache:
  - read-lock partition
  - try to find entry in cache
  - unlock
  - found: return future::ok
  - launch request on threadpool to read entry.

- writing:
  - write-lock partition
  - add entry to cache
  - unlock
  - launch request on threadpool to write entry.

- check:
  - use read-through-cache to find entry as above
  - found: return future::ok
  - write-lock partition
  - try to find entry in cache
  - if  found: unlock, return
  - add entry to cache
  - unlock, return


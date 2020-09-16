PATH=/usr/lib/nntp-rs:/bin:/usr/bin:/sbin:/usr/sbin

# Compress incoming.log, and remove old files.
10 0 * * *  news    logcompress

# History expire.
50 1 * * *  news    expire


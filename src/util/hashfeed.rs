/// hashfeed support.

use std::io;
use std::default::Default;

use md5;

#[derive(Clone, Default, Debug, Deserialize)]
struct HashEntry {
    start:  u32,
    end:    u32,
    modval: u32,
    offset: u32,
    and:    bool,
}

/// A HashFeed is a list of hash matches.
#[derive(Default, Debug, Clone, Deserialize)]
pub struct HashFeed {
    has_and:    bool,
    list:       Vec<HashEntry>,
}

impl HashFeed {
    /// Parse a string of hash matches.
    pub fn new(list: &str) -> io::Result<HashFeed> {
        let mut has_and = false;
        let mut valid = true;
        let mut seps = vec![ ',' ];
        let mut list: Vec<HashEntry> = list.split(|x| {
            if x == ',' || x == '&' || x == '|' {
                seps.push(x);
                true
            } else {
                false
            }
        }).map(|hm| {
            let mut dh = HashEntry::default();

            // Split this entry up in a number and a modulo..
            let a: Vec<&str> = hm.splitn(2, '/').collect();
            if a.len() != 2 {
                valid = false;
                return dh;
            }

            // After the modulo there might be a :offset
            let b: Vec<&str> = a[1].splitn(2, ':').collect();

            // Parse the modulo.
            if let Ok(x) = b[0].parse::<u32>() {
                if x > 0 {
                    dh.modval = x;
                } else {
                    valid = false;
                }
            } else {
                valid = false;
            }

            // Parse the number or number range.
            let c: Vec<&str> = a[0].splitn(2, '-').collect();
            if let Ok(x) = c[0].parse::<u32>() {
                if x > 0 && x <= dh.modval {
                    dh.start = x;
                    dh.end = x;
                } else {
                    valid = false;
                }
            } else {
                valid = false;
            }

            // parse the second part of the range, if present.
            if c.len() > 1 {
                if let Ok(x) = c[1].parse::<u32>() {
                    if x > 0 && x >= dh.start && x <= dh.modval {
                        dh.end = x;
                    } else {
                        valid = false;
                    }
                } else {
                    valid = false;
                }
            }

            // Parse the offset.
            if b.len() > 1 {
                has_and = true;
                if let Ok(x) = b[1].parse::<u32>() {
                    if x <= 12 {
                        dh.offset = x;
                    } else {
                        valid = false;
                    }
                } else {
                    valid = false;
                }
            } else {
                // Marker, meaning "not set".
                dh.offset = 666;
            }
            dh
        }).collect();

        if !valid {
            return Err(io::Error::new(io::ErrorKind::Other, "cannot parse hashfeed"));
        }

        // fill in the rest.
        for i in 0 .. list.len() {
            if seps[i] == '&' || seps[i] == '|' {
                if list[i].offset == 666 {
                    list[i].offset = 4;
                }
                list[i].and = true;
            } else {
                if list[i].offset == 666 {
                    list[i].offset = 0;
                }
            }
        }

        Ok(HashFeed{has_and, list})
    }

    /// See if a hash matches one of the hashmatches in this hashfeed.
    pub fn matches(&self, hash: u128) -> bool {
        if self.list.len() == 0 {
            return true;
        }
        let mut matches = false;
        for h in &self.list {
            let n = ((hash >> (h.offset * 8)) & 0xffffffff) as u32;
            let n = n % h.modval + 1;
            let mut hashmatch = n >= h.start && n <= h.end;
            if h.and {
                if hashmatch && matches {
                    break;
                }
                matches = false;
                hashmatch = false;
            }
            if hashmatch {
                matches = true;
            }
        }
        matches
    }

    /// Hash a string into an u128 (md5)
    pub fn hash_str(s: &str) -> u128 {
        let digest = md5::compute(s.as_bytes());
        u128::from_be_bytes(digest.0)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_example_hashfeed() {
        // Test the examples from diablo/lib/hashfeed.c
        let mut md5: [u8; 16] = [0; 16];
        md5[0] = 0x38;
        md5[1] = 0x55;
        md5[2] = 0x9c;
        md5[3] = 0x87;
        md5[4] = 0x1f;
        md5[5] = 0xba;
        md5[6] = 0x28;
        md5[7] = 0xd9;
        md5[8] = 0x92;
        md5[9] = 0xea;
        md5[10] = 0xd5;
        md5[11] = 0x15;
        md5[12] = 0x49;
        md5[13] = 0x36;
        md5[14] = 0x7f;
        md5[15] = 0x83;
        let m = u128::from_be_bytes(md5);
        let n = HashFeed::hash_str("<>");
        assert!(m == n);

        assert!(((n >> 0*8) & 0xffffffff) as u32 == 0x49367f83);
        assert!(((n >> 1*8) & 0xffffffff) as u32 == 0x1549367f);
        assert!(((n >> 6*8) & 0xffffffff) as u32 == 0x28d992ea);

	}
}

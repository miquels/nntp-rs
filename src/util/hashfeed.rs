/// hashfeed support.

use std::io;
use std::default::Default;

use md5;

#[derive(Clone, Default, Debug, Deserialize)]
struct HashEntry {
    start:  u32,
    end:    u32,
    modu:   u32,
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
        let mut seps: Vec<char> = Vec::new();
        let mut list: Vec<HashEntry> = list.split(|x| {
            if x == ',' || x == '&' || x == '|' {
                seps.push(x);
                true
            } else {
                false
            }
        }).map(|hm| {
            let mut dh = HashEntry::default();

            // Split this entry up in a number and a modules.
            let a: Vec<&str> = hm.splitn(2, '/').collect();
            if a.len() != 2 {
                println!("fuck 1");
                valid = false;
                return dh;
            }

            // After the modules there might be a :offset
            let b: Vec<&str> = a[1].splitn(2, ':').collect();

            // Parse the modules.
            if let Ok(x) = b[0].parse::<u32>() {
                if x > 0 {
                    dh.modu = x;
                } else {
                    println!("fuck 2");
                    valid = false;
                }
            } else {
                    println!("fuck 3");
                valid = false;
            }

            // Parse the number or number range.
            let c: Vec<&str> = a[0].splitn(2, '-').collect();
            if let Ok(x) = c[0].parse::<u32>() {
                if x > 0 && x <= dh.modu {
                    dh.start = x;
                    dh.end = x;
                } else {
                    println!("fuck4");
                    valid = false;
                }
            } else {
                    println!("fuck 5: {}", a[0]);
                valid = false;
            }

            // parse the second part of the range, if present.
            if c.len() > 1 {
                if let Ok(x) = c[1].parse::<u32>() {
                    if x > 0 && x >= dh.start && x <= dh.modu {
                        dh.end = x;
                    } else {
                        println!("fuck42");
                        valid = false;
                    }
                } else {
                        println!("fuck 52: {}", c[1]);
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
                    println!("fuck 6");
                        valid = false;
                    }
                } else {
                    println!("fuck 7");
                    valid = false;
                }
            } else {
                // Marker, meaning "not set".
                dh.offset = 666;
            }
            dh
        }).collect();
        seps.push(',');

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
            let n = n % (h.modu - 1);
            if n >= (h.start - 1) && n <= (h.end - 1) {
                matches = true;
            } else {
                if h.and {
                    matches = false;
                }
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

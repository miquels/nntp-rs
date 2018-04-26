
use std::collections::{HashMap,VecDeque};

use time;
use history::{HistEnt, HistStatus};

pub(crate) struct HCache {
    map:        HashMap<Vec<u8>, HistEnt>,
    vec:        VecDeque<Vec<u8>>,
    max_ent:    u32,
    max_age:    u64,
}

impl HCache {
    pub fn new(max_ent: u32, max_age: u64) -> HCache {
        HCache{
            map:        HashMap::new(),
            vec:        VecDeque::new(),
            max_ent:    max_ent,
            max_age:    max_age,
        }
    }

    pub fn lookup(&self, msgid: &[u8]) -> HistEnt {
        match self.map.get(msgid) {
            Some(he) => he.to_owned(),
            None => HistEnt{
                status:     HistStatus::NotFound,
                time:       0,
                head_only:  false,
                location:   None,
            },
        }
    }

    pub fn store(&mut self, msgid: &[u8], rhe: &HistEnt) {
        let mut he = rhe.to_owned();
        if he.time == 0 {
            he.time = time::now_utc().to_timespec().sec as u64;
        }
        let now = he.time;
        self.vec.push_back(msgid.to_owned());
        self.map.insert(msgid.to_owned(), he);

        // limit number of entries.
        if self.max_ent > 0 && self.vec.len() as u32 > self.max_ent {
            self.vec.pop_front();
        }

        // limit age of entries.
        if self.max_age > 0 {
            let mut to_pop = 0;
            for v in &self.vec {
                let rm = if let Some(he) = self.map.get(v) {
                    he.time < now - self.max_age
                } else {
                    false
                };
                if rm {
                    self.map.remove(v);
                    to_pop += 1;
                }
            }
            for _ in 0..to_pop {
                self.vec.pop_front();
            }
        }
    }
}

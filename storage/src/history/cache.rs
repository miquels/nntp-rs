
use linked_hash_map::LinkedHashMap;

use time;
use history::{HistEnt, HistStatus};

pub(crate) struct HCache {
    map:        LinkedHashMap<Vec<u8>, HistEnt>,
    max_ent:    u32,
    max_age:    u64,
}

impl HCache {
    pub fn new(max_ent: u32, max_age: u64) -> HCache {
        HCache{
            map:        LinkedHashMap::new(),
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
        self.map.insert(msgid.to_owned(), he);

        // limit cache size.
        let mut v = Vec::new();
        let len = self.map.len();
        for (k, h) in &self.map {
            if len - v.len() > self.max_ent as usize ||
               h.time < now - self.max_age {
                v.push(k.to_owned());
            }
        }
	    for k in &v {
            self.map.remove(k);
        }
    }
}

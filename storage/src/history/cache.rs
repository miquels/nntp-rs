
use std::sync::RwLock;
use std::collections::{HashMap,VecDeque};

use time;
use history::{HistEnt, HistStatus};

struct HCacheInner {
    map:        HashMap<Vec<u8>, HistEnt>,
    vec:        VecDeque<Vec<u8>>,
    max_ent:    u32,
    max_age:    u64,
}

pub(crate) struct HCache {
    inner:  RwLock<HCacheInner>,
}

impl HCache {
    pub fn new(max_ent: u32, max_age: u64) -> HCache {
        HCache{
            inner:  RwLock::new(HCacheInner{
                        map:        HashMap::new(),
                        vec:        VecDeque::new(),
                        max_ent:    max_ent,
                        max_age:    max_age,
                    })
        }
    }

    pub fn lookup(&self, msgid: &[u8]) -> HistEnt {
        let inner = self.inner.read().unwrap();
        match (&*inner).map.get(msgid) {
            Some(he) => he.to_owned(),
            None => HistEnt{
                status:     HistStatus::NotFound,
                time:       0,
                head_only:  false,
                location:   None,
            },
        }
    }

    pub fn store(&self, msgid: &[u8], rhe: &HistEnt) {
        let mut i = self.inner.write().unwrap();
        let inner = &mut *i;

        let mut he = rhe.to_owned();
        if he.time == 0 {
            he.time = time::now_utc().to_timespec().sec as u64;
        }
        let now = he.time;
        inner.vec.push_back(msgid.to_owned());
        inner.map.insert(msgid.to_owned(), he);

        // limit number of entries.
        if inner.max_ent > 0 && inner.vec.len() as u32 > inner.max_ent {
            inner.vec.pop_front();
        }

        // limit age of entries.
        if inner.max_age > 0 {
            let mut to_pop = 0;
            for v in &inner.vec {
                let rm = if let Some(he) = inner.map.get(v) {
                    he.time < now - inner.max_age
                } else {
                    false
                };
                if rm {
                    inner.map.remove(v);
                    to_pop += 1;
                }
            }
            for _ in 0..to_pop {
                inner.vec.pop_front();
            }
        }
    }
}

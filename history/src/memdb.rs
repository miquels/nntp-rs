
use std::collections::HashMap;
use std::io;

use parking_lot::RwLock;

use {HistBackend,HistEnt,HistStatus};

/// In in-memory history database. Not to be used for production,
/// mainly used for testing.
#[derive(Debug)]
pub struct MemDb {
    db:             RwLock<HashMap<Vec<u8>, HistEnt>>
}

impl MemDb {
    /// create new in-memory history database.
    pub fn new() -> MemDb {
        MemDb {
            db: RwLock::new(HashMap::new())
        }
    }
}

impl HistBackend for MemDb {

    /// lookup an article in the MemDb database
    fn lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {
        let db = self.db.read();
        let res = db.get(msgid).map(|e| e.clone()).unwrap_or(HistEnt{
            time:       0,
            status:     HistStatus::NotFound,
            head_only:  false,
            location:   None,
        });
        Ok(res)
    }

    /// store an article in the MemDb database
    fn store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let mut db = self.db.write();
        db.insert(msgid.to_vec(), he.clone());
        Ok(())
    }
}

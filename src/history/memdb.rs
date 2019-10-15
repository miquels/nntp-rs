
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;

use parking_lot::RwLock;

use crate::history::{HistBackend,HistEnt,HistStatus};

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

impl MemDb {

    async fn do_lookup(&self, msgid: &[u8]) -> io::Result<HistEnt> {
        let db = self.db.read();
        let res = db.get(msgid).map(|e| e.clone()).unwrap_or(HistEnt{
            time:       0,
            status:     HistStatus::NotFound,
            head_only:  false,
            location:   None,
        });
        Ok(res)
    }

    async fn do_store(&self, msgid: &[u8], he: &HistEnt) -> io::Result<()> {
        let mut db = self.db.write();
        db.insert(msgid.to_vec(), he.clone());
        Ok(())
    }
}

impl HistBackend for MemDb {

    /// lookup an article in the MemDb database
    fn lookup<'a>(&'a self, msgid: &'a [u8]) -> Pin<Box<dyn Future<Output=io::Result<HistEnt>> + Send + 'a>> {
        Box::pin(self.do_lookup(msgid))
    }

    /// store an article in the MemDb database
    fn store<'a>(&'a self, msgid: &'a [u8], he: &'a HistEnt) -> Pin<Box<dyn Future<Output=io::Result<()>> + Send + 'a>> {
        Box::pin(self.do_store(msgid, he))
    }
}

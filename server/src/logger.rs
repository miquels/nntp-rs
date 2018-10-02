use article::Article;
use errors::*;
use newsfeeds::NewsPeer;

pub fn incoming_reject(label: &str, art: &Article, error: ArtError) {
    info!("{} - {} {} {} {:?}", label, art.msgid, art.len, art.arttype, error);
}

pub fn incoming_accept(label: &str, art: &Article, peers: &[NewsPeer], wantpeers: &[u32]) {
    // allocate string with peers in one go.
    let len = wantpeers.iter().fold(0, |t, i| t + peers[*i as usize].label.len() + 1);
    let mut s = String::with_capacity(len);
    // push peers onto string, separated by space.
    for idx in 0..wantpeers.len() {
        s.push_str(&peers[idx as usize].label);
        if idx as usize + 1 < wantpeers.len() {
            s.push(' ');
        }
    }
    // and log.
    info!("{} + {} {} {} {}", label, art.msgid, art.len, art.arttype, s);
}

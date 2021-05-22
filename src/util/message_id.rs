use once_cell::sync::Lazy;

use crate::util::{self, hostname};

static HOSTNAME: Lazy<String> = Lazy::new(|| hostname());

/// Generate a unique message-id.
///
/// If `hostname` is `None`, the system hostname will be used.
pub fn generate_message_id(hostname: Option<&str>) -> String {
    let mut hostname = hostname.unwrap_or(HOSTNAME.as_str()).to_string();
    if !hostname.contains(".") {
        hostname.push_str(".invalid");
    }
    let now = util::monotime_ms();
    let mut rnd = 0;
    while rnd < u32::MAX as u64 {
        rnd = rand::random::<u64>();
    }
    format!("{}.{}@{}", util::base50(now), util::base50(rnd), hostname)
}

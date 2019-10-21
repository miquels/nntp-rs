use serde::{self, Deserialize, Deserializer};
use std::io;
use std::time::Duration;

/// Parse size in B, K, KB, KiB, M, MB, MiB, G, GB, GiB.
pub fn parse_size(s: &str) -> io::Result<u64> {
    let mut s = s.to_string();
    s.make_ascii_lowercase();
    let (num, mul) = match s.find(|c: char| c.is_alphabetic()) {
        Some(n) => s.split_at(n),
        None => (s.as_str(), "b"),
    };
    let num = match num.parse::<u64>() {
        Ok(n) => n,
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("cannot parse {}", s),
            ));
        },
    };
    #[rustfmt::skip]
    let r = match mul {
        "b" => num,
        "k" |
        "kb" => num * 1000,
        "kib" => num * 1024,
        "m" |
        "mb" => num * 1000_000,
        "mib" => num * 1024*1024,
        "g" |
        "gb" => num * 1000_000_000,
        "gib" => num * 1024*1024*1024,
        "t" |
        "tb" => num * 1000_000_000_000,
        "tib" => num * 1024*1024*1024*1024,
        _ => {
            return Err(io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s)));
        },
    };
    Ok(r)
}

/// Parse duration in s, m, h, d.
pub fn parse_duration(s: &str) -> io::Result<Duration> {
    let mut s = s.to_string();
    s.make_ascii_lowercase();
    let (num, mul) = match s.find(|c: char| c.is_alphabetic()) {
        Some(n) => s.split_at(n),
        None => (s.as_str(), "s"),
    };
    let num = match num.parse::<u64>() {
        Ok(n) => n,
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("cannot parse {}", s),
            ))
        },
    };
    let r = match mul {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("cannot parse {}", s),
            ))
        },
    };
    Ok(Duration::new(r, 0))
}

/// Parse bool: y, yes, t, true, 1 vs n, no, f, false, 0.
pub fn parse_bool(s: &str) -> io::Result<bool> {
    let mut s = s.to_string();
    s.make_ascii_lowercase();
    let r = match s.as_str() {
        "y" | "yes" | "t" | "true" | "1" => true,
        "n" | "no" | "f" | "false" | "0" => false,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("cannot parse {}", s),
            ));
        },
    };
    Ok(r)
}


/// Serde helper, calls parse_size().
pub fn deserialize_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    parse_size(&s).map_err(serde::de::Error::custom)
}

/// Serde helper, calls parse_size().
pub fn option_deserialize_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where D: Deserializer<'de> {
    deserialize_size(deserializer).map(|s| Some(s))
}

/// Serde helper, calls parse_duration().
pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

/// Serde helper, calls parse_bool().
pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    parse_bool(&s).map_err(serde::de::Error::custom)
}

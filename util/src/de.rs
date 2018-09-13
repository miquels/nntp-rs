use std::io;
use std::time::Duration;
use serde::{self,Deserialize, Deserializer};

/// Deserialize size in B, K, KB, KiB, M, MB, MiB, G, GB, GiB.
pub fn deserialize_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
	where D: Deserializer<'de> {

    let mut s = String::deserialize(deserializer)?;
    s.make_ascii_lowercase();
    let (num, mul) = match s.find(|c: char| c.is_alphabetic()) {
        Some(n) => s.split_at(n),
        None => (s.as_str(), "b"),
    };
    let num = match num.parse::<u64>() {
        Ok(n) => n,
        Err(_) => {
            let e = io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s));
            return Err(serde::de::Error::custom(e));
        },
    };
    let r = match mul {
        "b" => num,
        "k" |
        "kb" => num * 1000,
        "kib" => num * 1024,
        "m" |
        "mb" => num * 1000_000,
        "mib" => num * 1024*1024,
        "g" |
        "gb" => num * 1000_1000_1000,
        "gib" => num * 1024*1024*1024,
        "t" |
        "tb" => num * 1000_1000_1000_1000,
        "tib" => num * 1024*1024*1024*1024,
        _ => {
            let e = io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s));
            return Err(serde::de::Error::custom(e));
        },
    };
    Ok(r)
}

/// Ditto but for Option<u64>.
pub fn option_deserialize_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
	where D: Deserializer<'de> {
    deserialize_size(deserializer).map(|s| Some(s))
}

/// Deserialize duration in s, m, h, d.
pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
	where D: Deserializer<'de> {

    let mut s = String::deserialize(deserializer)?;
    s.make_ascii_lowercase();
    let (num, mul) = match s.find(|c: char| c.is_alphabetic()) {
        Some(n) => s.split_at(n),
        None => (s.as_str(), "s"),
    };
    let num = match num.parse::<u64>() {
        Ok(n) => n,
        Err(_) => {
            let e = io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s));
            return Err(serde::de::Error::custom(e));
        },
    };
    let r = match mul {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => {
            let e = io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s));
            return Err(serde::de::Error::custom(e));
        },
    };
    Ok(Duration::new(r, 0))
}

/// Deserialize bool: y, yes, t, true, 1 vs n, no, f, false, 0.
pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
	where D: Deserializer<'de> {

    let mut s = String::deserialize(deserializer)?;
    s.make_ascii_lowercase();
    let r = match s.as_str() {
        "y" | "yes" | "t" | "true" | "1" => true,
        "n" | "no" | "f" | "false" | "0" => false,
        _ => {
            let e = io::Error::new(io::ErrorKind::Other, format!("cannot parse {}", s));
            return Err(serde::de::Error::custom(e));
        },
    };
    Ok(r)
}


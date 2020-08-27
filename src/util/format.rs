//! Format things into human readable values.

use std::time::Duration;

// std::time::Duration pretty printer.
pub fn duration(d: &Duration) -> String {
    let mut t = d.as_secs();
    let t2 = t;
    let mut s = String::new();
    if t >= 86400 {
        let days = t / 86400;
        t -= 86400 * days;
        s.push_str(&days.to_string());
        s.push('d');
    }
    if t >= 3600 {
        let hours = t / 3600;
        t -= 3600 * hours;
        s.push_str(&hours.to_string());
        s.push('h');
    }
    if t >= 60 {
        let mins = t / 60;
        t -= 60 * mins;
        s.push_str(&mins.to_string());
        s.push('m');
    }
    if t > 0 || t2 == 0 {
        s.push_str(&t.to_string());
        s.push('s');
    }
    s
}

const KIB: f64 = 1024f64;
const MIB: f64 = 1024f64 * KIB;
const GIB: f64 = 1024f64 * MIB;
const TIB: f64 = 1024f64 * GIB;

// size pretty printer.
pub fn size(sz: u64) -> String {
    let sz = sz as f64;
    if sz >= TIB {
        return format!("{:.1}TiB", sz / TIB);
    }
    if sz >= GIB {
        return format!("{:.1}GiB", sz / GIB);
    }
    if sz >= MIB {
        return format!("{:.1}MiB", sz / MIB);
    }
    if sz >= KIB {
        return format!("{:.1}KiB", sz / KIB);
    }
    format!("{}B", sz)
}

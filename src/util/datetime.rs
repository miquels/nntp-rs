//! Date and time structs and functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{self, offset::{Local, FixedOffset}, offset::TimeZone, Datelike, Timelike};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::util::clock;

pub trait SystemTimeExt {
    fn coarse() -> SystemTime;
    fn from_secs(sec: u64) -> SystemTime;
    fn from_millis(millis: u64) -> SystemTime;
    fn as_secs(&self) -> u64;
    fn as_millis(&self) -> u64;
    fn datetime_local(&self) -> DateTime<Local>;
}

impl SystemTimeExt for SystemTime {
    fn coarse() -> SystemTime {
        let now = clock::gettime(clock::ClockId::RealtimeCoarse);
        UNIX_EPOCH.checked_add(Duration::new(now.sec as u64, now.nsec as u32)).unwrap()
    }

    fn from_secs(sec: u64) -> SystemTime {
        UNIX_EPOCH.checked_add(Duration::new(sec, 0)).unwrap()
    }

    fn from_millis(millis: u64) -> SystemTime {
        let sec = millis / 1000;
        let ns = (millis - sec * 1000) * 1_000_000;
        UNIX_EPOCH.checked_add(Duration::new(sec, ns as u32)).unwrap()
    }

    fn as_secs(&self) -> u64 {
        let d = self.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        d.as_secs()
    }

    fn as_millis(&self) -> u64 {
        let d = self.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        d.as_millis() as u64
    }

    fn datetime_local(&self) -> DateTime<Local> {
        let d = self.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        DateTime(Local.timestamp(d.as_secs() as i64, d.subsec_nanos()))
    }
}

pub struct DateTime<Tz: TimeZone>(chrono::DateTime<Tz>);

impl<Tz: TimeZone> DateTime<Tz> {
    pub fn year(&self) -> i32 {
        self.0.year()
    }

    pub fn month(&self) -> u32 {
        self.0.month()
    }

    pub fn day(&self) -> u32 {
        self.0.day()
    }

    pub fn hour(&self) -> u32 {
        self.0.hour()
    }

    pub fn minute(&self) -> u32 {
        self.0.minute()
    }

    pub fn second(&self) -> u32 {
        self.0.second()
    }

    pub fn timestamp_subsec_millis(&self) -> u32 {
        self.0.timestamp_subsec_millis()
    }
}


static DT_RE: Lazy<Regex> = Lazy::new(|| {
    let re = [
        r"^\s*",
        r"(?:Mon,|Tue,|Wed,|Thu,|Fri,|Sat,|Sun,|\s*)\s*", // day of week
        r"(\d{1,2})\s+",                                  // day of month
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+", // month
        r"(\d{4})\s+",                                    // year
        r"(\d\d):(\d\d)(?:\s*|:(\d\d))\s+",               // hh:mm[:ss]
        r"([-+]\d{4}|[a-zA-Z]+)",                         // tz
    ];
    Regex::new(&re.concat()).expect("could not compile datetime RE")
});

/// Parse a Date: header as in RFC5322 paragraph 3.3 (formerly RFC2822)
/// but also allow for some variations as seen in the wild on Usenet.
///
/// Only parses dates from years 1970..9999.
pub fn parse_date(datetime: &str) -> Option<SystemTime> {
    let s = DT_RE.captures(datetime)?;
    if s.len() < 8 {
        return None;
    }

    let day = match s[1].parse::<u32>() {
        Ok(d) if d >= 1 && d <= 31 => d,
        _ => return None,
    };

    let mon = month_to_num(&s[2])?;

    let year = match s[3].parse::<i32>() {
        Ok(y) if y >= 1970 && y < 9999 => y,
        _ => return None,
    };

    let hour = match s[4].parse::<u32>() {
        Ok(h) if h <= 23 => h,
        _ => return None,
    };

    let mins = match s[5].parse::<u32>() {
        Ok(m) if m <= 59 => m,
        _ => return None,
    };

    let secs = match s.get(6).map_or("", |m| m.as_str()).parse::<u32>() {
        Ok(s) if s <= 60 => s,
        _ => 0,
    };

    let off = &s[7];
    let dt = if off.starts_with("-") || off.starts_with("+") {
        let hh = match off[1..3].parse::<u32>() {
            Ok(h) if h <= 23 => h,
            _ => return None,
        };
        let mm = match off[3..5].parse::<u32>() {
            Ok(h) if h <= 59 => h,
            _ => return None,
        };
        let offset = (hh * 3600 + mm * 60) as i32;
        if off.starts_with("+") {
            FixedOffset::east(offset)
        } else {
            FixedOffset::west(offset)
        }
    } else {
        FixedOffset::east(0)
    };

    let tm = dt.ymd(year, mon, day).and_hms(hour, mins, secs).timestamp();
    if tm >= 0 {
        Some(SystemTime::from_secs(tm as u64))
    } else {
        None
    }
}

// helper.
fn month_to_num(mon: &str) -> Option<u32> {
    Some(match mon {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}


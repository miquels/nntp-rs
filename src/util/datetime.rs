//! Date and time structs and functions.

use std::cmp;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use std::time::{Duration, SystemTime};

use chrono::{
    self,
    offset::{FixedOffset, Local, TimeZone, Utc},
    Datelike, Timelike,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;

use crate::util::clock;

#[derive(Clone, Copy, Default, Deserialize)]
pub struct UnixTime(u64);

impl fmt::Debug for UnixTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{:03}", self.0 / 1000, self.0 % 1000)
    }
}

impl fmt::Display for UnixTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_rfc3339())
    }
}

impl cmp::PartialOrd for UnixTime {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl cmp::Ord for UnixTime {
    fn cmp(&self, other: &UnixTime) -> cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl cmp::PartialEq for UnixTime {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl cmp::Eq for UnixTime {}

impl std::ops::Add<Duration> for UnixTime {
    type Output = UnixTime;

    fn add(self, other: Duration) -> UnixTime {
        UnixTime(self.0 + other.as_millis() as u64)
    }
}

impl AsRef<UnixTime> for UnixTime {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl UnixTime {
    pub fn coarse() -> UnixTime {
        let now = clock::gettime(clock::ClockId::RealtimeCoarse);
        UnixTime((now.sec as u64 * 1000) + (now.nsec as u64 / 1_000_000))
    }

    pub fn now() -> UnixTime {
        let d = SystemTime::UNIX_EPOCH.elapsed().unwrap();
        UnixTime(d.as_secs() as u64 * 1000 + d.subsec_millis() as u64)
    }

    pub fn from_secs(secs: u64) -> UnixTime {
        UnixTime(secs * 1000)
    }

    pub fn from_millis(millis: u64) -> UnixTime {
        UnixTime(millis)
    }

    pub fn zero() -> UnixTime {
        UnixTime(0)
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }

    pub fn as_secs(&self) -> u64 {
        self.0 / 1000
    }

    pub fn subsec_millis(&self) -> u32 {
        (self.0 % 1000) as u32
    }

    pub fn datetime_local(&self) -> DateTime<Local> {
        DateTime(Local.timestamp(self.as_secs() as i64, self.subsec_millis() * 1_000_000))
    }

    pub fn datetime_utc(&self) -> DateTime<Utc> {
        DateTime(Utc.timestamp(self.as_secs() as i64, self.subsec_millis() * 1_000_000))
    }

    pub fn to_rfc3339(&self) -> String {
        Local
            .timestamp(self.as_secs() as i64, self.subsec_millis() * 1_000_000)
            .to_rfc3339()
    }

    pub fn to_rfc2822(&self) -> String {
        Local
            .timestamp(self.as_secs() as i64, self.subsec_millis() * 1_000_000)
            .to_rfc2822()
    }

    pub fn seconds_since(&self, earlier: impl AsRef<UnixTime>) -> u64 {
        let earlier = earlier.as_ref();
        if self.0 >= earlier.0 {
            (self.0 - earlier.0) / 1000
        } else {
            0
        }
    }

    pub fn elapsed(&self) -> Duration {
        let now = UnixTime::coarse();
        if now.0 >= self.0 {
            Duration::from_millis(now.0 - self.0)
        } else {
            Duration::from_millis(0)
        }
    }

    pub fn seconds_elapsed(&self) -> u64 {
        let now = UnixTime::coarse();
        if now.0 >= self.0 {
            (now.0 - self.0) / 1000
        } else {
            0
        }
    }

    pub fn to_atomic(&self, atomic: &AtomicU64) {
        atomic.store(self.0, Ordering::Release)
    }
}

impl From<&AtomicU64> for UnixTime {
    fn from(t: &AtomicU64) -> UnixTime {
        UnixTime(t.load(Ordering::Acquire))
    }
}

impl From<SystemTime> for UnixTime {
    fn from(t: SystemTime) -> UnixTime {
        let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        UnixTime(d.as_millis() as u64)
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
pub fn parse_date(datetime: &str) -> Option<UnixTime> {
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
        Some(UnixTime::from_secs(tm as u64))
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

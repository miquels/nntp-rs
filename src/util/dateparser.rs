//! Date: parser.
//!
//! Parse a Date: header as in RFC5322 paragraph 3.3 (formerly RFC2822)
//! but also allow for some variations as seen in the wild on Usenet.

use chrono::prelude::*;
use regex::Regex;

/// Parser.
pub struct DateParser {
    re: Regex,
}

fn build_datetime_regex() -> Regex {
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
}

impl DateParser {
    pub fn new() -> DateParser {
        lazy_static! {
            static ref RE: Regex = build_datetime_regex();
        }
        DateParser { re: RE.clone() }
    }

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

    /// parse a date from an article's Date: header.
    pub fn parse(&self, datetime: &str) -> Option<u64> {
        let s = self.re.captures(datetime)?;
        if s.len() < 8 {
            return None;
        }

        let day = match s[1].parse::<u32>() {
            Ok(d) if d >= 1 && d <= 31 => d,
            _ => return None,
        };

        let mon = DateParser::month_to_num(&s[2])?;

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
            Some(tm as u64)
        } else {
            None
        }
    }
}

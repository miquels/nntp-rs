use std::collections::HashMap;
use std::default::Default;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::wildmat;

lazy_static! {
    static ref IDCOUNTER : Arc<AtomicUsize> = Arc::<AtomicUsize>::default();
}

/// Match result.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum MatchResult {
    Match,
    NoMatch,
    Poison,
}

/// Wildcard pattern.
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum WildPat {
    Text(String),
    Pattern(String),
    Prefix(String),
    Reference(usize),
}

impl Default for WildPat {
    fn default() -> WildPat {
        WildPat::Text("=    ".to_string())
    }
}

impl FromStr for WildPat {
    type Err = ();
    fn from_str(pat: &str) -> Result<WildPat, ()> {
        let r = if let Some(pos) = pat.find(|c| c == '?' || c == '*' || c == '[' || c == '\\') {
            if pos > 0 && pos == pat.len() - 1 && pat.as_bytes()[pos] == b'*' {
                WildPat::Prefix(pat[..pat.len() - 1].to_string())
            } else {
                WildPat::Pattern(pat.to_string())
            }
        } else {
            WildPat::Text(pat.to_string())
        };
        Ok(r)
    }
}

#[inline]
fn wtype(s: &str) -> (&str, MatchResult) {
    match s.as_bytes()[0] {
        // this is a non-resolved reference that never matches. hacky.
        b'=' => (".", MatchResult::NoMatch),

        b'!' => (&s[1..], MatchResult::NoMatch),
        b'@' => (&s[1..], MatchResult::Poison),
        _ => (s, MatchResult::Match),
    }
}

impl WildPat {
    /// See if this patter matches the text.
    pub fn matches(&self, text: &str) -> Option<MatchResult> {
        match *self {
            WildPat::Text(ref p) => {
                let (p, t) = wtype(p);
                if p == text { Some(t) } else { None }
            },
            WildPat::Prefix(ref p) => {
                let (p, t) = wtype(p);
                if text.starts_with(p) { Some(t) } else { None }
            }
            WildPat::Pattern(ref p) => {
                let (p, t) = wtype(p);
                if wildmat(text, p) { Some(t) } else { None }
            }
            WildPat::Reference(_) => None,
        }
    }

    /// Is this an empty pattern?
    pub fn is_empty(&self) -> bool {
        match *self {
            WildPat::Text(ref p) |
            WildPat::Prefix(ref p) |
            WildPat::Pattern(ref p) => p.is_empty(),
            _ => false,
        }
    }
}

fn new_id() -> u16 {
    (IDCOUNTER.fetch_add(1, Ordering::SeqCst) & 0xffff) as u16
}

/// A list of wildcard patterns.
#[derive(Debug,Clone)]
pub struct WildMatList {
    pub id:         u16,
    pub name:       String,
    pub initial:    MatchResult,
    pub patterns:   Vec<WildPat>,
}

impl Default for WildMatList {
    fn default() -> WildMatList {
        WildMatList{
            name: "".to_string(),
            initial: MatchResult::NoMatch,
            patterns: Vec::new(),
            id: new_id(),
        }
    }
}

impl WildMatList {
    /// Convert comma seperated list of patterns into a WildMatList.
    pub fn new(name: &str, pattern: &str) -> WildMatList {
        let mut patterns = Vec::new();
        let mut initial = MatchResult::NoMatch;
        for pat in pattern.split(",").map(|s| s.trim()).filter(|s| !s.is_empty()) {
            if patterns.len() == 0 && pat.starts_with("!") {
                initial = MatchResult::Match;
            }
            patterns.push(pat.parse().unwrap());
        }
        WildMatList{ name: name.to_string(), patterns, initial, id: new_id() }
    }

    /// Add a pattern.
    pub fn push(&mut self, pattern: impl AsRef<str>) {
        let pat = pattern.as_ref();
        if self.patterns.len() == 0 && pat.starts_with("!") {
            self.initial = MatchResult::Match;
        }
        self.patterns.push(pat.parse().unwrap());
    }

    /// Set the name.
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    /// Walk over the elements of a WildMatList. Replace names of references
    /// with a WildPat::Reference index value. This is an important optimization
    /// so that when actually matching we can find the reference simply by
    /// indexing into a WildMat array.
    ///
    /// Logs a warning when a reference cannot be found, or when a circular
    /// reference is detected.
    pub fn resolve(&mut self, refs: &[WildMatList]) {

        for p in self.patterns.iter_mut() {
            let mut newpat = None;
            if let WildPat::Text(x) = p {
                if x.starts_with("=") {
                    let x = &x[1..];
                    for idx in 0..refs.len() {
                        if refs[idx].name == x {
                            newpat = Some(WildPat::Reference(idx));
                            break;
                        }
                    }
                    if newpat.is_none() {
                        warn!("resolving references for {}: reference {} not found", self.name, x);
                        newpat = Some(WildPat::Text("".to_string()));
                    }
                }
            }
            if let Some(pat) = newpat {
                *p = pat;
            }
        }
        let mut v = Vec::new();
        self.check_loops(&self.name, refs, &mut v);
     }

    // Check if there is a circular reference loop
    fn check_loops(&self, top: &str, refs: &[WildMatList], visited: &mut [bool]) {
        for p in &self.patterns {
            if let WildPat::Reference(idx) = p {
                let gref = &refs[*idx];
                if visited[*idx] {
                    warn!("resolving references for {}: loop at {}", top, gref.name);
                    break;
                }
                visited[*idx] = true;
                gref.check_loops(top, refs, visited);
            }
        }
    }

    /// See if the text matches any of the patterns. The MatchResult of the
    /// last matching pattern is returned, except for MatchResult::Poison
    /// which is returned on the first match.
    pub fn matches(&self, text: &str) -> MatchResult {
        let mut res = self.initial;
        for p in &self.patterns {
            match p.matches(text) {
                Some(m @ MatchResult::Match) => res = m,
                Some(m @ MatchResult::NoMatch) => res = m,
                Some(m @ MatchResult::Poison) => return m,
                None => {},
            }
        }
        res
    }

    // like matches, but knows about references, and cached references.
    pub fn matches2(&self, word: &str, word_idx: usize, list: &mut MatchList) -> Option<MatchResult> {

        let mut result = None;

        for p in &self.patterns {

            let res = if let WildPat::Reference(ref_idx) = p {
                // a reference. do we have it in cache?
                let gref = &list.refs[*ref_idx];
                if let Some(cached) = list.cache.get(&(word_idx as u16, gref.id)).map(|c| c.clone()) {
                    cached
                } else {
                    // recurse.
                    let res = gref.matches2(word, word_idx, list);
                    list.cache.insert((word_idx as u16, gref.id), res.clone());
                    res
                }
            } else {
                // Normal match.
                p.matches(word)
            };

            match res {
                Some(m @ MatchResult::Match) => result = Some(m),
                Some(m @ MatchResult::NoMatch) => result = Some(m),
                Some(m @ MatchResult::Poison) => return Some(m),
                None => {},
            }
        }
        result
    }

    /// Given a list of words, check each word for a match. If any word
    /// matches, it is a positive match, unless it's a Poison reult.
    pub fn matchlist(&self, words: &[&str]) -> MatchResult {
        let mut res = MatchResult::NoMatch;
        for w in words {
             match self.matches(w) {
                m @ MatchResult::Poison => return m,
                m @ MatchResult::Match => res = m,
                MatchResult::NoMatch => {},
            }
        }
        res
    }

    /// Like matchlist, but we support "references", where a pattern
    /// can be a reference to another WildMatList, and we cache
    /// reference lookups.
    pub fn matchlistx(&self, list: &mut MatchList) -> MatchResult {
        let mut result = MatchResult::NoMatch;

        // loop over all the words.
        for word_idx in 0..list.words.len() {
            let word = list.words[word_idx];
            match self.matches2(word, word_idx, list) {
                Some(m @ MatchResult::Match) => result = m,
                Some(m @ MatchResult::Poison) => return m,
                _ => {},
            }
        }
        result
    }
}

/// Passed to WildMatList::matchlistx(). Caches matches in references.
#[derive(Default,Debug)]
pub struct MatchList<'a> {
    cache:  HashMap<(u16,u16), Option<MatchResult>>,
    words:  &'a [&'a str],
    refs:   &'a [WildMatList],
}

impl<'a> MatchList<'a> {
    pub fn new(words: &'a [&str], refs: &'a [WildMatList]) -> MatchList<'a> {
        let cache = HashMap::with_capacity(words.len() * refs.len());
        MatchList { cache, words, refs }
    }
    pub fn len(&self) -> usize {
        self.words.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildmat() {
		let w = WildMatList::new("name", "hal*[o]");
        assert!(w.matches("hallo") == MatchResult::Match);
    }
}

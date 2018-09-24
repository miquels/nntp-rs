use wildmat;

#[derive(Clone,Copy,PartialEq,Eq)]
pub enum MatchResult {
    Match,
    NoMatch,
    Poison,
}

pub struct WildMat {
    pats:   Vec<String>,
    types:  Vec<MatchResult>,
    wtypes: Vec<WildType>,
}

enum WildType {
    Text,
    Pattern,
    Prefix,
}

impl WildMat {
    pub fn new(pattern: &str) -> Option<WildMat> {
        let mut pats : Vec<String> = Vec::new();
        let mut types = Vec::new();
        let mut wtypes = Vec::new();
        for mut pat in pattern.split(",") {

            let wt = if let Some(pos) = pat.find(|c| c == '?' || c == '*' || c == '[' || c == '\\') {
                if pos == pat.len() - 1 && pat.as_bytes()[pos] == b'*' {
                    pat = &pat[..pat.len() - 1];
                    WildType::Prefix
                } else {
                    WildType::Pattern
                }
            } else {
                WildType::Text
            };
            wtypes.push(wt);

            if pat.starts_with("!") {
                types.push(MatchResult::NoMatch);
                pats.push(pat[1..].to_string());
            } else if pat.starts_with("@") {
                types.push(MatchResult::Poison);
                pats.push(pat[1..].to_string());
            } else {
                types.push(MatchResult::Match);
                pats.push(pat.to_string());
            }
        }
        Some(WildMat{ pats, types, wtypes })
    }

    pub fn matches(&self, text: &str) -> MatchResult {
        let mut res = MatchResult::NoMatch;
        for (i, p) in self.pats.iter().enumerate() {
            let mt = match self.wtypes[i] {
                WildType::Text => text == p,
                WildType::Pattern => wildmat(text, p),
                WildType::Prefix => text.starts_with(p),
            };
            if mt {
                let t = self.types[i];
                match t {
                    MatchResult::Match | MatchResult::NoMatch => res = t,
                    MatchResult::Poison => return t,
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildmat() {
		let w = WildMatRe::new("hal*[o]").expect("invalid re");
        assert!(w.matches("hallo") == MatchResult::Match);
    }
}

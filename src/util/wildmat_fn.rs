//! Wildmat function.
//!
//! This is a straight port of Rich Salz's `wildmat.c` to rust (and it shows).
//!
//! The license asks to include this notice:
//! This product includes software developed by Rich Salz.

//    From the original wildmat.c:
//
//    Copyright 1991 Rich Salz.
//    All rights reserved.
//    $Revision: 1.3 $
//
//    Redistribution and use in any form are permitted provided that the
//    following restrictions are are met:
//      1.  Source distributions must retain this entire copyright notice
//          and comment.
//      2.  Binary distributions must include the acknowledgement ``This
//          product includes software developed by Rich Salz'' in the
//          documentation or other materials provided with the
//          distribution.  This must not be represented as an endorsement
//          or promotion without specific prior written permission.
//      3.  The origin of this software must not be misrepresented, either
//          by explicit claim or by omission.  Credits must appear in the
//          source and documentation.
//      4.  Altered versions must be plainly marked as such in the source
//          and documentation and must not be misrepresented as being the
//          original software.
//    THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
//    WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
//    MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
//
//

//  $Revision: 1.3 $
//
//  Do shell-style pattern matching for ?, \, [], and * characters.
//  Might not be robust in face of malformed patterns; e.g., "foo[a-"
//  could cause a segmentation violation.  It is 8bit clean.
//
//  Written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
//  Rich $alz is now <rsalz@osf.org>.
//  April, 1991:  Replaced mutually-recursive calls with in-line code
//  for the star character.
//
//  Special thanks to Lars Mathiesen <thorinn@diku.dk> for the ABORT code.
//  This can greatly speed up failing wildcard patterns.  For example:
//      pattern: -*-*-*-*-*-*-12-*-*-*-m-*-*-*
//      text 1:  -adobe-courier-bold-o-normal--12-120-75-75-m-70-iso8859-1
//      text 2:  -adobe-courier-bold-o-normal--12-120-75-75-X-70-iso8859-1
//  Text 1 matches with 51 calls, while text 2 fails with 54 calls.  Without
//  the ABORT code, it takes 22310 calls to fail.  Ugh.  The following
//  explanation is from Lars:
//  The precondition that must be fulfilled is that DoMatch will consume
//  at least one character in text.  This is true if *p is neither '*' nor
//  '\0'.)  The last return has ABORT instead of FALSE to avoid quadratic
//  behaviour in cases like pattern "*a*b*c*d" with text "abcxxxxx".  With
//  FALSE, each star-loop has to run to the end of the text; with ABORT
//  only the last one does.
//
//  Once the control of one instance of DoMatch enters the star-loop, that
//  instance will return either TRUE or ABORT, and any calling instance
//  will therefore return immediately after (without calling recursively
//  again).  In effect, only one star-loop is ever active.  It would be
//  possible to modify the code to maintain this context explicitly,
//  eliminating all recursive calls at the cost of some complication and
//  loss of clarity (and the ABORT stuff seems to be unclear enough by
//  itself).  I think it would be unwise to try to get this into a
//  released version unless you have a good test data base to try it out
//  on.
//

#[derive(Clone)]
struct BytesIter<'a> {
    bytes: &'a [u8],
}

impl<'a> BytesIter<'a> {
    fn new(bytes: &'a [u8]) -> BytesIter<'a> {
        BytesIter { bytes }
    }
}

impl<'a> Iterator for BytesIter<'a> {
    type Item = char;

    fn next(&mut self) -> Option<char> {
        if !self.bytes.is_empty() {
            let b = self.bytes[0];
            self.bytes = &self.bytes[1..];
            Some(b as char)
        } else {
            None
        }
    }
}

// Match 'text' and 'pat'. Returns Some(true), Some(false), or None.
// None is what in the original code is called ABORT.
fn do_match<T>(mut text_iter: T, mut pat_iter: T) -> Option<bool>
where
    T: Iterator<Item = char> + Clone,
{
    //println!("do_match({:?}, {:?}", text, pat);

    loop {
        let p = match pat_iter.next() {
            None => break,
            Some(p) => p,
        };

        match p {
            '?' => {
                // Match anything.
                text_iter.next()?;
                continue;
            }
            '*' => {
                // Check for consecutive stars.
                let mut pat_clone = pat_iter.clone();
                loop {
                    match pat_iter.next() {
                        None => {
                            // Trailing star matches everything.
                            return Some(true);
                        }
                        Some('*') => pat_clone = pat_iter.clone(),
                        _ => break,
                    }
                }
                // Try all possibilities.
                loop {
                    //println!("recurse");
                    match do_match(text_iter.clone(), pat_clone.clone()) {
                        Some(false) => {}
                        p => return p,
                    }
                    if text_iter.next().is_none() {
                        return None;
                    }
                }
            }
            '[' => {
                // Inverted character set?
                let (p, fail) = match pat_iter.next()? {
                    '^' => (pat_iter.next()?, true),
                    p => (p, false),
                };

                let mut matched = false;
                let c = text_iter.next()?;

                // Check if set starts with '-' or ']'
                let p = match p {
                    ']' | '-' => {
                        if c == p {
                            matched = true;
                        }
                        pat_iter.next()?
                    }
                    p => p,
                };

                // check if any chars in the set match.
                let mut p = p;
                loop {
                    // end of set - done
                    if p == ']' {
                        break;
                    }
                    if c == p {
                        matched = true;
                    }
                    let begin = p;
                    p = pat_iter.next()?;
                    if p == '-' {
                        p = pat_iter.next()?;
                        if p == ']' {
                            if c == '-' {
                                matched = true;
                            }
                        } else {
                            if c >= begin && c <= p {
                                matched = true;
                            }
                            p = pat_iter.next()?;
                        }
                    }
                }
                if matched == fail {
                    return Some(false);
                }
            }
            a => {
                let c = match text_iter.next()? {
                    '\\' => text_iter.next()?,
                    v => v,
                };
                if a != c {
                    return Some(false);
                }
            }
        }
    }
    Some(text_iter.next().is_none())
}

/// Do shell-style pattern matching for ?, \, [], and * characters.
/// It is UTF8-clean, but it won't match combining characters in '?' or ranges.
pub fn wildmat(text: impl AsRef<str>, pat: impl AsRef<str>) -> bool {
    let pat = pat.as_ref();
    if pat == "*" {
        return true;
    }
    match do_match(text.as_ref().chars(), pat.chars()) {
        Some(true) => true,
        _ => false,
    }
}

/// Do shell-style pattern matching for ?, \, [], and * characters.
/// It is UTF8-clean, but it won't match non-ascii characters in '?' or ranges.
#[allow(dead_code)]
pub fn wildmat_bytes(text: impl AsRef<[u8]>, pat: impl AsRef<[u8]>) -> bool {
    let pat = pat.as_ref();
    if pat == &b"*"[..] {
        return true;
    }
    match do_match(BytesIter::new(text.as_ref()), BytesIter::new(pat)) {
        Some(true) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildmat() {
        assert!(wildmat("hallo", "*"));
        assert!(wildmat("hallo", "hallo"));
        assert!(wildmat("hallo", "hal*"));
        assert!(wildmat("hallo", "*lo"));
        assert!(wildmat("hallo", "ha*lo"));
        assert!(wildmat("hallo", "ha*llo"));
        assert!(wildmat("halo", "ha*llo") == false);
        assert!(wildmat("hallo", "h[abc]llo"));
        assert!(wildmat("hallo", "h[^cba]llo") == false);
        assert!(wildmat("hallo", "h*[o") == false);
        assert!(wildmat("ba.announce", "[a-z]*") == true);
        //assert!(wildmat("vier € is 3 $", "* [xy€z] is 3 $"));
    }
}

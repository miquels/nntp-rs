const ALPHABET: [u8; 50] = [
    b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'B', b'C', b'D', b'F', b'G', b'H', b'J',
    b'K', b'L', b'M', b'N', b'P', b'Q', b'R', b'S', b'T', b'V', b'W', b'X', b'Z', b'b', b'c', b'd', b'f',
    b'g', b'h', b'j', b'k', b'l', b'm', b'n', b'p', b'q', b'r', b's', b't', b'v', b'w', b'x', b'z',
];

pub fn base50(mut num: u64) -> String {
    if num == 0 {
        return "0".to_owned();
    }
    let mut v = Vec::new();

    while num > 0 {
        let b = ALPHABET[(num % 50) as usize];
        v.push(b);
        num /= 50
    }
    v.reverse();
    String::from_utf8(v).unwrap()
}

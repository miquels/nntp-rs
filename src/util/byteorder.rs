use std::convert::TryInto;

#[inline]
pub fn u16_from_le_bytes(m: &[u8]) -> u16 {
    u16::from_le_bytes((&m[0..2]).try_into().unwrap())
}

#[inline]
pub fn u32_from_le_bytes(m: &[u8]) -> u32 {
    u32::from_le_bytes((&m[0..4]).try_into().unwrap())
}

#[inline]
pub fn u16_write_le_bytes(m: &mut [u8], v: u16) {
    m.copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn u32_write_le_bytes(m: &mut [u8], v: u32) {
    m.copy_from_slice(&v.to_le_bytes());
}

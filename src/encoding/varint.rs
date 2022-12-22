use std::mem::size_of;
use crate::encoding::parseerror::ParseError;

// Who coded it better?
// pub fn encode_zig_zag_32(n: i32) -> u32 {
//     ((n << 1) ^ (n >> 31)) as u32
// }
//
// pub fn encode_zig_zag_64(n: i64) -> u64 {
//     ((n << 1) ^ (n >> 63)) as u64
// }

pub fn num_encode_zigzag_i64(val: i64) -> u64 {
    val.unsigned_abs() * 2 + val.is_negative() as u64
}

pub fn num_encode_zigzag_i32(val: i32) -> u32 {
    val.unsigned_abs() * 2 + val.is_negative() as u32
}

pub fn num_encode_zigzag_isize(val: isize) -> usize {
    // TODO: Figure out a way to write this that gives compiler errors instead of runtime errors.
    if cfg!(target_pointer_width = "16") || cfg!(target_pointer_width = "32") {
        num_encode_zigzag_i32(val as i32) as usize
    } else if cfg!(target_pointer_width = "64") {
        num_encode_zigzag_i64(val as i64) as usize
    } else {
        panic!("Unsupported target pointer width")
    }
}

#[inline]
pub(crate) fn mix_bit_u64(value: u64, extra: bool) -> u64 {
    debug_assert!(value < u64::MAX >> 1);
    value * 2 + extra as u64
}

#[inline]
pub(crate) fn mix_bit_u32(value: u32, extra: bool) -> u32 {
    debug_assert!(value < u32::MAX >> 1);
    value * 2 + extra as u32
}

#[inline]
pub(crate) fn mix_bit_usize(value: usize, extra: bool) -> usize {
    debug_assert!(value < usize::MAX >> 1);
    if cfg!(target_pointer_width = "16") || cfg!(target_pointer_width = "32") {
        mix_bit_u32(value as u32, extra) as usize
    } else if cfg!(target_pointer_width = "64") {
        mix_bit_u64(value as u64, extra) as usize
    } else {
        panic!("Unsupported target pointer width")
    }
}

pub(crate) fn strip_bit_u64(value: u64) -> (u64, bool) {
    let bit = (value & 1) != 0;
    (value >> 1, bit)
}

pub(crate) fn strip_bit_u32(value: u32) -> (u32, bool) {
    let bit = (value & 1) != 0;
    (value >> 1, bit)
}
pub(crate) fn strip_bit_u32_2(value: &mut u32) -> bool {
    let bit = (*value & 1) != 0;
    *value >>= 1;
    bit
}


pub(crate) fn strip_bit_usize(value: usize) -> (usize, bool) {
    let bit = (value & 1) != 0;
    (value >> 1, bit)
}
pub(crate) fn strip_bit_usize_2(value: &mut usize) -> bool {
    let bit = (*value & 1) != 0;
    *value >>= 1;
    bit
}


// TODO: Remove this method. Callers should just use mix_bit.
// fn num_encode_i64_with_extra_bit(value: i64, extra: bool) -> u64 {
//     // We only have enough remaining bits in the u64 encoding to fit +/- 2^62.
//     debug_assert!(value.abs() < (i64::MAX / 2));
//     let val_1 = num_encode_zigzag_i64(value);
//     mix_bit_u64(val_1, extra)
// }

// pub(crate) fn num_encode_i64_with_extra_bit_2(value: i64, extra_1: bool, extra_2: bool) -> u64 {
//     // We only have enough remaining bits in the u64 encoding to fit +/- 2^62.
//     debug_assert!(value.abs() < (i64::MAX / 2));
//     let val_1 = num_encode_zigzag_i64(value);
//     let val_2 = mix_bit_u64(val_1, extra_1);
//     mix_bit_u64(val_2, extra_2)
// }

// pub fn encode_i64_with_extra_bit(value: i64, extra: bool, buf: &mut[u8]) -> usize {
//     encode_u64(num_encode_i64_with_extra_bit(value, extra), buf)
// }

pub fn num_decode_zigzag_i32(val: u32) -> i32 {
    // dbg!(val);
    (val >> 1) as i32 * (if val & 1 == 1 { -1 } else { 1 })
}

pub fn num_decode_zigzag_i64(val: u64) -> i64 {
    // dbg!(val);
    (val >> 1) as i64 * (if val & 1 == 1 { -1 } else { 1 })
}

pub fn num_decode_zigzag_isize(val: usize) -> isize {
    if cfg!(target_pointer_width = "16") || cfg!(target_pointer_width = "32") {
        num_decode_zigzag_i32(val as u32) as isize
    } else if cfg!(target_pointer_width = "64") {
        num_decode_zigzag_i64(val as u64) as isize
    } else {
        panic!("Unsupported target pointer width")
    }
}

pub fn num_decode_i64_with_extra_bit(value: u64) -> (i64, bool) {
    let bit = (value & 1) != 0;
    (num_decode_zigzag_i64(value >> 1), bit)
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;
    use crate::list::encoding::leb::{decode_leb_u64, decode_leb_u64_slow, encode_leb_u32, encode_leb_u64};

    fn check_zigzag(val: i64) {
        let zz = num_encode_zigzag_i64(val);
        let actual = num_decode_zigzag_i64(zz);
        assert_eq!(val, actual);

        // if val.abs() < i64::MAX / 2 {
        //     let zz_true = num_encode_i64_with_extra_bit(val, true);
        //     assert_eq!((val, true), num_decode_i64_with_extra_bit(zz_true));
        //     let zz_false = num_encode_i64_with_extra_bit(val, false);
        //     assert_eq!((val, false), num_decode_i64_with_extra_bit(zz_false));
        // }

        if val.abs() <= i32::MAX as i64 {
            let val = val as i32;
            let zz = num_encode_zigzag_i32(val);
            let actual = num_decode_zigzag_i32(zz);
            assert_eq!(val, actual);
        }
    }

    #[test]
    fn fuzz_encode() {
        let mut rng = SmallRng::seed_from_u64(20);

        for _i in 0..5000 {
            let x: u64 = rng.gen();

            for bits in 0..64 {
                let val = x >> bits;
                check_zigzag(val as i64);
                check_zigzag(-(val as i64));
            }
        }
    }
}
use std::mem::size_of;
use crate::list::encoding::ParseError;
use crate::list::encoding::ParseError::{InvalidVarInt, UnexpectedEOF};

/// We're using protobuf's encoding system for variable sized integers. Most numbers we store here
/// follow a Parato distribution, so this ends up being a space savings overall.
///
/// The encoding format is described in much more detail
/// [in google's protobuf documentation](https://developers.google.com/protocol-buffers/docs/encoding)
///
/// This code has been stolen with love from [rust-protobuf](https://github.com/stepancheg/rust-protobuf/blob/681462cc2a7068a2ff4111bbf19861c005c38225/protobuf/src/varint.rs)
///
/// (With some modifications.)

/// Encode u64 as varint.
/// Panics if buffer length is less than 10.
pub fn encode_u64(mut value: u64, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 10);

    fn iter(value: &mut u64, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    if !iter(&mut value, &mut buf[4]) {
        return 5;
    };
    if !iter(&mut value, &mut buf[5]) {
        return 6;
    };
    if !iter(&mut value, &mut buf[6]) {
        return 7;
    };
    if !iter(&mut value, &mut buf[7]) {
        return 8;
    };
    if !iter(&mut value, &mut buf[8]) {
        return 9;
    };
    buf[9] = value as u8;
    10
}

/// Encode u32 value as varint.
/// Panics if buffer length is less than 5.
pub fn encode_u32(mut value: u32, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 5);

    fn iter(value: &mut u32, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    buf[4] = value as u8;
    5
}

pub fn encode_usize(value: usize, buf: &mut [u8]) -> usize {
    if cfg!(target_pointer_width = "16") || cfg!(target_pointer_width = "32") {
        encode_u32(value as u32, buf)
    } else if cfg!(target_pointer_width = "64") {
        encode_u64(value as u64, buf)
    } else {
        panic!("Unsupported target pointer width")
    }
}

/// Returns (varint, number of bytes read). Or an empty Err if the parsing failed.
///
/// TODO: Make some ParseError struct or enum or something.
pub fn decode_u64_slow(buf: &[u8]) -> Result<(u64, usize), ParseError> {
    let mut r: u64 = 0;
    let mut i = 0;
    while i < buf.len() {
        if i == 10 {
            return Err(InvalidVarInt)
        }
        let b = buf[i];
        if i == 9 && (b & 0x7f) > 1 {
            return Err(InvalidVarInt)
        }
        r |= ((b & 0x7f) as u64) << (i * 7);
        i += 1;
        if b < 0x80 {
            return Ok((r, i))
        }
    }
    Err(UnexpectedEOF)
}

// TODO: This is from rust-protobuf. Check this is actually faster than decode_u64_slow.
/// Returns (varint, number of bytes read).
pub fn decode_u64(buf: &[u8]) -> Result<(u64, usize), ParseError> {
    if buf.is_empty() {
        Err(UnexpectedEOF)
    } else if buf[0] < 0x80 {
        // The most common case
        Ok((buf[0] as u64, 1))
    } else if buf.len() >= 2 && buf[1] < 0x80 {
        // Handle the case of two bytes too
        Ok((
            (buf[0] & 0x7f) as u64 | (buf[1] as u64) << 7,
            2
        ))
    } else if buf.len() >= 10 {
        // Read from array when buf at at least 10 bytes, which is the max len for varint.
        let mut r: u64 = 0;
        let mut i: usize = 0;
        // The i < buf.len() clause gets optimized out, but it gets the optimizer to remove bounds
        // checks on buf[i].
        while i < buf.len() && i < 10 {
            let b = buf[i];

            if i == 9 && (b & 0x7f) > 1 {
                return Err(InvalidVarInt);
            }
            r |= ((b & 0x7f) as u64) << (i as u64 * 7);
            i += 1;
            if b < 0x80 {
                return Ok((r, i));
            }
        }
        Err(InvalidVarInt)
    } else {
        decode_u64_slow(buf)
    }
}

pub fn decode_u32(buf: &[u8]) -> Result<(u32, usize), ParseError> {
    let (val, bytes_consumed) = decode_u64(buf)?;
    if val >= u32::MAX as u64 {
        // varint is not a u32!
        return Err(InvalidVarInt);
    }
    debug_assert!(bytes_consumed <= 5);
    Ok((val as u32, bytes_consumed))
}

pub fn decode_usize(buf: &[u8]) -> Result<(usize, usize), ParseError> {
    if size_of::<usize>() <= size_of::<u32>() {
        let (val, count) = decode_u32(buf)?;
        Ok((val as usize, count))
    } else if size_of::<usize>() == size_of::<u64>() {
        let (val, count) = decode_u64(buf)?;
        Ok((val as usize, count))
    } else {
        panic!("usize larger than u64 not supported");
    }
}

// Who coded it better?
// pub fn encode_zig_zag_32(n: i32) -> u32 {
//     ((n << 1) ^ (n >> 31)) as u32
// }
//
// pub fn encode_zig_zag_64(n: i64) -> u64 {
//     ((n << 1) ^ (n >> 63)) as u64
// }

pub fn num_encode_zigzag_i64(val: i64) -> u64 {
    val.abs() as u64 * 2 + val.is_negative() as u64
}

pub fn num_encode_zigzag_i32(val: i32) -> u32 {
    val.abs() as u32 * 2 + val.is_negative() as u32
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

pub fn encode_i64(value: i64, buf: &mut[u8]) -> usize {
    encode_u64(num_encode_zigzag_i64(value), buf)
}

pub fn encode_i32(value: i32, buf: &mut[u8]) -> usize {
    encode_u32(num_encode_zigzag_i32(value), buf)
}

pub fn encode_u32_with_extra_bit(value: u32, extra: bool, buf: &mut[u8]) -> usize {
    debug_assert!(value < (u32::MAX >> 1));
    let val_2 = value * 2 + (extra as u32);
    encode_u32(val_2, buf)
}

pub fn encode_u32_with_extra_bit_2(value: u32, extra_1: bool, extra_2: bool, buf: &mut[u8]) -> usize {
    debug_assert!(value < (u32::MAX >> 2));
    let val_2 = (value << 2) + ((extra_1 as u32) << 1) + (extra_2 as u32);
    encode_u32(val_2, buf)
}

#[inline]
pub(crate) fn mix_bit_u64(value: u64, extra: bool) -> u64 {
    debug_assert!(value < u64::MAX >> 1);
    value * 2 + extra as u64
}

pub(crate) fn mix_bit_u32(value: u32, extra: bool) -> u32 {
    debug_assert!(value < u32::MAX >> 1);
    value * 2 + extra as u32
}

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

pub(crate) fn strip_bit_usize(value: usize) -> (usize, bool) {
    let bit = (value & 1) != 0;
    (value >> 1, bit)
}
pub(crate) fn strip_bit_usize2(value: &mut usize) -> bool {
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
    use crate::list::encoding::varint::encode_u64;

    fn check_enc_dec_unsigned(val: u64) {
        let mut buf = [0u8; 10];
        let bytes_used = encode_u64(val, &mut buf);

        let v1 = decode_u64_slow(&buf).unwrap();
        assert_eq!(v1, (val, bytes_used));
        let v2 = decode_u64(&buf).unwrap();
        assert_eq!(v2, (val, bytes_used));
        let v3 = decode_u64_slow(&buf[..bytes_used]).unwrap();
        assert_eq!(v3, (val, bytes_used));

        if val < u32::MAX as u64 {
            let mut buf2 = [0u8; 5];
            let bytes_used_2 = encode_u32(val as u32, &mut buf2);
            assert_eq!(buf[..5], buf2);
            assert_eq!(bytes_used, bytes_used_2);
        }
    }

    #[test]
    fn simple_encode_u32() {
        // This isn't thorough, but its a decent smoke test.
        // Encoding example from https://developers.google.com/protocol-buffers/docs/encoding:
        let mut result = [0u8; 5];
        assert_eq!(2, encode_u32(300, &mut result[..]));
        assert_eq!(result[0], 0b10101100);
        assert_eq!(result[1], 0b00000010);
    }

    #[test]
    fn enc_edge_cases() {
        check_enc_dec_unsigned(0);
        check_enc_dec_unsigned(1);
        check_enc_dec_unsigned(u64::MAX);
    }

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
                check_enc_dec_unsigned(val);

                check_zigzag(val as i64);
                check_zigzag(-(val as i64));
            }
        }
    }
}
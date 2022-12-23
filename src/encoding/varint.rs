//! This file contains routines to do length-prefixed varint encoding. I'd use LEB128 but this is
//! faster to encode and decode because it plays better with branch predictor.
//!
//! This uses a bijective base, where each number has exactly 1 canonical encoding.
//! See https://news.ycombinator.com/item?id=11263378 for an explanation as to why.
//!
//! This format is extremely similar to how UTF8 works internally. Its almost certainly possible to
//! reuse existing efficient UTF8 <-> UTF32 SIMD encoders and decoders to make this code faster,
//! but frankly its not a priority right now.
//!
//! 0    - 2^7-1 encodes as `0b0xxx_xxxx`
//! 2^7  - 2^14+2^7-1 encodes as `0b10xx_xxxx xxxx_xxxx`
//! 2^14+2^7 - 2^21+2^14+2^7-1 encodes as `0b110x_xxxx xxxx_xxxx xxxx_xxxx`
//! 2^21 - 2^28-1 encodes as `0b1110_xxxx xxxx_xxxx xxxx_xxxx xxxx_xxxx`
//!
//! ... And so on.

use std::hint::unreachable_unchecked;
use std::mem::size_of;
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::ExtendFromSlice;

// const ENC_1_U64: u64 = 1u64 << 7;
// const ENC_2_U64: u64 = (1u64 << 14) + (1u64 << 7);

const ENC_1_U32: u32 = 1u32 << 7;
const ENC_2_U32: u32 = (1u32 << 14) + ENC_1_U32;
const ENC_3_U32: u32 = (1u32 << 21) + ENC_2_U32;
const ENC_4_U32: u32 = (1u32 << 28) + ENC_3_U32;

const ENC_1_U64: u64 = 1u64 << 7;
const ENC_2_U64: u64 = (1u64 << 14) + ENC_1_U64;
const ENC_3_U64: u64 = (1u64 << 21) + ENC_2_U64;
const ENC_4_U64: u64 = (1u64 << 28) + ENC_3_U64;
const ENC_5_U64: u64 = (1u64 << 35) + ENC_4_U64;
const ENC_6_U64: u64 = (1u64 << 42) + ENC_5_U64;
const ENC_7_U64: u64 = (1u64 << 49) + ENC_6_U64;
const ENC_8_U64: u64 = (1u64 << 54) + ENC_7_U64;

/// Encode u32 as a length-prefixed varint.
///
/// Returns the number of bytes which have been consumed in the provided buffer.
pub fn encode_prefix_varint_u32(mut value: u32) -> ([u8; 5], usize) {
    let mut buf = [0u8; 5];
    if value < ENC_1_U32 {
        buf[0] = value as u8;
        (buf, 1)
    } else if value < ENC_2_U32 {
        value -= ENC_1_U32;
        buf[0] = 0b1000_0000 | (value >> 8) as u8;
        buf[1] = value as u8; // Rust's casting rules will truncate this.
        (buf, 2)
    } else if value < ENC_3_U32 {
        value -= ENC_2_U32;
        buf[0] = 0b1100_0000 | (value >> 16) as u8;

        // buf[1..3].copy_from_slice(&(value as u16).to_be_bytes());
        buf[1] = (value >> 8) as u8;
        buf[2] = value as u8;
        (buf, 3)
    } else if value < ENC_4_U32 {
        value -= ENC_3_U32;

        // The code below is equivalent to this, but the generated assembly is a wash.
        // let n = (0b1110_0000 << 24) + value;
        // buf[0..4].copy_from_slice(&n.to_be_bytes());

        buf[0] = 0b1110_0000 | (value >> 24) as u8;
        buf[1] = (value >> 16) as u8;
        buf[2] = (value >> 8) as u8;
        buf[3] = value as u8;
        (buf, 4)
    } else {
        value -= ENC_4_U32;
        buf[0] = 0b1111_0000; // + (value >> 32) as u8;

        // This compiles to smaller code than the unrolled version.
        buf[1..5].copy_from_slice(&value.to_be_bytes());
        (buf, 5)
    }
}

/// Encode a u64 as a length-prefixed varint.
///
/// Returns the number of bytes which have been consumed in the provided buffer.
pub fn encode_prefix_varint_u64(mut value: u64) -> ([u8; 9], usize) {
    let mut buf = [0u8; 9];

    let bytes_used = if value < ENC_1_U64 {
        buf[0] = value as u8;
        1
    } else if value < ENC_2_U64 {
        value -= ENC_1_U64;
        buf[0] = 0b1000_0000 | (value >> 8) as u8;
        buf[1] = value as u8; // Rust's casting rules will truncate this.
        2
    } else if value < ENC_3_U64 {
        value -= ENC_2_U64;
        buf[0] = 0b1100_0000 | (value >> 16) as u8;
        buf[1] = (value >> 8) as u8;
        buf[2] = value as u8;
        3
    } else if value < ENC_4_U64 {
        value -= ENC_3_U64;
        buf[0] = 0b1110_0000 | (value >> 24) as u8;
        buf[1] = (value >> 16) as u8;
        buf[2] = (value >> 8) as u8;
        buf[3] = value as u8;
        4
    } else if value < ENC_5_U64 {
        value -= ENC_4_U64;
        buf[0] = 0b1111_0000 | (value >> 32) as u8;
        buf[1..5].copy_from_slice(&(value as u32).to_be_bytes());
        5
    } else if value < ENC_6_U64 {
        value -= ENC_5_U64;
        buf[0] = 0b1111_1000 | (value >> 40) as u8;
        buf[1] = (value >> 32) as u8;
        buf[2..6].copy_from_slice(&(value as u32).to_be_bytes());
        6
    } else if value < ENC_7_U64 {
        value -= ENC_6_U64;
        buf[0] = 0b1111_1100 | (value >> 48) as u8;
        buf[1] = (value >> 40) as u8;
        buf[2] = (value >> 32) as u8;
        buf[3..7].copy_from_slice(&(value as u32).to_be_bytes());
        7
    } else if value < ENC_8_U64 {
        value -= ENC_7_U64;
        buf[0] = 0b1111_1110 | (value >> 56) as u8;
        buf[1] = (value >> 48) as u8;
        buf[2] = (value >> 40) as u8;
        buf[3] = (value >> 32) as u8;
        buf[4..8].copy_from_slice(&(value as u32).to_be_bytes());
        8
    } else {
        value -= ENC_8_U64;
        buf[0] = 0b1111_1111;
        buf[1..9].copy_from_slice(&value.to_be_bytes());
        9
    };
    (buf, bytes_used)
}

const ENC_U64_VALS: [u64; 8] = [ENC_1_U64, ENC_2_U64, ENC_3_U64, ENC_4_U64, ENC_5_U64, ENC_6_U64, ENC_7_U64, ENC_8_U64];

pub fn decode_prefix_varint_u64(buf: &[u8]) -> Result<(u64, usize), ParseError> {
    // This implementation actually produces more code than the unrolled version below.
    if buf.is_empty() {
        return Err(ParseError::UnexpectedEOF);
    }

    let b0 = buf[0];
    if b0 <= 0b0111_1111 as u8 {
        Ok((b0 as u64, 1))
    } else if b0 <= 0b1011_1111 {
        if buf.len() < 2 { return Err(ParseError::UnexpectedEOF); }
        let val: u64 = ((b0 as u64 & 0b0011_1111) << 8)
            + buf[1] as u64
            + ENC_1_U64;
        Ok((val, 2))
    } else {
        // The & 0b111 is unnecessary, but this tells LLVM that the variable will definitely be
        // in the range of 0..7, which prevents the compiler from getting too excited about
        // unrolling the loop (below).
        let idx = (b0.leading_ones() as usize - 1) & 0b111;
        let byte_len = idx + 2;
        if buf.len() < byte_len {
            return Err(ParseError::UnexpectedEOF)
        }

        let mut val: u64 = (b0 & ((1 << (7 - idx)) - 1)) as u64;
        for b in &buf[1..byte_len] {
            val = (val << 8) + *b as u64;
        }

        val += ENC_U64_VALS[idx];

        Ok((val, byte_len))
    }
}

pub fn decode_prefix_varint_u32(buf: &[u8]) -> Result<(u32, usize), ParseError> {
    if cfg!(target_arch = "wasm32") {
        // For some reason, the rust compiler does a *terrible* job optimizing the
        // decode_prefix_varint_u32_loop variant of this function for wasm (or generally when
        // optimizing for size). We'll use the hand unrolled version for wasm to save 3kb off the
        // compiled wasm size.
        decode_prefix_varint_u32_unroll(buf)
    } else {
        decode_prefix_varint_u32_loop(buf)
    }
}

fn decode_prefix_varint_u32_loop(buf: &[u8]) -> Result<(u32, usize), ParseError> {
    decode_prefix_varint_u64(buf)
        .and_then(|(val, bytes)| {
            if val > u32::MAX as u64 {
                Err(ParseError::InvalidVarInt)
            } else {
                Ok((val as u32, bytes))
            }
        })
}

fn decode_prefix_varint_u32_unroll(buf: &[u8]) -> Result<(u32, usize), ParseError> {
    // println!("{:b} {:#04x} {:#04x} {:#04x} {:#04x} {:#04x}", buf[0], buf[0], buf[1], buf[2], buf[3], buf[4]);
    // assert!(buf.len() >= 5);
    if buf.is_empty() {
        return Err(ParseError::UnexpectedEOF);
    }

    let b0 = buf[0];
    if b0 <= 0b0111_1111 as u8 {
        Ok((b0 as u32, 1))
    } else if b0 <= 0b1011_1111 {
        if buf.len() < 2 { return Err(ParseError::UnexpectedEOF); }
        let val: u32 = ((b0 as u32 & 0b0011_1111) << 8)
            + buf[1] as u32
            + ENC_1_U32;
        Ok((val, 2))
    } else if b0 <= 0b1101_1111 {
        if buf.len() < 3 { return Err(ParseError::UnexpectedEOF); }
        let val: u32 = ((b0 as u32 & 0b0001_1111) << 16)
            + ((buf[1] as u32) << 8)
            + buf[2] as u32
            + ENC_2_U32;
        Ok((val, 3))
    } else if b0 <= 0b1110_1111 {
        if buf.len() < 4 { return Err(ParseError::UnexpectedEOF); }
        let n = unsafe { std::ptr::read_unaligned(&buf[0] as *const u8 as *const u32) };
        let val = u32::from_be(n) - (0b1110_0000 << 24) + ENC_3_U32;
        Ok((val, 4))
    } else {
        if buf.len() < 5 { return Err(ParseError::UnexpectedEOF); }
        if b0 != 0b1111_0000 { return Err(ParseError::InvalidVarInt); } // Well, this happens when the data does not fit!

        // Here we're really parsing a u32 big endian value. The optimizer is clever enough to
        // figure that out and optimize this code with a read + byteswap.
        let val: u32 = ((buf[1] as u32) << 24)
            + ((buf[2] as u32) << 16)
            + ((buf[3] as u32) << 8)
            + buf[4] as u32
            + ENC_4_U32;
        Ok((val, 5))
    }
}

pub fn decode_prefix_varint_u64_unroll(buf: &[u8]) -> Result<(u64, usize), ParseError> {
    // println!("{:b} {:#04x} {:#04x} {:#04x} {:#04x} {:#04x}", buf[0], buf[0], buf[1], buf[2], buf[3], buf[4]);
    // assert!(buf.len() >= 5);
    if buf.is_empty() {
        return Err(ParseError::UnexpectedEOF);
    }

    let b0 = buf[0];
    if b0 <= 0b0111_1111 as u8 {
        Ok((b0 as u64, 1))
    } else if b0 <= 0b1011_1111 {
        if buf.len() < 2 { return Err(ParseError::UnexpectedEOF); }
        let val: u64 = ((b0 as u64 & 0b0011_1111) << 8)
            + buf[1] as u64
            + ENC_1_U64;
        Ok((val, 2))
    } else if b0 <= 0b1101_1111 {
        if buf.len() < 3 { return Err(ParseError::UnexpectedEOF); }
        let val: u64 = ((b0 as u64 & 0b0001_1111) << 16)
            + ((buf[1] as u64) << 8)
            + buf[2] as u64
            + ENC_2_U64;
        Ok((val, 3))
    } else if b0 <= 0b1110_1111 {
        if buf.len() < 4 { return Err(ParseError::UnexpectedEOF); }
        let n = unsafe { std::ptr::read_unaligned(&buf[0] as *const u8 as *const u32) };
        let val = u32::from_be(n) as u64 - (0b1110_0000 << 24)
            + ENC_3_U64;
        Ok((val, 4))
    } else if b0 <= 0b1111_0111 {
        if buf.len() < 5 { return Err(ParseError::UnexpectedEOF); }

        // Here we're really parsing a u64 big endian value. The optimizer is clever enough to
        // figure that out and optimize this code with a read + byteswap.
        let val: u64 = ((b0 as u64 & 0b0000_0111) << 32)
            + ((buf[1] as u64) << 24)
            + ((buf[2] as u64) << 16)
            + ((buf[3] as u64) << 8)
            + buf[4] as u64
            + ENC_4_U64;
        Ok((val, 5))
    } else if b0 <= 0b1111_1011 {
        if buf.len() < 6 { return Err(ParseError::UnexpectedEOF); }

        let val: u64 = ((b0 as u64 & 0b0000_0011) << 40)
            + ((buf[1] as u64) << 32)
            + ((buf[2] as u64) << 24)
            + ((buf[3] as u64) << 16)
            + ((buf[4] as u64) << 8)
            + buf[5] as u64
            + ENC_5_U64;
        Ok((val, 6))
    } else if b0 <= 0b1111_1101 {
        if buf.len() < 7 { return Err(ParseError::UnexpectedEOF); }
        let val: u64 = ((b0 as u64 & 0b0000_0001) << 48)
            + ((buf[1] as u64) << 40)
            + ((buf[2] as u64) << 32)
            + ((buf[3] as u64) << 24)
            + ((buf[4] as u64) << 16)
            + ((buf[5] as u64) << 8)
            + buf[6] as u64
            + ENC_6_U64;
        Ok((val, 7))
    } else if b0 == 0b1111_1110 {
        if buf.len() < 8 { return Err(ParseError::UnexpectedEOF); }
        let n = unsafe { std::ptr::read_unaligned(&buf[0] as *const u8 as *const u64) };

        let val = u64::from_be(n) - (0b1111_1110 << 56)
            + ENC_7_U64;
        Ok((val, 8))
    } else {
        if buf.len() < 9 { return Err(ParseError::UnexpectedEOF); }
        let n = unsafe { std::ptr::read_unaligned(&buf[1] as *const u8 as *const u64) };

        let val = u64::from_be(n) + ENC_8_U64;
        Ok((val, 9))
    }
}

#[inline]
pub(crate) fn decode_prefix_varint_u64_unroll_flat(buf: &[u8; 9]) -> (u64, usize) {
    // println!("{:b} {:#04x} {:#04x} {:#04x} {:#04x} {:#04x}", buf[0], buf[0], buf[1], buf[2], buf[3], buf[4]);
    // assert!(buf.len() >= 5);
    let b0 = buf[0];
    if b0 <= 0b0111_1111 as u8 {
        (b0 as u64, 1)
    } else if b0 <= 0b1011_1111 {
        let val: u64 = ((b0 as u64 & 0b0011_1111) << 8)
            + buf[1] as u64
            + ENC_1_U64;
        (val, 2)
    } else if b0 <= 0b1101_1111 {
        let val: u64 = ((b0 as u64 & 0b0001_1111) << 16)
            + ((buf[1] as u64) << 8)
            + buf[2] as u64
            + ENC_2_U64;
        (val, 3)
    } else if b0 <= 0b1110_1111 {
        let n = unsafe { std::ptr::read_unaligned(&buf[0] as *const u8 as *const u32) };
        let val = u32::from_be(n) as u64 - (0b1110_0000 << 24)
            + ENC_3_U64;
        (val, 4)
    } else if b0 <= 0b1111_0111 {

        // Here we're really parsing a u64 big endian value. The optimizer is clever enough to
        // figure that out and optimize this code with a read + byteswap.
        let n = unsafe { std::ptr::read_unaligned(&buf[1] as *const u8 as *const u32) };

        let val: u64 = ((b0 as u64 & 0b0000_0111) << 32)
            + u32::from_be(n as u32) as u64
            // + ((buf[1] as u64) << 24)
            // + ((buf[2] as u64) << 16)
            // + ((buf[3] as u64) << 8)
            // + buf[4] as u64
            + ENC_4_U64;
        (val, 5)
    } else if b0 <= 0b1111_1011 {

        let val: u64 = ((b0 as u64 & 0b0000_0011) << 40)
            + ((buf[1] as u64) << 32)
            + ((buf[2] as u64) << 24)
            + ((buf[3] as u64) << 16)
            + ((buf[4] as u64) << 8)
            + buf[5] as u64
            + ENC_5_U64;
        (val, 6)
    } else if b0 <= 0b1111_1101 {
        let val: u64 = ((b0 as u64 & 0b0000_0001) << 48)
            + ((buf[1] as u64) << 40)
            + ((buf[2] as u64) << 32)
            + ((buf[3] as u64) << 24)
            + ((buf[4] as u64) << 16)
            + ((buf[5] as u64) << 8)
            + buf[6] as u64
            + ENC_6_U64;
        (val, 7)
    } else if b0 == 0b1111_1110 {
        let n = unsafe { std::ptr::read_unaligned(&buf[0] as *const u8 as *const u64) };

        let val = u64::from_be(n) - (0b1111_1110 << 56)
            + ENC_7_U64;
        (val, 8)
    } else {
        let n = unsafe { std::ptr::read_unaligned(&buf[1] as *const u8 as *const u64) };

        let val = u64::from_be(n) + ENC_8_U64;
        (val, 9)
    }
}

pub fn decode_prefix_varint_usize(buf: &[u8]) -> Result<(usize, usize), ParseError> {
    if size_of::<usize>() <= size_of::<u32>() {
        let (val, count) = decode_prefix_varint_u32(buf)?;
        Ok((val as usize, count))
    } else if size_of::<usize>() == size_of::<u64>() {
        let (val, count) = decode_prefix_varint_u64(buf)?;
        Ok((val as usize, count))
    } else {
        panic!("usize larger than u64 not supported");
    }
}


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

pub(crate) fn push_u32<V: ExtendFromSlice>(into: &mut V, val: u32) {
    let (buf, pos) = encode_prefix_varint_u32(val);
    into.extend_from_slice(&buf[..pos]);
}

pub(crate) fn push_u64<V: ExtendFromSlice>(into: &mut V, val: u64) {
    let (buf, pos) = encode_prefix_varint_u64(val);
    into.extend_from_slice(&buf[..pos]);
}

pub(crate) fn push_usize<V: ExtendFromSlice>(into: &mut V, val: usize) {
    if size_of::<usize>() <= size_of::<u32>() {
        push_u32(into, val as u32);
    } else if size_of::<usize>() == size_of::<u64>() {
        push_u64(into, val as u64);
    } else {
        panic!("usize larger than u64 is not supported");
    }
}

pub(crate) fn push_str<V: ExtendFromSlice>(into: &mut V, val: &str) {
    let bytes = val.as_bytes();
    push_usize(into, bytes.len());
    into.extend_from_slice(bytes);
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

    fn check_enc_dec_unsigned(val: u64) {
        let (buf, bytes_used) = encode_prefix_varint_u64(val);
        // println!("{:#04x} {:#04x} {:#04x} {:#04x} {:#04x}", buf[0], buf[1], buf[2], buf[3], buf[4]);
        let v1 = decode_prefix_varint_u64(&buf).unwrap();
        assert_eq!(v1, (val, bytes_used));
        let v2 = decode_prefix_varint_u64_unroll(&buf).unwrap();
        assert_eq!(v2, (val, bytes_used));
        let v3 = decode_prefix_varint_u64_unroll_flat(&buf);
        assert_eq!(v3, (val, bytes_used));

        // And check 32 bit variants.
        let val32 = val as u32;
        let (buf, bytes_used_u32) = encode_prefix_varint_u32(val32);

        if val == val32 as u64 {
            assert_eq!(bytes_used, bytes_used_u32);
        }

        let v1 = decode_prefix_varint_u32_unroll(&buf).unwrap();
        assert_eq!(v1, (val32, bytes_used_u32));
        let v2 = decode_prefix_varint_u32_loop(&buf).unwrap();
        assert_eq!(v2, (val32, bytes_used_u32));
    }

    #[test]
    fn simple_enc_dec() {
        check_enc_dec_unsigned(0);
        check_enc_dec_unsigned(1);
        check_enc_dec_unsigned(0x7f);
        check_enc_dec_unsigned(0x80);
        check_enc_dec_unsigned(0x100);
        check_enc_dec_unsigned(0xffffffff);
        check_enc_dec_unsigned(158933560); // from testing.
        check_enc_dec_unsigned(15779779462787834424); // from testing.
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

                check_enc_dec_unsigned(val);
            }
        }
    }
}
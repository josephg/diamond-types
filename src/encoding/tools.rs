use std::mem::size_of;
use crate::encoding::ChunkType;
use crate::encoding::varint::{encode_u32, encode_u64};
use bumpalo::collections::vec::Vec as BumpVec;

pub(crate) trait ExtendFromSlice {
    fn extend_from_slice(&mut self, slice: &[u8]);
}

impl ExtendFromSlice for Vec<u8> {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        Vec::extend_from_slice(self, slice);
    }
}

impl<'a> ExtendFromSlice for BumpVec<'a, u8> {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        BumpVec::extend_from_slice(self, slice);
    }
}

pub(crate) fn push_u32<V: ExtendFromSlice>(into: &mut V, val: u32) {
    let mut buf = [0u8; 5];
    let pos = encode_u32(val, &mut buf);
    into.extend_from_slice(&buf[..pos]);
}

pub(crate) fn push_u64<V: ExtendFromSlice>(into: &mut V, val: u64) {
    let mut buf = [0u8; 10];
    let pos = encode_u64(val, &mut buf);
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

fn push_chunk_header<V: ExtendFromSlice>(into: &mut V, chunk_type: ChunkType, len: usize) {
    push_u32(into, chunk_type as u32);
    push_usize(into, len);
}

pub(crate) fn push_chunk<V: ExtendFromSlice>(into: &mut V, chunk_type: ChunkType, data: &[u8]) {
    push_chunk_header(into, chunk_type, data.len());
    into.extend_from_slice(data);
}

pub fn calc_checksum(data: &[u8]) -> u32 {
    // This is crc32c. Using the crc library because the resulting binary size is much smaller.
    // let checksum = crc32c::crc32c(&result);
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(data)
}

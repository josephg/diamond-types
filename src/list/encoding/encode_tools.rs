use std::mem::{replace, size_of};
use std::error::Error;
use std::fmt::{Display, Formatter};
use rle::{MergableSpan, RleRun};
use std::marker::PhantomData;
use crate::list::encoding::ListChunkType;
use crate::encoding::varint::{encode_u32, encode_u64, mix_bit_usize};
use crate::list::remote_ids::ConversionError;

#[cfg(feature = "serde")]
use serde_crate::Serialize;

pub(super) fn push_u32(into: &mut Vec<u8>, val: u32) {
    let mut buf = [0u8; 5];
    let pos = encode_u32(val, &mut buf);
    into.extend_from_slice(&buf[..pos]);
}

pub(super) fn push_u64(into: &mut Vec<u8>, val: u64) {
    let mut buf = [0u8; 10];
    let pos = encode_u64(val, &mut buf);
    into.extend_from_slice(&buf[..pos]);
}

pub(super) fn push_usize(into: &mut Vec<u8>, val: usize) {
    if size_of::<usize>() <= size_of::<u32>() {
        push_u32(into, val as u32);
    } else if size_of::<usize>() == size_of::<u64>() {
        push_u64(into, val as u64);
    } else {
        panic!("usize larger than u64 is not supported");
    }
}

pub(super) fn push_str(into: &mut Vec<u8>, val: &str) {
    let bytes = val.as_bytes();
    push_usize(into, bytes.len());
    into.extend_from_slice(bytes);
}

pub(super) fn push_u32_le(into: &mut Vec<u8>, val: u32) {
    // This is used for the checksum. Using LE because varint is LE.
    let bytes = val.to_le_bytes();
    into.extend_from_slice(&bytes);
}

fn push_chunk_header(into: &mut Vec<u8>, chunk_type: ListChunkType, len: usize) {
    push_u32(into, chunk_type as u32);
    push_usize(into, len);
}

pub(super) fn push_chunk(into: &mut Vec<u8>, chunk_type: ListChunkType, data: &[u8]) {
    push_chunk_header(into, chunk_type, data.len());
    into.extend_from_slice(data);
}

pub(super) fn write_bit_run(run: RleRun<bool>, into: &mut Vec<u8>) {
    // dbg!(run);
    let mut n = run.len;
    n = mix_bit_usize(n, run.val);
    push_usize(into, n);
}

#[derive(Clone)]
pub(super) struct Merger<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx = ()> {
    last: Option<S>,
    f: F,
    _ctx: PhantomData<Ctx> // This is awful.
}

impl<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx> Merger<S, F, Ctx> {
    pub fn new(f: F) -> Self {
        Self { last: None, f, _ctx: PhantomData }
    }

    pub fn push2(&mut self, span: S, ctx: &mut Ctx) {
        if let Some(last) = self.last.as_mut() {
            if last.can_append(&span) {
                last.append(span);
            } else {
                let old = replace(last, span);
                (self.f)(old, ctx);
            }
        } else {
            self.last = Some(span);
        }
    }

    pub fn flush2(mut self, ctx: &mut Ctx) {
        if let Some(span) = self.last.take() {
            (self.f)(span, ctx);
        }
    }
}

// I hate this.
impl<S: MergableSpan, F: FnMut(S, &mut ())> Merger<S, F, ()> {
    pub fn push(&mut self, span: S) {
        self.push2(span, &mut ());
    }
    pub fn flush(self) {
        self.flush2(&mut ());
    }
}

impl<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx> Drop for Merger<S, F, Ctx> {
    fn drop(&mut self) {
        if self.last.is_some() && !std::thread::panicking() {
            panic!("Merger dropped with unprocessed data");
        }
    }
}

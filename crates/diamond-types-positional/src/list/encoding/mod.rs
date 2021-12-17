#[allow(unused)]
#[allow(unused_imports)]

/// The encoding module converts the internal data structures to and from a lossless compact binary
/// data format.
///
/// This is modelled after the run-length encoding in Automerge and Yjs.

mod varint;
mod encode_oplog;
mod decode_oplog;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{replace, size_of};
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::list::encoding::varint::*;
use num_enum::TryFromPrimitive;
pub use encode_oplog::EncodeOptions;

const MAGIC_BYTES_SMALL: [u8; 8] = *b"DIAMONDp";

fn push_u32(into: &mut Vec<u8>, val: u32) {
    let mut buf = [0u8; 5];
    let pos = encode_u32(val, &mut buf);
    into.extend_from_slice(&buf[..pos]);
}

fn push_u64(into: &mut Vec<u8>, val: u64) {
    let mut buf = [0u8; 10];
    let pos = encode_u64(val, &mut buf);
    into.extend_from_slice(&buf[..pos]);
}

fn push_usize(into: &mut Vec<u8>, val: usize) {
    if size_of::<usize>() <= size_of::<u32>() {
        push_u32(into, val as u32);
    } else if size_of::<usize>() == size_of::<u64>() {
        push_u64(into, val as u64);
    } else {
        panic!("usize larger than u64 is not supported");
    }
}

fn push_str(into: &mut Vec<u8>, val: &str) {
    let bytes = val.as_bytes();
    push_usize(into, bytes.len());
    into.extend_from_slice(bytes);
}


#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct Run<V: Clone + PartialEq + Eq> {
    val: V,
    len: usize,
}

impl<V: Clone + PartialEq + Eq> HasLength for Run<V> {
    fn len(&self) -> usize { self.len }
}
impl<V: Clone + PartialEq + Eq> SplitableSpan for Run<V> {
    fn truncate(&mut self, at: usize) -> Self {
        let remainder = Self {
            len: self.len - at,
            val: self.val.clone()
        };
        self.len = at;
        remainder
    }
}
impl<V: Clone + PartialEq + Eq> MergableSpan for Run<V> {
    fn can_append(&self, other: &Self) -> bool { self.val == other.val }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

fn push_run_u32(into: &mut Vec<u8>, run: Run<u32>) {
    let mut dest = [0u8; 15];
    let mut pos = 0;
    let send_length = run.len != 1;
    let n = mix_bit_u32(run.val, send_length);
    pos += encode_u32(n, &mut dest[..]);
    // pos += encode_u32_with_extra_bit(run.val, run.len != 1, &mut dest[..]);
    if send_length {
        pos += encode_usize(run.len, &mut dest[pos..]);
    }

    into.extend_from_slice(&dest[..pos]);
}

// #[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
// enum Chunk {
pub enum Chunk {
    FileInfo,

    AgentNames,
    AgentAssignment,
    PositionalPatches,
    TimeDAG,

    StartFrontier,
    EndFrontier,


    InsertedContent,
    DeletedContent,
    BranchContent,


    // Content = 2,
    //
    // AgentNames = 3,
    // AgentAssignment = 4,
    //
    // Frontier = 5,
    //
    // Parents = 6,
    //
    // DelData = 8,
    //
    // Patches = 11,
}

fn push_chunk_header(into: &mut Vec<u8>, chunk_type: Chunk, len: usize) {
    push_u32(into, chunk_type as u32);
    push_usize(into, len);
}

fn push_chunk(into: &mut Vec<u8>, chunk_type: Chunk, data: &[u8]) {
    push_chunk_header(into, chunk_type, data.len());
    into.extend_from_slice(data);
}


struct Merger<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx = ()> {
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

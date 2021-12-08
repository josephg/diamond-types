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
use std::mem::{replace, size_of};
use rle::MergableSpan;
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

fn push_run_u32(into: &mut Vec<u8>, run: Run<u32>) {
    let mut dest = [0u8; 15];
    let mut pos = 0;
    let n = mix_bit_u32(run.val, run.len != 1);
    pos += encode_u32(n, &mut dest[..]);
    // pos += encode_u32_with_extra_bit(run.val, run.len != 1, &mut dest[..]);
    if run.len != 1 {
        pos += encode_usize(run.len, &mut dest[pos..]);
    }

    into.extend_from_slice(&dest[..pos]);
}

// #[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
enum Chunk {
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

/// A SpanWriter is a helper struct for writing objects which implement MergableSpan. Essentially,
/// this acts as a single-item buffer.
///
/// It would probably be possible to replace this with clever uses of merge_iter.
#[derive(Debug, Clone, Default)]
struct SpanWriter<S: MergableSpan + Debug, F: FnMut(&mut Vec<u8>, S)> {
    dest: Vec<u8>,
    last: Option<S>,
    flush: F,

    // #[cfg(debug_assertions)]
    pub count: usize,
}

impl<S: MergableSpan + Debug, F: FnMut(&mut Vec<u8>, S)> SpanWriter<S, F> {
    pub fn new(flush: F) -> Self {
        Self {
            dest: vec![],
            last: None,
            count: 0,
            flush
        }
    }

    pub fn new_with_val(val: S, flush: F) -> Self {
        let mut result = Self::new(flush);
        result.last = Some(val);
        result
    }

    pub fn push(&mut self, s: S) {
        // assert!(s.len() > 0);
        if let Some(last) = self.last.as_mut() {
            if last.can_append(&s) {
                last.append(s);
            } else {
                let old = replace(last, s);
                self.count += 1;
                (self.flush)(&mut self.dest, old);
            }
        } else {
            self.last = Some(s);
        }
    }

    /// Write the last item and consume the writer into its inner Vec<u8>
    pub fn bake(mut self) -> Vec<u8> {
        if let Some(elem) = self.last.take() {
            self.count += 1;
            (self.flush)(&mut self.dest, elem);
        }
        self.dest
    }
}


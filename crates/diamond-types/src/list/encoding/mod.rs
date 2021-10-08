use std::convert::TryFrom;
use std::fmt::Debug;
use std::mem::{replace, size_of};
use std::ops::Range;

use num_enum::TryFromPrimitive;
use smallvec::SmallVec;

use diamond_core::CRDTId;
use rle::{MergeableIterator, SplitableSpan};

use crate::crdtspan::CRDTSpan;
use crate::list::{ListCRDT, Order};
use crate::list::encoding::varint::*;
use crate::list::span::YjsSpan;
use crate::order::OrderSpan;
use crate::rangeextra::OrderRange;
use crate::rle::KVPair;

mod varint;
mod patch_encoding;

// struct BitWriter<W: Write> {
//     to: W,
//     buf: u32,
//     buf_pos: u8,
// }
//
// impl<W: Write> BitWriter<W> {
//     fn new(writer: W) -> Self {
//         Self {
//             to: writer,
//             buf: 0,
//             buf_pos: 0
//         }
//     }
//
//     fn unwrap(self) -> W {
//         self.to
//     }
//
//     fn append(&mut self, bit: bool) {
//         self.buf |= (bit as u32) << self.buf_pos;
//         self.buf_pos += 1;
//
//         if self.buf_pos > size_of::<u32>() as u8 {
//             self.flush();
//         }
//     }
//
//     fn flush(&mut self) {
//         if self.buf_pos > 0 {
//             self.to.write_all(&self.buf.to_le_bytes()).unwrap();
//             self.buf_pos = 0;
//             self.buf = 0;
//         }
//         self.to.flush().unwrap();
//     }
// }

#[derive(Debug, Clone, Default)]
struct SpanWriter<S: SplitableSpan + Clone + Debug, F: FnMut(&mut Vec<u8>, S)> {
    dest: Vec<u8>,
    last: Option<S>,
    flush: F,
    
    // #[cfg(debug_assertions)]
    pub count: usize,
}

impl<S: SplitableSpan + Clone + Debug, F: FnMut(&mut Vec<u8>, S)> SpanWriter<S, F> {
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
        assert!(s.len() > 0);
        // if s.len() == 0 { return; }
        // println!("append {:?}", &s);
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

    pub fn flush_into_inner(mut self) -> Vec<u8> {
        if let Some(elem) = self.last.take() {
            self.count += 1;
            (self.flush)(&mut self.dest, elem);
        }
        self.dest
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct Run<V: Clone + PartialEq + Eq> {
    val: V,
    len: usize,
}

impl<V: Clone + PartialEq + Eq> SplitableSpan for Run<V> {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = Self {
            len: self.len - at,
            val: self.val.clone()
        };
        self.len = at;
        remainder
    }

    fn can_append(&self, other: &Self) -> bool { self.val == other.val }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct Run2<const INC: bool> {
    diff: isize,
    len: usize,
}

impl<const INC: bool> SplitableSpan for Run2<INC> {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = Self {
            diff: INC as isize,
            len: self.len - at,
        };
        self.len = at;
        remainder
    }

    fn can_append(&self, other: &Self) -> bool { other.diff == INC as isize }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) {
        self.diff = other.diff;
        self.len += other.len;
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct DiffVal(Order);

impl DiffVal {
    fn new() -> Self { Self(0) }

    fn next(&mut self, new_val: Order) -> i32 {
        let diff = new_val.wrapping_sub(self.0) as i32;
        self.0 = new_val;
        diff
    }

    fn next_run<const INC: bool>(&mut self, new_val: Order, len: u32) -> Run2<INC> {
        let diff = new_val.wrapping_sub(self.0) as i32 as isize;
        self.0 = new_val + len - 1;
        Run2 { diff, len: len as usize }
    }
}


// fn write_run<const INC: bool>(run: Run2<INC>, vec: &mut Vec<u8>) {
//     let mut dest = [0u8; 10];
//     let mut pos = 0;
//     // println!("{:?}", run);
//     pos += encode_i32(run.diff as i32, &mut dest[..]);
//     pos += encode_u32(run.len as u32, &mut dest[pos..]);
//     vec.extend_from_slice(&dest[..pos]);
// }

fn push_run_u32(into: &mut Vec<u8>, run: Run<u32>) {
    let mut dest = [0u8; 15];
    let mut pos = 0;
    pos += encode_u32_with_extra_bit(run.val, run.len != 1, &mut dest[..]);
    if run.len != 1 {
        pos += encode_u64(run.len as u64, &mut dest[pos..]);
    }

    into.extend_from_slice(&dest[..pos]);
}
fn push_run_2<const INC: bool>(into: &mut Vec<u8>, val: Run2<INC>) {
    let mut dest = [0u8; 20];
    let mut pos = 0;
    pos += encode_i64_with_extra_bit(val.diff as i64, val.len != 1, &mut dest[..]);
    if val.len != 1 {
        pos += encode_u64(val.len as u64, &mut dest[pos..]);
    }

    into.extend_from_slice(&dest[..pos]);
}

fn push_pos_neg(vec: &mut Vec<u8>, len: PosNegRun) {
    push_u32(vec, len.0.abs() as u32);
}

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

fn push_chunk_header(into: &mut Vec<u8>, chunk_type: Chunk, len: usize) {
    push_u32(into, chunk_type as u32);
    push_usize(into, len);
}

fn push_chunk(into: &mut Vec<u8>, chunk_type: Chunk, data: &[u8]) {
    push_chunk_header(into, chunk_type, data.len());
    into.extend_from_slice(data);
}

// The 0 is a simple top level version identifier.
const MAGIC_BYTES_SMALL: [u8; 8] = *b"DIAMONDz";

// I'm sure there's lots of simple structures like this - but I'm just going to have my own.
#[derive(Debug)]
struct BufReader<'a>(&'a [u8]);

impl<'a> BufReader<'a> {
    fn check_has_bytes(&self, num: usize) {
        assert!(self.0.len() >= num);
    }

    fn consume(&mut self, num: usize) {
        self.0 = unsafe { self.0.get_unchecked(num..) };
    }

    fn read_magic(&mut self) {
        self.check_has_bytes(8);
        assert_eq!(&self.0[..8], MAGIC_BYTES_SMALL);
        self.consume(8);
    }

    fn next_u32(&mut self) -> u32 {
        let (val, count) = decode_u32(self.0);
        self.consume(count);
        val
    }

    fn next_u64(&mut self) -> u64 {
        let (val, count) = decode_u64(self.0);
        self.consume(count);
        val
    }

    fn next_usize(&mut self) -> usize {
        let (val, count) = decode_u64(self.0);
        self.consume(count);
        val as usize
    }

    fn next_n_bytes(&mut self, num_bytes: usize) -> &'a [u8] {
        let (data, remainder) = self.0.split_at(num_bytes);
        self.0 = remainder;
        data
    }

    fn next_chunk(&mut self) -> (Chunk, BufReader<'a>) {
        let chunk_type = Chunk::try_from(self.next_u32()).unwrap();
        let len = self.next_usize();
        (chunk_type, BufReader(self.next_n_bytes(len)))
    }

    fn expect_chunk(&mut self, expect_chunk_type: Chunk) -> BufReader<'a> {
        let (actual_chunk_type, r) = self.next_chunk();
        assert_eq!(expect_chunk_type, actual_chunk_type);
        r
    }

    fn next_str(&mut self) -> Option<&str> {
        if self.0.is_empty() { return None; }
        let len = self.next_usize();
        let bytes = self.next_n_bytes(len);
        Some(std::str::from_utf8(bytes).unwrap())
    }

    fn next_u32_run(&mut self) -> Option<Run<u32>> {
        if self.0.is_empty() { return None; }
        // TODO: Error checking!!
        let (val, has_len) = num_decode_u32_with_extra_bit(self.next_u32());
        let len = if has_len {
            self.next_u64() as usize
        } else {
            1
        };
        Some(Run { val, len })
    }

    fn next_u32_diff_run<const INC: bool>(&mut self, last: &mut u32) -> Option<Run<u32>> {
        let (diff, has_len) = num_decode_i64_with_extra_bit(self.next()?);
        *last = last.wrapping_add(diff as i32 as u32);
        let base_val = *last;
        let len = if has_len {
            self.next().unwrap()
        } else {
            1
        };
        // println!("LO order {} len {}", last, len);
        if INC {
            // This is kinda gross. Why -1?
            *last = last.wrapping_add(len as u32 - 1);
        }
        Some(Run {
            val: base_val,
            len: len as usize
        })
    }
}

impl<'a> Iterator for BufReader<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() { None }
        else { Some(self.next_u64()) }
    }
}

struct PartialReader<'a, S: SplitableSpan, F: FnMut(&mut BufReader<'a>) -> Option<S>> {
    reader: BufReader<'a>,
    read_fn: F,
    data: Option<S>,
}

impl<'a, S: SplitableSpan, F: FnMut(&mut BufReader<'a>) -> Option<S>> PartialReader<'a, S, F> {
    fn new(reader: BufReader<'a>, read_fn: F) -> Self {
        Self {
            reader,
            read_fn,
            data: None
        }
    }

    fn fill(&mut self) {
        if self.data.is_none() {
            self.data = (self.read_fn)(&mut self.reader);
        }
    }

    fn peek_next(&mut self) -> &Option<S> {
        self.fill();
        &self.data
    }

    fn consume(&mut self, max_len: usize) -> Option<S> {
        self.fill();
        match &mut self.data {
            None => None,
            Some(span) => {
                if span.len() <= max_len {
                    // Take the whole span.
                    self.data.take() // I'm surprised the borrow checker lets me do this!
                } else {
                    Some(span.truncate_keeping_right(max_len))
                }
            }
        }
    }
}

impl<'a, S: SplitableSpan, F: FnMut(&mut BufReader<'a>) -> Option<S>> Iterator for PartialReader<'a, S, F> {
    type Item = S;

    fn next(&mut self) -> Option<Self::Item> {
        self.fill();
        self.data.take()
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
enum Chunk {
    FileInfo = 1,
    Content = 2,

    AgentNames = 3,
    AgentAssignment = 4,

    Frontier = 5,

    Parents = 6,

    InsOrDelFlags = 7,
    DelData = 8,

    InsOrders = 9, // Not used by encode_small.
    InsOrigins = 10,

    Patches = 11,
}

/// Entries are runs of positive or negative items.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct PosNegRun(i32);

impl SplitableSpan for PosNegRun {
    fn len(&self) -> usize {
        self.0.abs() as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at = at as i32;
        debug_assert!(at > 0 && at < self.0.abs());
        debug_assert_ne!(self.0, 0);

        let abs = self.0.abs();
        let sign = self.0.signum();
        *self = PosNegRun(at * sign);

        PosNegRun((abs - at) * sign)
    }

    fn can_append(&self, other: &Self) -> bool {
        (self.0 >= 0) == (other.0 >= 0)
    }

    fn append(&mut self, other: Self) {
        debug_assert!(self.can_append(&other));
        self.0 += other.0;
    }

    fn prepend(&mut self, other: Self) {
        self.append(other);
    }
}

impl ListCRDT {
    fn encode_common(&self) -> Vec<u8> {
        // TODO: Add flag about the file format passed in here.
        // All document file formats start with some common fields. They just encode the CRDT data
        // itself differently.
        let mut result: Vec<u8> = Vec::new();
        result.extend_from_slice(&MAGIC_BYTES_SMALL);
        push_chunk(&mut result, Chunk::FileInfo, &[]);

        // The content itself is added early in the file for a couple reasons:
        // 1. This allows a reader to fetch the document data more easily (without skipping over
        // all the CRDT stuff).
        // 2. Many compression algorithms (eg snappy) work reasonably well for text data, but bail
        // when trying to encode the compressed CRDT data. Putting the text first allows these
        // compression algorithms to still do their thing.
        if let Some(d) = self.text_content.as_ref() {
            // push_usize(&mut result, d.len_bytes());
            push_chunk_header(&mut result, Chunk::Content, d.len());
            for (str, _) in d.chunks() {
                // writer.write_all(chunk.as_bytes());
                result.extend_from_slice(str.as_bytes());
            }
        }

        // TODO: Do this without the unnecessary allocation.
        let mut agent_names = Vec::new();
        for client_data in self.client_data.iter() {
            push_str(&mut agent_names, client_data.name.as_str());
        }
        push_chunk(&mut result, Chunk::AgentNames, &agent_names);

        result
    }

    // pub fn encode_small<W: Write>(&self, writer: &mut W, verbose: bool) -> std::io::Result<()> {
    pub fn encode_small(&self, verbose: bool) -> Vec<u8> {
        // So here we know:
        // - Each entry has the next group of order numbers (its packed)
        // - Also the CRDT locations for each agent are packed.
        // So we really just have to store the runs of agent ID.
        //
        // I could use SpanWriter here but self.client_with_order should already be packed. (And if
        // it wasn't, .merge_spans() would be simpler.)
        let mut agent_data = Vec::new();
        for KVPair(_, span) in self.client_with_order.iter() {
            push_run_u32(&mut agent_data, Run { val: span.loc.agent as _, len: span.len() });
        }

        // The lengths alternate back and forth each operation.
        let mut ins_del_runs = SpanWriter::new_with_val(PosNegRun(0), push_pos_neg);

        let mut del_spans = SpanWriter::new(push_run_2);

        let mut next_order = 0;
        let mut dv = DiffVal(0);
        for KVPair(order, d) in self.deletes.iter() {
            let target = dv.next_run::<true>(d.order as _, d.len as _);
            // Some((target, *order, d.len))
            if *order > next_order {
                ins_del_runs.push(PosNegRun((*order - next_order) as i32));
            }
            // dbg!((d.end(), *order));
            ins_del_runs.push(PosNegRun(-(d.len as i32)));
            next_order = *order + d.len;

            del_spans.push(target);
        }

        let doc_next_order = self.get_next_order();
        if next_order < doc_next_order {
            // There's an insert after the last delete. Include it in ins_del_runs.
            ins_del_runs.push(PosNegRun((doc_next_order - next_order) as i32));
        }

        let mut entries = self.range_tree.raw_iter().collect::<Vec<YjsSpan>>();
        entries.sort_by_key(|e| e.order);

        let mut fancy_runs = Vec::new();
        let mut prev_ol = 0;
        let mut prev_or = DiffVal(0);
        // dbg!(entries.clone().into_iter().map(|e| e.activated()).merge_spans().count());
        for entry in entries.into_iter().map(|e| e.activated()).merge_spans() {
            let len = entry.len as u32;

            let lo_diff = entry.origin_left.wrapping_sub(prev_ol) as i32;
            prev_ol = entry.origin_left;
            // prev_ol = entry.origin_left_at_offset(len - 1);
            // prev_ol = entry.origin_left;

            // prev_ol = if len == 1 {
            //     entry.origin_left
            // } else { // len > 1
            //     entry.order + len as u32 - 2
            // };

            let ro_diff = prev_or.next(entry.origin_right);

            let mut dest = [0u8; 20];
            let mut pos = 0;
            pos += encode_i32(lo_diff, &mut dest[pos..]);
            pos += encode_i64_with_extra_bit(ro_diff as i64, len != 1, &mut dest[..]);
            if len != 1 {
                pos += encode_u32(len as u32, &mut dest[pos..]);
            }

            fancy_runs.extend_from_slice(&dest[..pos]);
        }

        // dbg!(ins_del_runs.count, self.deletes.iter().map(|r| {}).copied().merge_spans().count());
        let ins_del_runs_data = ins_del_runs.flush_into_inner();
        let del_data = del_spans.flush_into_inner();
        // let lo_data = left_origin_runs.flush_into_inner();
        // let ro_data = right_origin_runs.flush_into_inner();

        // TODO: Avoid this allocation and just use write_all() to the writer for each section.
        // let mut result: Vec<u8> = Vec::with_capacity(ins_del_runs_data.len() + del_data.len() + fancy_runs.len() + 1000);
        // let mut result: Vec<u8> = Vec::with_capacity(ins_del_runs_data.len() + del_data.len() + lo_data.len() + ro_data.len() + 1000);
        // writer.write_all(&MAGIC_BYTES_SMALL)?;
        // writer.write_all(runs_data.as_slice())?;
        // writer.write_all(del_data.as_slice())?;
        // writer.write_all(lo_data.as_slice())?;
        // writer.write_all(ro_runs.as_slice())?;

        // result.extend_from_slice(&MAGIC_BYTES_SMALL);
        // push_chunk(&mut result, Chunk::FileInfo, &[]);

        let mut result = self.encode_common();

        let mut frontier_data = vec!();
        for v in self.frontier.iter() {
            push_u32(&mut frontier_data, *v);
        }

        push_chunk(&mut result, Chunk::AgentAssignment, agent_data.as_slice());
        push_chunk(&mut result, Chunk::Frontier, frontier_data.as_slice());
        push_chunk(&mut result, Chunk::InsOrDelFlags, ins_del_runs_data.as_slice());
        push_chunk(&mut result, Chunk::DelData, del_data.as_slice());
        push_chunk(&mut result, Chunk::InsOrigins, fancy_runs.as_slice());

        if verbose {
            println!("\n===== encoding stats 3 =====");

            // println!("Agent names {}", agent_names.len());
            println!("Agent assignments {}", agent_data.len());

            println!("Del run info {}", ins_del_runs_data.len());
            println!("Del data {}", del_data.len());

            // println!("left origin RLE {} bytes", lo_data.len());
            //
            // // dbg!(right_origin_runs.count);
            // println!("right origin RLE {} bytes", ro_data.len());

            println!("Left / right origins {}", fancy_runs.len());

            // println!("without del {}", fancy_runs.len());

            println!("total (no doc content) {}", ins_del_runs_data.len() + del_data.len() + fancy_runs.len());

            // if let Some(d) = self.text_content.as_ref() {
            //     println!("total (with doc content) {}", runs_data.len() + del_data.len() + lo_data.len() + ro_runs.len() + d.len_bytes());
            // }
            println!("total {}", result.len());
        }

        // std::io::Result::Ok(())
        // writer.write_all(result.as_slice())
        result
    }

    pub fn encode_fast(&self) -> Vec<u8> {
        todo!();
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut result = ListCRDT::new();
        result.text_content = None; // Disable document content while we import data

        let mut reader = BufReader(bytes);
        reader.read_magic();

        let _info = reader.expect_chunk(Chunk::FileInfo);

        // TODO: Optional!
        let _content = reader.expect_chunk(Chunk::Content);

        let mut agent_names = reader.expect_chunk(Chunk::AgentNames);
        while let Some(name) = agent_names.next_str() {
            dbg!(name);
            // TODO: This is gross for multiple reasons:
            // - Its n^2 (since we're scanning each time)
            // - We aren't checking the ID matches the current index.
            // Tidy this up!
            result.get_or_create_agent_id(name);
        }
        dbg!(&result.client_data);

        let mut agent_data = reader.expect_chunk(Chunk::AgentAssignment);
        // let mut agent_reader = PartialReader::new(agent_data, |r| {
        //     r.next_u32_run()
        // });

        let mut order: Order = 0;
        while let Some(Run { val: agent, len }) = agent_data.next_u32_run() {
            // TODO: Consider calling assign_order_to_client instead.
            let client_data = &mut result.client_data[agent as usize];
            let seq = client_data.get_next_seq();
            result.client_with_order.push(KVPair(order, CRDTSpan {
                loc: CRDTId {
                    agent: agent as _,
                    seq
                },
                len: len as _
            }));

            client_data.item_orders.push(KVPair(seq, OrderSpan {
                order,
                len: len as _
            }));

            order += len as Order;
        }

        // This one is easy.
        let frontier_data = reader.expect_chunk(Chunk::Frontier);
        result.frontier = frontier_data.map(|o| o as u32).collect();

        let ins_del_flags = reader.expect_chunk(Chunk::InsOrDelFlags);
        let del_data = reader.expect_chunk(Chunk::DelData);


        let mut last_del_target = 0;
        let mut del_reader = PartialReader::new(del_data,  |r| {
            r.next_u32_diff_run::<true>(&mut last_del_target)
        });

        let mut _origins = reader.expect_chunk(Chunk::InsOrigins);

        let mut is_ins = true;
        let mut order = 0;
        for run in ins_del_flags {
            dbg!(run, is_ins);
            let mut run_remaining = run as usize;
            if is_ins {
                // Handle inserts!
                // while run_remaining > 0 {
                //     let agent = agent_reader.consume(run_remaining).unwrap();
                //
                //     run_remaining -= agent.len();
                // }
            } else {
                // Handle deletes
                while run_remaining > 0 {
                    let Run {
                        val: target, len
                    } = del_reader.consume(run_remaining).unwrap();
                    dbg!((order, target, len));

                    run_remaining -= len;
                }
            }

            is_ins = !is_ins;
            order += run as u32;
        }

        assert_eq!(del_reader.next(), None);

        // dbg!(&result);
        result
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Parents {
    order: Range<Order>,
    parents: SmallVec<[Order; 2]>,
}

impl SplitableSpan for Parents {
    fn len(&self) -> usize { self.order.order_len() as usize }

    fn truncate(&mut self, _at: usize) -> Self { unimplemented!(); }

    fn can_append(&self, other: &Self) -> bool {
        other.parents.len() == 1 && other.parents[0] == self.order.end - 1
    }

    fn append(&mut self, other: Self) {
        self.order.end = other.order.end;
    }
}

fn write_parents(into: &mut Vec<u8>, val: Parents) {
    // dbg!(&val);
    let mut iter = val.parents.iter().peekable();
    loop {
        if let Some(&p) = iter.next() {
            let is_last = iter.peek().is_none();
            let mut diff = val.order.start.wrapping_sub(p);
            diff = mix_bit_u32(diff, is_last);
            // dbg!(diff);
            push_u32(into, diff);
        } else { break; }
    }

    push_u32(into, val.order.order_len());
}

#[cfg(test)]
mod tests {
    use rle::test_splitable_methods_valid;

    use crate::list::encoding::*;
    use crate::list::external_txn::{RemoteCRDTOp, RemoteId, RemoteTxn};
    use crate::list::ListCRDT;
    use crate::test_helpers::root_id;
    use smallvec::smallvec;

    #[test]
    fn simple_encode() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi".into());
        doc.local_delete(0, 1, 1);

        let out = doc.encode_small(true);
        dbg!(&out);
    }

    #[test]
    fn splitable_span_checks() {
        test_splitable_methods_valid(Run { len: 5, val: true });
        test_splitable_methods_valid(Run { len: 5, val: false });

        test_splitable_methods_valid(Run2::<true> { diff: 1, len: 5 });
        test_splitable_methods_valid(Run2::<true> { diff: 2, len: 5 });
        test_splitable_methods_valid(Run2::<true> { diff: -1, len: 5 });

        test_splitable_methods_valid(Run2::<false> { diff: 1, len: 5 });
        test_splitable_methods_valid(Run2::<false> { diff: 2, len: 5 });
        test_splitable_methods_valid(Run2::<false> { diff: -1, len: 5 });
    }

    #[test]
    fn alternate_assumes_positive_first() {
        let mut runs = SpanWriter::new_with_val(PosNegRun(0), push_pos_neg);

        // If we start with positive numbers, we should just get the positive values out.
        runs.push(PosNegRun(10));
        runs.push(PosNegRun(-5));

        let out = runs.flush_into_inner();
        assert_eq!(vec![10, 5], BufReader(out.as_slice()).collect::<Vec<u64>>());

        // But if we start with negative numbers, we get a 0 out first.
        let mut runs = SpanWriter::new_with_val(PosNegRun(0), push_pos_neg);

        // If we start with positive numbers, we should just get the positive values out.
        runs.push(PosNegRun(-10));
        runs.push(PosNegRun(5));

        let out = runs.flush_into_inner();
        assert_eq!(vec![0, 10, 5], BufReader(out.as_slice()).collect::<Vec<u64>>());
    }

    #[test]
    fn encode_decode() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        doc.local_insert(0, 0, "hi".into());
        doc.local_delete(1, 1, 1);
        doc.local_insert(0, 1, "o".into());

        // dbg!(&doc);

        // let enc = doc.encode_small(true);
        // let _dec = ListCRDT::from_bytes(enc.as_slice());

        let _enc = doc.encode_patches(true);

        // let mut spans = doc.content_tree.iter().collect::<Vec<YjsSpan>>();
        // spans.sort_by_key(|s| s.order);
        // dbg!(spans);
        // assert_eq!(doc, dec);
    }

    #[test]
    fn encode_complex_tree() {
        let mut doc = ListCRDT::new();
        // doc.get_or_create_agent_id("seph");
        // doc.local_insert(0, 0, "xxx");

        // Two users will have edits which interleave each other. These will get flattened out
        // by the encoder.
        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "a".into(), seq: 0 },
            parents: smallvec![root_id()],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: root_id(),
                origin_right: root_id(),
                len: 1,
                content_known: false
            }],
            ins_content: "".into(),
        });

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "b".into(), seq: 0 },
            parents: smallvec![root_id()],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: root_id(),
                origin_right: root_id(),
                len: 10,
                content_known: false
            }],
            ins_content: "".into(),
        });

        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "a".into(), seq: 1 },
            parents: smallvec![RemoteId { agent: "a".into(), seq: 0 }],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: RemoteId { agent: "a".into(), seq: 0 },
                origin_right: root_id(),
                len: 20,
                content_known: false
            }],
            ins_content: "".into(),
        });
        doc.apply_remote_txn(&RemoteTxn {
            id: RemoteId { agent: "c".into(), seq: 0 },
            parents: smallvec![
                RemoteId { agent: "a".into(), seq: 20 },
                RemoteId { agent: "b".into(), seq: 9 },
            ],
            ops: smallvec![RemoteCRDTOp::Ins {
                origin_left: RemoteId { agent: "a".into(), seq: 0 },
                origin_right: root_id(),
                len: 20,
                content_known: false
            }],
            ins_content: "".into(),
        });

        // dbg!(&doc.txns);
        doc.check(true);

        doc.encode_patches(true);
    }
}

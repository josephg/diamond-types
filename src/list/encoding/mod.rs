mod varint;

use crate::list::{ListCRDT, Order};
use std::io::Write;
use std::mem::{size_of, replace};
use crate::list::encoding::varint::*;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use crate::rle::KVPair;
use crate::list::span::YjsSpan;
use num_enum::TryFromPrimitive;
use std::convert::TryFrom;

struct BitWriter<W: Write> {
    to: W,
    buf: u32,
    buf_pos: u8,
}

impl<W: Write> BitWriter<W> {
    fn new(writer: W) -> Self {
        Self {
            to: writer,
            buf: 0,
            buf_pos: 0
        }
    }

    fn unwrap(self) -> W {
        self.to
    }

    fn append(&mut self, bit: bool) {
        self.buf |= (bit as u32) << self.buf_pos;
        self.buf_pos += 1;

        if self.buf_pos > size_of::<u32>() as u8 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buf_pos > 0 {
            self.to.write_all(&self.buf.to_le_bytes()).unwrap();
            self.buf_pos = 0;
            self.buf = 0;
        }
        self.to.flush().unwrap();
    }
}

#[derive(Debug, Clone, Default)]
struct SpanWriter<S: SplitableSpan + Clone + Debug, F: FnMut(S, &mut Vec<u8>)> {
    dest: Vec<u8>,
    last: Option<S>,
    flush: F,
    
    // #[cfg(debug_assertions)]
    pub count: usize,
}

impl<S: SplitableSpan + Clone + Debug, F: FnMut(S, &mut Vec<u8>)> SpanWriter<S, F> {
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
    
    pub fn append(&mut self, s: S) {
        assert!(s.len() > 0);
        // if s.len() == 0 { return; }
        // println!("append {:?}", &s);
        if let Some(last) = self.last.as_mut() {
            if last.can_append(&s) {
                last.append(s);
            } else {
                let old = replace(last, s);
                self.count += 1;
                (self.flush)(old, &mut self.dest);
            }
        } else {
            self.last = Some(s);
        }
    }

    pub fn flush_into_inner(mut self) -> Vec<u8> {
        if let Some(elem) = self.last.take() {
            self.count += 1;
            (self.flush)(elem, &mut self.dest);
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


fn write_run<const INC: bool>(run: Run2<INC>, vec: &mut Vec<u8>) {
    let mut dest = [0u8; 10];
    let mut pos = 0;
    // println!("{:?}", run);
    pos += encode_i32(run.diff as i32, &mut dest[..]);
    pos += encode_u32(run.len as u32, &mut dest[pos..]);
    vec.extend_from_slice(&dest[..pos]);
}

fn write_run_2<const INC: bool>(run: Run2<INC>, vec: &mut Vec<u8>) {
    let mut dest = [0u8; 15];
    let mut pos = 0;
    pos += encode_i64_with_extra_bit(run.diff as i64, run.len != 1, &mut dest[..]);
    if run.len != 1 {
        pos += encode_u32(run.len as u32, &mut dest[pos..]);
    }

    vec.extend_from_slice(&dest[..pos]);
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
        let (val, count) = decode_u32(&self.0);
        self.consume(count);
        val
    }

    fn next_u64(&mut self) -> u64 {
        let (val, count) = decode_u64(&self.0);
        self.consume(count);
        val
    }

    fn next_usize(&mut self) -> usize {
        let (val, count) = decode_u64(&self.0);
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

    fn next_str(&mut self) -> &str {
        let len = self.next_usize();
        let bytes = self.next_n_bytes(len);
        std::str::from_utf8(bytes).unwrap()
    }

    fn next_u32_run<const INC: bool>(&mut self, last: &mut u32) -> Option<Run<u32>> {
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

#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
enum Chunk {
    FileInfo = 1,
    Frontier = 2,

    InsOrDelFlags = 3,
    DelData = 4,

    InsOrigins = 5, // Not used by encode_small.
    InsLeftOrigins = 6,
    InsRightOrigins = 7,

    Content = 8,
}


impl ListCRDT {
    // pub fn encode_small<W: Write>(&self, writer: &mut W, verbose: bool) -> std::io::Result<()> {
    pub fn encode_small(&self, verbose: bool) -> Vec<u8> {
        let mut ins_del_runs = SpanWriter::new_with_val(0, |len: i32, vec: &mut Vec<u8>| {
            // The lengths alternate back and forth each operation.
            push_u32(vec, len.abs() as u32);
        });

        let mut del_spans = SpanWriter::new(write_run_2);

        let mut next_order = 0;
        let mut dv = DiffVal(0);
        for KVPair(order, d) in self.deletes.iter() {
            let target = dv.next_run::<true>(d.order as _, d.len as _);
            // Some((target, *order, d.len))
            if *order > next_order {
                ins_del_runs.append((*order - next_order) as i32);
            }
            // dbg!((d.end(), *order));
            ins_del_runs.append(-(d.len as i32));
            next_order = *order + d.len;

            del_spans.append(target);
        }

        let mut entries = self.range_tree.iter().collect::<Vec<YjsSpan>>();
        entries.sort_by_key(|e| e.order);
        // dbg!(&entries[..10]);

        let mut left_origin_runs = SpanWriter::new(write_run_2);
        let mut right_origin_runs = SpanWriter::new(write_run_2);


        let mut prev_ol = 0;
        let mut prev_or = DiffVal(0);
        for entry in entries.iter() {
            let len = entry.len();

            // This wrapping and casting is a bit weird. I'm using it to force ROOT_ORIGIN
            // (u32::MAX) to encode as -1 instead of 2^32-1.
            left_origin_runs.append(Run2::<true> {
                diff: entry.origin_left.wrapping_sub(prev_ol) as i32 as isize,
                len: 1
            });
            prev_ol = if len == 1 {
                entry.origin_left
            } else { // len > 1
                left_origin_runs.append(Run2::<true> {
                    diff: entry.order.wrapping_sub(entry.origin_left) as i32 as isize,
                    len: len - 1
                });
                entry.order + len as u32 - 2
            };

            let diff_ro = prev_or.next(entry.origin_right);
            right_origin_runs.append(Run2::<false> { diff: diff_ro as isize, len });
        }

        let ins_del_runs_data = ins_del_runs.flush_into_inner();
        let del_data = del_spans.flush_into_inner();
        let lo_data = left_origin_runs.flush_into_inner();
        let ro_data = right_origin_runs.flush_into_inner();

        // TODO: Avoid this allocation and just use write_all() to the writer for each section.
        let mut result: Vec<u8> = Vec::with_capacity(ins_del_runs_data.len() + del_data.len() + lo_data.len() + ro_data.len() + 1000);
        // writer.write_all(&MAGIC_BYTES_SMALL)?;
        // writer.write_all(runs_data.as_slice())?;
        // writer.write_all(del_data.as_slice())?;
        // writer.write_all(lo_data.as_slice())?;
        // writer.write_all(ro_runs.as_slice())?;
        result.extend_from_slice(&MAGIC_BYTES_SMALL);
        push_chunk(&mut result, Chunk::FileInfo, &[]);

        let mut frontier_data = vec!();
        for v in self.frontier.iter() {
            push_u32(&mut frontier_data, *v);
        }
        push_chunk(&mut result, Chunk::Frontier, frontier_data.as_slice());
        push_chunk(&mut result, Chunk::InsOrDelFlags, ins_del_runs_data.as_slice());
        push_chunk(&mut result, Chunk::DelData, del_data.as_slice());
        push_chunk(&mut result, Chunk::InsLeftOrigins, lo_data.as_slice());
        push_chunk(&mut result, Chunk::InsRightOrigins, ro_data.as_slice());

        // result.extend_from_slice(runs_data.as_slice());
        // result.extend_from_slice(del_data.as_slice());
        // result.extend_from_slice(lo_data.as_slice());
        // result.extend_from_slice(ro_runs.as_slice());

        if let Some(d) = self.text_content.as_ref() {
            // push_usize(&mut result, d.len_bytes());
            push_chunk_header(&mut result, Chunk::Content, d.len_bytes());
            for chunk in d.chunks() {
                // writer.write_all(chunk.as_bytes());
                result.extend_from_slice(chunk.as_bytes());
            }
        }

        if verbose {
            println!("\n===== encoding stats 3 =====");

            println!("Del run info {}", ins_del_runs_data.len());
            println!("Del data {}", del_data.len());

            println!("left origin RLE {} bytes", lo_data.len());

            // dbg!(right_origin_runs.count);
            println!("right origin RLE {} bytes", ro_data.len());

            println!("without del {}", lo_data.len() + ro_data.len());

            println!("total (no doc content) {}", ins_del_runs_data.len() + del_data.len() + lo_data.len() + ro_data.len());

            // if let Some(d) = self.text_content.as_ref() {
            //     println!("total (with doc content) {}", runs_data.len() + del_data.len() + lo_data.len() + ro_runs.len() + d.len_bytes());
            // }
            println!("total {}", result.len());
        }

        // std::io::Result::Ok(())
        // writer.write_all(result.as_slice())
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut result = ListCRDT::new();

        let mut reader = BufReader(bytes);
        reader.read_magic();

        let _info = reader.expect_chunk(Chunk::FileInfo);

        let frontier_data = reader.expect_chunk(Chunk::Frontier);
        result.frontier = frontier_data.map(|o| o as u32).collect();

        let _ins_del_flags = reader.expect_chunk(Chunk::InsOrDelFlags);
        let _del_data = reader.expect_chunk(Chunk::DelData);
        let mut ins_lo = reader.expect_chunk(Chunk::InsLeftOrigins);
        let mut ins_ro = reader.expect_chunk(Chunk::InsRightOrigins);
        // dbg!(ins_del_flags.collect::<Vec<u64>>());
        // dbg!(del_data.collect::<Vec<u64>>());
        // dbg!(ins_lo.collect::<Vec<u64>>());
        // dbg!(ins_ro.collect::<Vec<u64>>());

        let mut last: Order = 0;
        // let mut n = 0;
        while let Some(lo_run) = ins_lo.next_u32_run::<true>(&mut last) {
            dbg!(lo_run.len);
            // n += 1;
            // if n > 1000 {
            //     break;
            // }
        }
        // while let Some(val) = ins_lo.next() {
        //     let (diff, has_len) = num_decode_i64_with_extra_bit(val);
        //     // dbg!((diff, has_len));
        //     last = last.wrapping_add(diff as i32 as u32);
        //     let len = if has_len {
        //         ins_lo.next().unwrap()
        //     } else {
        //         1
        //     };
        //     println!("LO order {} len {}", last, len);
        //     last = last.wrapping_add(len as u32 - 1);
        // }

        let mut last: Order = 0;
        // n = 0;
        while let Some(ro_run) = ins_ro.next_u32_run::<false>(&mut last) {
            dbg!(ro_run.len);
            // n += 1;
            // if n > 30 {
            //     break;
            // }
        }
        // println!("");
        // while let Some(val) = ins_ro.next() {
        //     let (diff, has_len) = num_decode_i64_with_extra_bit(val);
        //     last = last.wrapping_add(diff as i32 as u32);
        //     let len = if has_len {
        //         ins_ro.next().unwrap()
        //     } else {
        //         1
        //     };
        //     println!("RO order {} len {}", last, len);
        // }

        // TODO: Optional!
        let _content = reader.expect_chunk(Chunk::Content);


        // dbg!(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;
    use crate::splitable_span::{test_splitable_methods_valid};
    use crate::list::encoding::*;

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
        let mut runs = SpanWriter::new_with_val(0, |len: i32, vec: &mut Vec<u8>| {
            push_u32(vec, len.abs() as u32);
        });
        runs.last = Some(0);

        // If we start with positive numbers, we should just get the positive values out.
        runs.append(10);
        runs.append(-5);

        let out = runs.flush_into_inner();
        assert_eq!(vec![10, 5], BufReader(out.as_slice()).collect::<Vec<u64>>());

        // But if we start with negative numbers, we get a 0 out first.
        let mut runs = SpanWriter::new_with_val(0, |len: i32, vec: &mut Vec<u8>| {
            push_u32(vec, len.abs() as u32);
        });
        runs.last = Some(0);

        // If we start with positive numbers, we should just get the positive values out.
        runs.append(-10);
        runs.append(5);

        let out = runs.flush_into_inner();
        assert_eq!(vec![0, 10, 5], BufReader(out.as_slice()).collect::<Vec<u64>>());
    }

    #[test]
    fn encode_decode() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi".into());
        doc.local_delete(0, 1, 1);
        doc.local_insert(0, 1, "o".into());

        let enc = doc.encode_small(true);
        let _dec = ListCRDT::from_bytes(enc.as_slice());

        let mut spans = doc.range_tree.iter().collect::<Vec<YjsSpan>>();
        spans.sort_by_key(|s| s.order);
        dbg!(spans);
        // assert_eq!(doc, dec);
    }

    // other.order == self.order + len
    // && other.origin_left == other.order - 1

    // #[test]
    // fn foo() {
    //     let mut left_origin_runs = SpanWriter::new(|e, v| {
    //         dbg!(e);
    //     });
    //
    //     for (len, d_lo1, d_lo2) in [
    //         YjsSpan {
    //             order: 10,
    //             origin_left: 50,
    //             origin_right: 0,
    //             len: 1
    //         },
    //         YjsSpan {
    //             order: 11,
    //             origin_left: 10,
    //             origin_right: 0,
    //             len: 5
    //         }
    //     ].iter().scan(0, |state, entry| {
    //
    //         let diff_lo_1 = entry.origin_left as isize - *state as isize;
    //         let diff_lo_2 = entry.order as isize - entry.origin_left as isize;
    //         *state = entry.origin_left_at_offset(entry.len() as u32 - 1);
    //
    //         Some((entry.len(), diff_lo_1, diff_lo_2))
    //     }) {
    //         left_origin_runs.append(Run3::<true> { jump: d_lo1, len: 1 });
    //         if len > 1 {
    //             left_origin_runs.append(Run3::<true> { jump: d_lo2, len: len - 1 });
    //         }
    //     }
    //
    //     left_origin_runs.flush_into_inner();
    // }
}
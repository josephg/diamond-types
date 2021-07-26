mod varint;

use crate::list::{ListCRDT, Order};
use std::io::Write;
use std::ops::{BitOrAssign};
use std::mem::{size_of, replace};
use crate::range_tree::CRDTItem;
use crate::list::encoding::varint::{encode_i32, encode_u32};
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;


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
        self.buf.bitor_assign((bit as u32) << self.buf_pos);
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
    
    pub fn append(&mut self, s: S) {
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
    len: usize,
    val: V,
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
struct Run2 {
    jump: isize,
    len: usize,
}

impl SplitableSpan for Run2 {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = Self {
            jump: 1,
            len: self.len - at,
        };
        self.len = at;
        remainder
    }

    fn can_append(&self, other: &Self) -> bool { other.jump == 1 }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) {
        self.jump = other.jump;
        self.len += other.len;
    }
}

impl ListCRDT {
    fn write_order<W: Write>(writer: &mut W, order: Order) {
        writer.write_all(&order.to_be_bytes()).unwrap()
    }


    pub fn write_to<W: Write>(&self, writer: &mut W) {
        for v in self.frontier.iter() {
            Self::write_order(writer, *v);
        }

        let mut bits = BitWriter::new(vec![]);
        for entry in self.range_tree.iter() {
            bits.append(entry.is_deactivated());
        }
        bits.flush();
        writer.write_all(&bits.unwrap()).unwrap();
    }

    pub fn write_encoding_stats(&self) {
        println!("\n===== encoding stats - time order edition =====");


    }

    pub fn write_encoding_stats_2(&self) {
        println!("\n===== encoding stats =====");

        let mut del_runs = SpanWriter::new(|len: i32, vec: &mut Vec<u8>| {
            let mut dest = [0u8; 5];
            let len = encode_u32(len.abs() as u32, &mut dest[..]);
            vec.extend_from_slice(&dest[..len]);
        });
        // let mut del_runs = SpanWriter::new();

        let mut ro_runs = SpanWriter::new(|run: Run<u32>, vec: &mut Vec<u8>| {
            let mut dest = [0u8; 10];
            let mut pos = 0;
            // println!("{:?}", run);
            pos += encode_u32(run.val as u32, &mut dest[..]);
            pos += encode_u32(run.len as u32, &mut dest[pos..]);
            vec.extend_from_slice(&dest[..pos]);
        });

        for entry in self.range_tree.iter() {
            del_runs.append(entry.len);
            ro_runs.append(Run {
                len: entry.len(),
                val: entry.origin_right,
            });

            // order_runs.append(OrderSpan {
            //     order: entry.order,
            //     len: entry.len() as u32
            // });

            // println!("{}", entry.origin_right);
        }

        let mut order_runs = SpanWriter::new(|run: Run2, vec: &mut Vec<u8>| {
            let mut dest = [0u8; 10];
            let mut pos = 0;
            // println!("{:?}", run);
            pos += encode_i32(run.jump as i32, &mut dest[..]);
            pos += encode_u32(run.len as u32, &mut dest[pos..]);
            vec.extend_from_slice(&dest[..pos]);
        });
        let mut left_origin_runs = SpanWriter::new(|run: Run2, vec: &mut Vec<u8>| {
            let mut dest = [0u8; 10];
            let mut pos = 0;
            // println!("{:?}", run);
            pos += encode_i32(run.jump as i32, &mut dest[..]);
            pos += encode_u32(run.len as u32, &mut dest[pos..]);
            vec.extend_from_slice(&dest[..pos]);
        });
        // let mut write_order_2 = ;
        for x in self.range_tree.iter().scan((0, 0), |state, entry| {
            // State is the previous order & previous LO.
            let diff_origin = entry.order as isize - state.0 as isize;
            state.0 = entry.order + entry.len() as u32 - 1;

            let diff_lo = entry.origin_left as isize - state.1 as isize;
            state.1 = entry.origin_left_at_offset(entry.len() as u32 - 1);

            Some((Run2 {
                jump: diff_origin,
                len: entry.len(),
            }, Run2 {
                jump: diff_lo,
                len: entry.len(),
            }))
        }) {
            order_runs.append(x.0);
            left_origin_runs.append(x.1);
        }

        dbg!(del_runs.count);
        println!("Delete RLE {} bytes", del_runs.flush_into_inner().len());
        // dbg!(order_runs.count);
        // println!("Naive order RLE {} bytes", order_runs.flush_into_inner().len());
        dbg!(order_runs.count);
        let or2_data = order_runs.flush_into_inner();
        println!("order runs 2 RLE {} bytes", or2_data.len());
        dbg!(left_origin_runs.count);
        let lo_data = left_origin_runs.flush_into_inner();
        println!("left origin RLE {} bytes", lo_data.len());

        dbg!(ro_runs.count);
        println!("right origin RLE {} bytes", ro_runs.flush_into_inner().len());

    }
}

#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;

    #[test]
    fn simple_encode() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi".into());
        doc.local_delete(0, 1, 1);

        let mut out = vec![];
        doc.write_to(&mut out);
        dbg!(&out);
    }
}
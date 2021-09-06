mod varint;

use crate::list::{ListCRDT, Order};
use std::io::Write;
use std::mem::{size_of, replace};
use crate::range_tree::CRDTItem;
use crate::list::encoding::varint::*;
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
struct Run2<const INC: bool> {
    jump: isize,
    len: usize,
}

impl<const INC: bool> SplitableSpan for Run2<INC> {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = Self {
            jump: INC as isize,
            len: self.len - at,
        };
        self.len = at;
        remainder
    }

    fn can_append(&self, other: &Self) -> bool { other.jump == INC as isize }
    fn append(&mut self, other: Self) { self.len += other.len; }
    fn prepend(&mut self, other: Self) {
        self.jump = other.jump;
        self.len += other.len;
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct DiffVal(usize);

impl DiffVal {
    fn new() -> Self { Self(0) }

    fn next(&mut self, new_val: usize) -> isize {
        let diff = new_val as isize - self.0 as isize;
        self.0 = new_val;
        diff
    }
    fn next_run(&mut self, new_val: usize, len: usize) -> isize {
        let diff = new_val as isize - self.0 as isize;
        self.0 = new_val + len - 1;
        diff
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

    pub fn write_encoding_stats_2(&self) {
        println!("\n===== encoding stats =====");

        let mut del_runs = SpanWriter::new(|len: i32, vec: &mut Vec<u8>| {
            let mut dest = [0u8; 5];
            // The lengths alternate back and forth each operation.
            let len = encode_u32(len.abs() as u32, &mut dest[..]);
            // let len = encode_i32(len, &mut dest[..]);
            vec.extend_from_slice(&dest[..len]);
        });
        // let mut del_runs = SpanWriter::new();

        for entry in self.range_tree.iter() {
            del_runs.append(entry.len);
        }

        fn write_run<const INC: bool>(run: Run2<INC>, vec: &mut Vec<u8>) {
            let mut dest = [0u8; 10];
            let mut pos = 0;
            // println!("{:?}", run);
            pos += encode_i32(run.jump as i32, &mut dest[..]);
            pos += encode_u32(run.len as u32, &mut dest[pos..]);
            vec.extend_from_slice(&dest[..pos]);
        }
        fn write_run2_2<const INC: bool>(run: Run2<INC>, vec: &mut Vec<u8>) {
            let mut dest = [0u8; 15];
            let mut pos = 0;
            pos += encode_i64_with_extra_bit(run.jump as i64, run.len != 1, &mut dest[..]);
            if run.len != 1 {
                pos += encode_u32(run.len as u32, &mut dest[pos..]);
            }

            vec.extend_from_slice(&dest[..pos]);
        }

        let mut order_runs = SpanWriter::new(write_run2_2);
        let mut left_origin_runs = SpanWriter::new(write_run2_2);
        let mut right_origin_runs = SpanWriter::new(write_run2_2);

        for (len, d_o, d_lo1, d_lo2, d_ro) in self.range_tree.iter().scan((DiffVal(0), 0, DiffVal(0)), |state, entry| {
            // State is the previous order & previous LO.
            let diff_origin = state.0.next_run(entry.order as usize, entry.len());

            let diff_lo_1 = entry.origin_left as isize - state.1 as isize;
            let diff_lo_2 = entry.order as isize - entry.origin_left as isize;
            state.1 = entry.origin_left_at_offset(entry.len() as u32 - 1);

            let diff_ro = state.2.next(entry.origin_right as usize);

            Some((entry.len(), diff_origin, diff_lo_1, diff_lo_2, diff_ro))
        }) {
            order_runs.append(Run2::<true> { jump: d_o, len });

            left_origin_runs.append(Run2::<true> { jump: d_lo1, len: 1 });
            if len > 1 {
                left_origin_runs.append(Run2::<true> { jump: d_lo2, len: len - 1 });
            }

            right_origin_runs.append(Run2::<false> { jump: d_ro, len });
        }



        dbg!(del_runs.count);
        let del_runs_data = del_runs.flush_into_inner();
        println!("Delete RLE {} bytes", del_runs_data.len());
        // dbg!(order_runs.count);
        // println!("Naive order RLE {} bytes", order_runs.flush_into_inner().len());
        dbg!(order_runs.count);
        let or2_data = order_runs.flush_into_inner();
        println!("order runs 2 RLE {} bytes", or2_data.len());
        dbg!(left_origin_runs.count);
        let lo_data = left_origin_runs.flush_into_inner();
        println!("left origin RLE {} bytes", lo_data.len());

        // dbg!(ro_runs.count);
        // println!("right origin RLE {} bytes", ro_runs.flush_into_inner().len());

        dbg!(right_origin_runs.count);
        let ro_runs2 = right_origin_runs.flush_into_inner();
        println!("right origin RLE 2 {} bytes", ro_runs2.len());

        println!("total {}",
                 del_runs_data.len()
            + or2_data.len() + lo_data.len() + ro_runs2.len());

    }

    // pub fn write_encoding_stats_3(&self) {
    //     println!("\n===== encoding stats 3 =====");
    //
    //     let mut del_target = SpanWriter::new(|target: KVPair<OrderSpan>, vec: &mut Vec<u8>| {
    //         // let mut dest = [0u8; 5];
    //         // // The lengths alternate back and forth each operation.
    //         // let len = encode_u32(len.abs() as u32, &mut dest[..]);
    //         // // let len = encode_i32(len, &mut dest[..]);
    //         // vec.extend_from_slice(&dest[..len]);
    //     });
    //
    //
    //     let end_order = self.get_next_order();
    //     let mut order = 0;
    //
    //     while order < end_order {
    //         let len_remaining = end_order - order;
    //
    //         let (next, len) = if let Some((d, offset)) = self.deletes.find(order) {
    //             // Its a delete.
    //
    //             // Limit by #4
    //             let len_limit_2 = u32::min(d.1.len - offset, len_remaining);
    //             // Limit by #5
    //             del_target.append(KVPair(order, OrderSpan {
    //                 order: d.1.order + offset,
    //                 len: len_limit_2
    //             }));
    //             let (id, len) = self.order_to_remote_id_span(d.1.order + offset, len_limit_2);
    //             // dbg!((&id, len));
    //             (RemoteCRDTOp::Del { id, len }, len)
    //         } else {
    //             // It must be an insert. Fish information out of the range tree.
    //             let cursor = self.get_cursor_before(order);
    //             let entry = cursor.get_raw_entry();
    //             // Limit by #4
    //             let len = u32::min((entry.len() - cursor.offset) as u32, len_remaining);
    //
    //             // I'm not fishing out the deleted content at the moment, for any reason.
    //             // This might be simpler if I just make up content for deleted items O_o
    //             let content_known = if entry.is_activated() {
    //                 if let Some(ref text) = self.text_content {
    //                     let pos = unsafe { cursor.count_pos() as usize };
    //                     let content = text.chars_at(pos).take(len as usize);
    //                     ins_content.extend(content);
    //                     true
    //                 } else { false }
    //             } else { false };
    //
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;
    use crate::splitable_span::{test_splitable_methods_valid, SplitableSpan};
    use crate::list::encoding::*;
    use crate::list::span::YjsSpan;

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

    #[test]
    fn splitable_span_checks() {
        test_splitable_methods_valid(Run { len: 5, val: true });
        test_splitable_methods_valid(Run { len: 5, val: false });

        test_splitable_methods_valid(Run2::<true> { jump: 1, len: 5 });
        test_splitable_methods_valid(Run2::<true> { jump: 2, len: 5 });
        test_splitable_methods_valid(Run2::<true> { jump: -1, len: 5 });

        test_splitable_methods_valid(Run2::<false> { jump: 1, len: 5 });
        test_splitable_methods_valid(Run2::<false> { jump: 2, len: 5 });
        test_splitable_methods_valid(Run2::<false> { jump: -1, len: 5 });
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
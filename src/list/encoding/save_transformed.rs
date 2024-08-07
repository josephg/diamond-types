use rle::{AppendRle, HasLength, RleRun};
use crate::encoding::Merger;
use crate::list::encoding::leb::num_encode_zigzag_isize_old;
use crate::list::encoding::encode_tools::{push_leb_usize, write_leb_bit_run};
use crate::list::ListOpLog;
use crate::listmerge::merge::{TransformedResult, TransformedResultRaw};
use crate::LV;

/// *** This is EXPERIMENTAL work-in-progress code to save transformed positions ***

/// This feature isn't implemented yet, but I wanted to get some benchmarking figures for my blog
/// post.


#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum XFState {
    Cancelled,
    XFBy(isize),
}

impl ListOpLog {
    pub fn bench_writing_xf_since(&self, from_version: &[LV]) {
        let mut tn_ops: Vec<RleRun<XFState>> = vec![];

        // for r in self.get_xf_operations_full_raw(from_version, self.cg.version.as_ref()) {
        //     tn_ops.push_rle(match r {
        //         TransformedResultRaw::FF(range) => {
        //             RleRun::new(XFState::XFBy(0), range.len())
        //         },
        //         TransformedResultRaw::Apply { xf_pos, op } => {
        //             RleRun::new(XFState::XFBy(xf_pos as isize - op.1.start() as isize), op.len())
        //         },
        //         TransformedResultRaw::DeleteAlreadyHappened(range) => {
        //             RleRun::new(XFState::Cancelled, range.len())
        //         },
        //     });
        // }

        
        for op in self.get_xf_operations_full(from_version, self.cg.version.as_ref()) {
            let (val, len) = match op {
                TransformedResultRaw::FF(range) => {
                    (XFState::XFBy(0), range.len())
                }
                TransformedResultRaw::Apply { xf_pos, op } => {
                    let origin_pos = op.1.start() as isize;
                    (XFState::XFBy(xf_pos as isize - origin_pos), op.len())
                }
                TransformedResultRaw::DeleteAlreadyHappened(range) => {
                    (XFState::Cancelled, range.len())
                }
            };
            
            tn_ops.push_rle(RleRun { val, len });
        }

        // for (_, op, xf) in self.get_xf_operations_full_old(from_version, self.cg.version.as_ref()) {
        //     let val = match xf {
        //         TransformedResult::BaseMoved(xf_pos) => {
        //             let origin_pos = op.start() as isize;
        //             XFState::XFBy(xf_pos as isize - origin_pos)
        //         },
        //         TransformedResult::DeleteAlreadyHappened => XFState::Cancelled,
        //     };
        // 
        //     tn_ops.push_rle(RleRun {
        //         val,
        //         len: op.len()
        //     });
        // }

        dbg!(&tn_ops.len());

        // First pass: just write it.

        let mut buf = Vec::new();
        let mut buf2 = Vec::new();
        // let mut last: isize = 0;

        // let mut onoff = Vec::new();

        let mut w = Merger::new(write_leb_bit_run);

        for e in tn_ops.iter() {
            // onoff.push_rle(RleRun::new(e.val == XFState::Cancelled, e.len));
            w.push2(RleRun::new(e.val == XFState::Cancelled, e.len), &mut buf2);

            // if e.len > 10000 {
            //     dbg!(e);
            // }
            match e.val {
                XFState::Cancelled => {}
                XFState::XFBy(dist) => {
                    let n = num_encode_zigzag_isize_old(dist);
                    push_leb_usize(&mut buf, n);
                    push_leb_usize(&mut buf, e.len);
                }
            }
        }

        w.flush2(&mut buf2);

        dbg!(buf2.len() + buf.len());
        dbg!(buf2.len());
        // dbg!(onoff.len());

        // dbg!(&onoff);


        // 2.65kb.
        // let mut buf = Vec::new();
        // for e in tn_ops.iter() {
        //     match e.val {
        //         XFState::Cancelled => {}
        //         XFState::XFBy(dist) => {
        //             let n = num_encode_zigzag_isize(dist);
        //             push_usize(&mut buf, n);
        //             push_usize(&mut buf, e.len);
        //         }
        //     }
        // }

        dbg!(buf.len());
    }
}

use jumprope::JumpRope;
use crate::list::{Checkout, OpSet};
use smallvec::smallvec;
use rle::HasLength;
use crate::list::operation::InsDelTag::*;
use crate::list::operation::Operation;
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;
use crate::unicount::consume_chars;

impl Checkout {
    pub fn new() -> Self {
        Self {
            frontier: smallvec![ROOT_TIME],
            content: JumpRope::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.content.len_chars()
    }

    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Apply a single operation. This method does not update the frontier - that is left as an
    /// exercise for the caller.
    pub(crate) fn apply_1(&mut self, op: &Operation) {
        let pos = op.pos;

        match op.tag {
            Ins => {
                assert!(op.content_known);
                self.content.insert(pos, &op.content);
            }

            Del => {
                self.content.remove(pos..pos + op.len());
            }
        }
    }

    /// Apply a set of operations. Does not update frontier.
    pub(crate) fn apply(&mut self, ops: &[Operation]) {
        for c in ops {
            // let content = if c.tag == Ins && c.content_known {
            //     consume_chars(&mut content, c.len())
            // } else { "" };
            self.apply_1(c);
        }
    }

    pub(crate) fn apply_range_from(&mut self, ops: &OpSet, range: TimeSpan) {
        for op in ops.iter_range(range) {
            self.apply_1(&op.1);
        }
    }
}
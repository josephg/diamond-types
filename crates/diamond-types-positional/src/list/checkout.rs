use jumprope::JumpRope;
use crate::list::Checkout;
use smallvec::smallvec;
use rle::HasLength;
use crate::list::operation::InsDelTag::*;
use crate::list::operation::Operation;
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

    pub fn apply_1(&mut self, op: &Operation) {
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

    pub fn apply(&mut self, ops: &[Operation]) {
        for c in ops {
            // let content = if c.tag == Ins && c.content_known {
            //     consume_chars(&mut content, c.len())
            // } else { "" };
            self.apply_1(c);
        }
    }
}
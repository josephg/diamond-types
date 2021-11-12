use jumprope::JumpRope;
use crate::list::Checkout;
use smallvec::smallvec;
use rle::HasLength;
use crate::list::operation::InsDelTag::*;
use crate::list::operation::PositionalComponent;
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

    pub fn apply_1(&mut self, op: &PositionalComponent, content: &str) {
        let pos = op.pos;
        let len = op.len();

        match op.tag {
            Ins => {
                assert!(op.content_known);
                self.content.insert(pos, content);
            }

            Del => {
                self.content.remove(pos..pos + len);
            }
        }
    }

    pub fn apply(&mut self, ops: &[PositionalComponent], mut content: &str) {
        for c in ops {
            let content = if c.tag == Ins && c.content_known {
                consume_chars(&mut content, c.len())
            } else { "" };
            self.apply_1(c, content);
        }
    }
}
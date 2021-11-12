use jumprope::JumpRope;
use crate::list::Checkout;
use smallvec::smallvec;
use crate::ROOT_TIME;

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
}
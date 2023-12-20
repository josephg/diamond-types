// I use Range<Order> a bunch internally. Its nice to have some extra methods on them

use crate::list::LV;
use std::ops::Range;

pub(crate) trait OrderRange {
    fn last_order(&self) -> LV;
    fn order_len(&self) -> LV;

    fn transpose(&self, new_start: LV) -> Self;
}

impl OrderRange for Range<LV> {
    fn last_order(&self) -> LV {
        self.end - 1
    }

    fn order_len(&self) -> LV {
        self.end - self.start
    }

    fn transpose(&self, new_start: LV) -> Self {
        new_start..new_start + self.order_len()
    }
}
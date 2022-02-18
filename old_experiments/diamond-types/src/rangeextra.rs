// I use Range<Order> a bunch internally. Its nice to have some extra methods on them

use crate::list::Time;
use std::ops::Range;

pub(crate) trait OrderRange {
    fn last_order(&self) -> Time;
    fn order_len(&self) -> Time;

    fn transpose(&self, new_start: Time) -> Self;
}

impl OrderRange for Range<Time> {
    fn last_order(&self) -> Time {
        self.end - 1
    }

    fn order_len(&self) -> Time {
        self.end - self.start
    }

    fn transpose(&self, new_start: Time) -> Self {
        new_start..new_start + self.order_len()
    }
}
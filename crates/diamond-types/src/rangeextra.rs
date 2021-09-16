// I use Range<Order> a bunch internally. Its nice to have some extra methods on them

use crate::list::Order;
use std::ops::Range;

pub(crate) trait OrderRange {
    fn last_order(&self) -> Order;
    fn order_len(&self) -> Order;
}

impl OrderRange for Range<Order> {
    fn last_order(&self) -> Order {
        self.end - 1
    }

    fn order_len(&self) -> Order {
        self.end - self.start
    }
}
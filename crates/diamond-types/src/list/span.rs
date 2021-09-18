use rle::Searchable;
use rle::SplitableSpan;

use content_tree::ContentLength;
use content_tree::Toggleable;
use crate::list::Order;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct YjsSpan {
    pub order: Order,

    /**
     * The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: Order,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: Order,

    pub len: i32, // negative if deleted.
}

impl YjsSpan {
    pub fn origin_left_at_offset(&self, at: u32) -> Order {
        if at == 0 { self.origin_left }
        else { self.order + at - 1 }
    }

    pub fn activated(mut self) -> Self {
        self.len = self.len.abs();
        self
    }

    pub fn order_len(&self) -> Order {
        self.len.abs() as _
    }
}

impl SplitableSpan for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.len.abs() as usize }

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        let at_signed = at as i32 * self.len.signum();
        let other = YjsSpan {
            order: self.order + at as Order,
            origin_left: self.order + at as u32 - 1,
            origin_right: self.origin_right,
            len: self.len - at_signed
        };

        self.len = at_signed;
        other
    }

    // Could have a custom truncate_keeping_right method here - I once did. But the optimizer
    // does a great job flattening the generic implementation anyway.

    // This method gets inlined all over the place.
    // TODO: Might be worth tagging it with inline(never) and seeing what happens.
    fn can_append(&self, other: &Self) -> bool {
        let len = self.len.abs() as u32;
        (self.len > 0) == (other.len > 0)
            && other.order == self.order + len
            && other.origin_left == other.order - 1
            && other.origin_right == self.origin_right
    }

    #[inline(always)]
    fn append(&mut self, other: Self) {
        self.len += other.len
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(other.can_append(self));
        self.order = other.order;
        self.len += other.len;
        self.origin_left = other.origin_left;
    }
}

impl Searchable for YjsSpan {
    type Item = Order;

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        if (loc >= self.order) && (loc < self.order + self.len.abs() as u32) {
            Some((loc - self.order) as usize)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.order + offset as Order
    }
}

impl ContentLength for YjsSpan {
    #[inline(always)]
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }
}

impl Toggleable for YjsSpan {
    fn is_activated(&self) -> bool {
        self.len > 0
    }

    fn mark_activated(&mut self) {
        debug_assert!(self.len < 0);
        self.len = -self.len;
    }

    fn mark_deactivated(&mut self) {
        debug_assert!(self.len > 0);
        self.len = -self.len
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use rle::test_splitable_methods_valid;

    use crate::list::span::YjsSpan;

    #[test]
    fn print_span_sizes() {
        println!("size of YjsSpan {}", size_of::<YjsSpan>());
    }

    #[test]
    fn yjsspan_entry_valid() {
        test_splitable_methods_valid(YjsSpan {
            order: 10,
            origin_left: 20,
            origin_right: 30,
            len: 5
        });

        test_splitable_methods_valid(YjsSpan {
            order: 10,
            origin_left: 20,
            origin_right: 30,
            len: -5
        });
    }
}
// This file contains some integration tests for the range tree
#[cfg(test)]
mod tests {
    use crate::range_tree::{RangeTree, AbsPositionIndex};
    use std::pin::Pin;
    use crate::order::OrderMarker;

    #[test]
    fn abs_position_append() {
        let mut t: Pin<Box<RangeTree<OrderMarker, AbsPositionIndex>>> = RangeTree::new();
        let c = t.cursor_at_end();
        t.replace_range(c, OrderMarker {
            order: 50,
            len: 100
        }, |_, _| {});

        let c = t.cursor_at_position(80, false);
        assert_eq!(c.get_item(), Some(80));
        // dbg!(c);
        // dbg!(t);
    }
}
use std::fmt::{Debug, Formatter};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::{DTRange, LV};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub(crate) enum SpanState {
    NotInsertedYet = 0, Inserted = 1, Deleted = 2
}
use SpanState::*;

impl SpanState {
    pub(crate) fn max(a: Self, b: Self) -> Self {
        let result = (a as u8).max(b as u8);
        match result {
            0 => NotInsertedYet,
            1 => Inserted,
            2 => Deleted,
            _ => unreachable!(), // The compiler can prove this.
        }
    }
}

impl Default for SpanState {
    fn default() -> Self { NotInsertedYet }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct YjsSpan {
    /// The local version of the corresponding insert operation. This is needed when items are
    /// inserted at the same location, so we can figure out how to compare them.
    pub id: DTRange,

    /// NOTE: The origin_left is only for the first item in the span. Each subsequent item has an
    /// origin_left of order+offset
    pub origin_left: LV,

    /// Each item in the span has the same origin_right.
    pub origin_right: LV,
}

// impl SpanState {
//     pub(crate) fn insert(&mut self) {
//         assert_eq!(*self, NotInsertedYet);
//         *self = Inserted;
//     }
//
//     /// Note this doesn't (can't) set the ever_deleted flag. Use yjsspan.delete() instead.
//     pub(crate) fn delete(&mut self) {
//         assert_eq!(*self, Inserted);
//         *self = Deleted;
//     }
// }

// Some inserts don't have anything concurrent with them. In this case, we don't care about
// origin_left / origin_right or preserving the ID range. These items will have an ID that starts
// at usize::MAX / 4.
pub(crate) const UNDIFFERENTIATED_START: usize = usize::MAX / 4;

pub(crate) fn is_undiff(time: LV) -> bool {
    time >= UNDIFFERENTIATED_START
}


impl YjsSpan {
    pub fn origin_left_at_offset(&self, offset: LV) -> LV {
        if offset == 0 { self.origin_left }
        else { self.id.start + offset - 1 }
    }

    pub fn new_undiff_max() -> Self {
        YjsSpan {
            id: DTRange::new(UNDIFFERENTIATED_START, UNDIFFERENTIATED_START * 2 - 1),
            origin_left: usize::MAX,
            origin_right: usize::MAX,
        }
    }

    pub fn new_undiff(len: usize) -> Self {
        YjsSpan {
            id: DTRange::new(UNDIFFERENTIATED_START, UNDIFFERENTIATED_START + len),
            origin_left: usize::MAX,
            origin_right: usize::MAX,
        }
    }

    pub fn is_undiff(&self) -> bool {
        self.id.start >= UNDIFFERENTIATED_START
    }

    pub(crate) fn content_len_with_state(&self, state: SpanState) -> usize {
        if state == Inserted { self.id.len() } else { 0 }
    }

    // pub fn upstream_len_at(&self, offset: usize) -> usize {
    //     if self.ever_deleted { 0 } else { offset }
    // }
}

// So the length is described in two ways - one for the current content position, and the other for
// the merged upstream perspective of this content.
impl HasLength for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.id.len() }
}

impl SplitableSpanHelpers for YjsSpan {
    fn truncate_h(&mut self, offset: usize) -> Self {
        debug_assert!(offset > 0);
        debug_assert!(offset < self.len());

        // Could make this behave differently for undifferentiated items, but I don't think it
        // matters.
        YjsSpan {
            id: self.id.truncate(offset),
            origin_left: self.id.start + offset - 1,
            origin_right: self.origin_right,
        }
    }
}

impl MergableSpan for YjsSpan {
    // Could have a custom truncate_keeping_right method here - I once did. But the optimizer
    // does a great job flattening the generic implementation anyway.

    fn can_append(&self, other: &Self) -> bool {
        match (self.is_undiff(), other.is_undiff()) {
            (true, true) => true,
            (false, false) => {
                self.id.can_append(&other.id)
                    && other.origin_left == other.id.start - 1
                    && other.origin_right == self.origin_right
            },
            _ => false,
        }
    }

    #[inline(always)]
    fn append(&mut self, other: Self) {
        if self.is_undiff() {
            self.id.end += other.len();
        } else {
            self.id.append(other.id)
        }
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(other.can_append(self));
        if self.is_undiff() {
            self.id.end += other.len();
        } else {
            self.id.prepend(other.id);
            self.origin_left = other.origin_left;
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct YjsSpanWithState(pub YjsSpan, pub SpanState);

impl MergableSpan for YjsSpanWithState {
    fn can_append(&self, other: &Self) -> bool {
        self.1 == other.1 && self.0.can_append(&other.0)
    }

    fn append(&mut self, other: Self) {
        self.0.append(other.0)
    }
}

// impl Searchable for YjsSpan {
//     type Item = LV;
//
//     fn get_offset(&self, loc: Self::Item) -> Option<usize> {
//         self.id.get_offset(loc)
//     }
//
//     fn at_offset(&self, offset: usize) -> Self::Item {
//         self.id.start + offset
//     }
// }

// impl ContentLength for YjsSpan {
//     #[inline(always)]
//     fn content_len(&self) -> usize {
//         if self.state == INSERTED { self.len() } else { 0 }
//     }
//
//     fn content_len_at_offset(&self, offset: usize) -> usize {
//         if self.state == INSERTED { offset } else { 0 }
//     }
// }

// impl Toggleable for YjsSpan {
//     fn is_activated(&self) -> bool {
//         self.state == INSERTED
//         // self.state == Inserted && !self.ever_deleted
//     }
//
//     fn mark_activated(&mut self) {
//         panic!("Cannot mark activated");
//         // Not entirely sure this logic is right.
//         // self.state.undelete();
//     }
//
//     fn mark_deactivated(&mut self) {
//         // debug_assert!(!self.is_deleted);
//         // self.state.delete();
//         self.delete();
//     }
// }

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn print_span_sizes() {
        // 40 bytes (compared to just 16 bytes in the older implementation).
        println!("size of YjsSpan {}", size_of::<YjsSpan>());
    }

    #[test]
    fn yjsspan_entry_valid() {
        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
        });

        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
        });

        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
        });
    }

    #[ignore]
    #[test]
    fn print_size() {
        dbg!(std::mem::size_of::<YjsSpan>());
        dbg!(std::mem::size_of::<SpanState>());
    }
}
use std::fmt::{Debug, Formatter};
use content_tree::{ContentLength, Toggleable};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};
use crate::list::Time;
use crate::localtime::{debug_time, TimeSpan, UNDERWATER_START};
use crate::ROOT_TIME;

#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct YjsSpan {
    /// The local times for this entry
    pub id: TimeSpan,

    /**
     * NOTE: The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: Time,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: Time,

    pub is_deleted: bool,
}

impl Debug for YjsSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("YjsSpan");
        s.field("id", &self.id);
        debug_time(&mut s, "origin_left", self.origin_left);
        debug_time(&mut s, "origin_right", self.origin_right);
        s.field("is_deleted", &self.is_deleted);
        s.finish()
    }
}

impl YjsSpan {
    pub fn origin_left_at_offset(&self, offset: Time) -> Time {
        if offset == 0 { self.origin_left }
        else { self.id.start + offset - 1 }
    }

    // pub fn clone_activated(mut self) -> Self {
    //     self.len = self.len.abs();
    //     self
    // }

    pub fn new_underwater() -> Self {
        YjsSpan {
            id: TimeSpan::new(UNDERWATER_START, UNDERWATER_START * 2),
            origin_left: ROOT_TIME,
            origin_right: ROOT_TIME,
            is_deleted: false
        }
    }

    pub fn is_underwater(&self) -> bool {
        self.id.start >= UNDERWATER_START
    }
}

impl HasLength for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.id.len() }
}

impl SplitableSpan for YjsSpan {
    fn truncate(&mut self, offset: usize) -> Self {
        debug_assert!(offset > 0);
        // let at_signed = offset as i32 * self.len.signum();
        YjsSpan {
            id: self.id.truncate(offset),
            origin_left: self.id.start + offset - 1,
            origin_right: self.origin_right,
            is_deleted: self.is_deleted
        }
    }

    // fn truncate(&mut self, at: usize) -> Self {
    //     debug_assert!(at > 0);
    //     let at_signed = at as i32 * self.len.signum();
    //     let other = YjsSpan {
    //         id: self.id + at as Time,
    //         origin_left: self.id + at as u32 - 1,
    //         origin_right: self.origin_right,
    //         len: self.len - at_signed
    //     };
    //
    //     self.len = at_signed;
    //     other
    // }
}

impl MergableSpan for YjsSpan {
    // Could have a custom truncate_keeping_right method here - I once did. But the optimizer
    // does a great job flattening the generic implementation anyway.

    fn can_append(&self, other: &Self) -> bool {
        self.id.can_append(&other.id)
            && other.origin_left == other.id.start - 1
            && other.origin_right == self.origin_right
            && other.is_deleted == self.is_deleted
    }

    #[inline(always)]
    fn append(&mut self, other: Self) {
        self.id.append(other.id)
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(other.can_append(self));
        self.id.prepend(other.id);
        self.origin_left = other.origin_left;
    }
}

impl Searchable for YjsSpan {
    type Item = Time;

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        self.id.get_offset(loc)
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.id.start + offset
    }
}

impl ContentLength for YjsSpan {
    #[inline(always)]
    fn content_len(&self) -> usize {
        if self.is_deleted { 0 } else { self.len() }
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        if self.is_deleted { 0 } else { offset }
    }
}

impl Toggleable for YjsSpan {
    fn is_activated(&self) -> bool {
        !self.is_deleted
    }

    fn mark_activated(&mut self) {
        debug_assert!(self.is_deleted);
        self.is_deleted = false;
    }

    fn mark_deactivated(&mut self) {
        debug_assert!(!self.is_deleted);
        self.is_deleted = true;
    }
}

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
            is_deleted: false
        });

        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            is_deleted: true
        });
    }
}
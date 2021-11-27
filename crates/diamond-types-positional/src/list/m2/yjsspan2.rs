use std::fmt::{Debug, Formatter};
use content_tree::{ContentLength, Toggleable};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};
use crate::list::Time;
use crate::localtime::{debug_time, TimeSpan, UNDERWATER_START};
use crate::ROOT_TIME;
use YjsSpanState::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum YjsSpanState {
    NotInsertedYet,
    Inserted,
    // TODO: Somehow guard against malicious delete overflows here.
    Deleted(u16),
}

#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct YjsSpan2 {
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

    // TODO: Replace this field with an integer.
    /// 0 = not inserted yet,
    /// 1 = inserted but not deleted
    /// 2+ = deleted n-1 times.
    /// Enum is used for now to make the code more explicit.
    pub state: YjsSpanState,

    pub ever_deleted: bool,
}

impl Default for YjsSpanState {
    fn default() -> Self { NotInsertedYet }
}

impl YjsSpanState {
    // pub(crate) fn is_deleted(&self) -> bool {
    //     match self {
    //         Deleted(_) => true,
    //         _ => false
    //     }
    // }

    fn delete(&mut self) {
        match self {
            NotInsertedYet => panic!("Cannot deleted NIY item"),
            Inserted => {
                // Most common case.
                *self = Deleted(0);
            }
            Deleted(n) => {
                *n += 1;
            }
        }
    }

    pub(crate) fn undelete(&mut self) {
        if let Deleted(n) = self {
            if *n > 0 { *n -= 1; }
            else {
                // Most common case.
                *self = Inserted
            }
        } else {
            // dbg!(self);
            panic!("Invalid undelete target");
        }
    }

    pub(crate) fn mark_inserted(&mut self) {
        if *self != NotInsertedYet {
            panic!("Invalid insert target - item already marked as inserted");
        }

        *self = Inserted;
    }
    pub(crate) fn mark_not_inserted_yet(&mut self) {
        if *self != Inserted {
            panic!("Invalid insert target - item not inserted");
        }

        *self = NotInsertedYet;
    }
}

impl Debug for YjsSpan2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("YjsSpan");
        s.field("id", &self.id);
        debug_time(&mut s, "origin_left", self.origin_left);
        debug_time(&mut s, "origin_right", self.origin_right);
        s.field("state", &self.state); // Could probably do better than this.
        s.field("ever_deleted", &self.ever_deleted);
        s.finish()
    }
}

impl YjsSpan2 {
    pub fn origin_left_at_offset(&self, offset: Time) -> Time {
        if offset == 0 { self.origin_left }
        else { self.id.start + offset - 1 }
    }

    // pub fn clone_activated(mut self) -> Self {
    //     self.len = self.len.abs();
    //     self
    // }

    pub fn new_underwater() -> Self {
        YjsSpan2 {
            id: TimeSpan::new(UNDERWATER_START, UNDERWATER_START * 2 - 1),
            origin_left: ROOT_TIME,
            origin_right: ROOT_TIME,
            state: Inserted, // Underwater items are never in the NotInsertedYet state.
            ever_deleted: false,
        }
    }

    #[allow(unused)]
    pub fn is_underwater(&self) -> bool {
        self.id.start >= UNDERWATER_START
    }

    pub(crate) fn delete(&mut self) {
        self.state.delete();
        self.ever_deleted = true;
    }

    pub fn upstream_len(&self) -> usize {
        if self.ever_deleted { 0 } else { self.id.len() }
    }

    pub fn upstream_len_at(&self, offset: usize) -> usize {
        if self.ever_deleted { 0 } else { offset }
    }
}

// So the length is described in two ways - one for the current content position, and the other for
// the merged upstream perspective of this content.
//
// I could make a custom index for this, but I'm gonna be lazy and say content length = current,
// and "offset length" = upstream.
impl HasLength for YjsSpan2 {
    #[inline(always)]
    fn len(&self) -> usize { self.id.len() }
}

impl SplitableSpan for YjsSpan2 {
    fn truncate(&mut self, offset: usize) -> Self {
        debug_assert!(offset > 0);
        // let at_signed = offset as i32 * self.len.signum();
        YjsSpan2 {
            id: self.id.truncate(offset),
            origin_left: self.id.start + offset - 1,
            origin_right: self.origin_right,
            state: self.state,
            ever_deleted: self.ever_deleted,
        }
    }
}

impl MergableSpan for YjsSpan2 {
    // Could have a custom truncate_keeping_right method here - I once did. But the optimizer
    // does a great job flattening the generic implementation anyway.

    fn can_append(&self, other: &Self) -> bool {
        self.id.can_append(&other.id)
            && other.origin_left == other.id.start - 1
            && other.origin_right == self.origin_right
            && other.state == self.state
            && other.ever_deleted == self.ever_deleted
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

impl Searchable for YjsSpan2 {
    type Item = Time;

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        self.id.get_offset(loc)
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.id.start + offset
    }
}

impl ContentLength for YjsSpan2 {
    #[inline(always)]
    fn content_len(&self) -> usize {
        if self.state == Inserted { self.len() } else { 0 }
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        if self.state == Inserted { offset } else { 0 }
    }
}

impl Toggleable for YjsSpan2 {
    fn is_activated(&self) -> bool {
        self.state == Inserted
        // self.state == Inserted && !self.ever_deleted
    }

    fn mark_activated(&mut self) {
        panic!("Cannot mark activated");
        // Not entirely sure this logic is right.
        // self.state.undelete();
    }

    fn mark_deactivated(&mut self) {
        // debug_assert!(!self.is_deleted);
        // self.state.delete();
        self.delete();
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
        println!("size of YjsSpan {}", size_of::<YjsSpan2>());
    }

    #[test]
    fn yjsspan_entry_valid() {
        test_splitable_methods_valid(YjsSpan2 {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: NotInsertedYet,
            ever_deleted: false,
        });

        test_splitable_methods_valid(YjsSpan2 {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: Inserted,
            ever_deleted: false
        });

        test_splitable_methods_valid(YjsSpan2 {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: Deleted(0),
            ever_deleted: false
        });
    }
}
use std::fmt::{Debug, Formatter};
use content_tree::{ContentLength, Toggleable};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::LV;
use crate::dtrange::{debug_time, DTRange, UNDERWATER_START};

/// 0 = not inserted yet,
/// 1 = inserted but not deleted
/// 2+ = deleted n-1 times.
///
/// Note a u16 (or even a u8) should be fine in practice. Double deletes almost never happen in
/// reality - unless someone is maliciously generating them.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct YjsSpanState(u32);

pub const NOT_INSERTED_YET: YjsSpanState = YjsSpanState(0);
pub const INSERTED: YjsSpanState = YjsSpanState(1);
pub const DELETED_ONCE: YjsSpanState = YjsSpanState(2);

pub(super) fn deleted_n_state(n: u32) -> YjsSpanState {
    YjsSpanState(1 + n)
}

#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub struct YjsSpan {
    /// The local times for this entry
    pub id: DTRange,

    /**
     * NOTE: The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: LV,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: LV,

    /// Stores whether the item has been inserted, inserted and deleted, or not inserted yet at the
    /// current moment in time.
    pub state: YjsSpanState,

    pub ever_deleted: bool,
}

impl YjsSpanState {
    /// Note this doesn't (can't) set the ever_deleted flag. Use yjsspan.delete() instead.
    fn delete(&mut self) {
        if self.0 == NOT_INSERTED_YET.0 {
            panic!("Cannot deleted NIY item");
        } else {
            // Insert -> Delete, Delete -> Double delete, etc.
            // self.0 += 1;

            // So this case is interesting. Almost every item will only ever be deleted once.
            // Occasionally two branches will delete the same item then merge - in which case we'll
            // store 2. To overflow a u32, we need 4gb of edits which all repeatedly delete the same
            // item in the document - which should never happen except maliciously. Panicking is
            // probably a reasonable choice here. Try not to collaboratively edit documents with
            // malicious actors - this code isn't BFT.
            self.0 = self.0.checked_add(1)
                .expect("Double delete overflow detected. Refusing to merge.");
        }
    }

    pub(crate) fn undelete(&mut self) {
        if self.0 >= DELETED_ONCE.0 {
            // Double delete -> single delete
            // Deleted -> inserted
            self.0 -= 1;
        } else {
            panic!("Invalid undelete target");
        }
    }

    pub(crate) fn mark_inserted(&mut self) {
        if *self != NOT_INSERTED_YET {
            panic!("Invalid insert target - item already marked as inserted");
        }

        *self = INSERTED;
    }
    pub(crate) fn mark_not_inserted_yet(&mut self) {
        if *self != INSERTED {
            panic!("Invalid insert target - item not inserted");
        }

        *self = NOT_INSERTED_YET;
    }
}

impl Debug for YjsSpan {
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

impl YjsSpan {
    pub fn origin_left_at_offset(&self, offset: LV) -> LV {
        if offset == 0 { self.origin_left }
        else { self.id.start + offset - 1 }
    }

    pub fn new_underwater() -> Self {
        YjsSpan {
            id: DTRange::new(UNDERWATER_START, UNDERWATER_START * 2 - 1),
            origin_left: usize::MAX,
            origin_right: usize::MAX,
            state: INSERTED, // Underwater items are never in the NotInsertedYet state.
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
impl HasLength for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.id.len() }
}

impl SplitableSpanHelpers for YjsSpan {
    fn truncate_h(&mut self, offset: usize) -> Self {
        debug_assert!(offset > 0);
        // let at_signed = offset as i32 * self.len.signum();
        YjsSpan {
            id: self.id.truncate(offset),
            origin_left: self.id.start + offset - 1,
            origin_right: self.origin_right,
            state: self.state,
            ever_deleted: self.ever_deleted,
        }
    }
}

impl MergableSpan for YjsSpan {
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

impl Searchable for YjsSpan {
    type Item = LV;

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
        if self.state == INSERTED { self.len() } else { 0 }
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        if self.state == INSERTED { offset } else { 0 }
    }
}

impl Toggleable for YjsSpan {
    fn is_activated(&self) -> bool {
        self.state == INSERTED
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
        println!("size of YjsSpan {}", size_of::<YjsSpan>());
    }

    #[test]
    fn yjsspan_entry_valid() {
        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: NOT_INSERTED_YET,
            ever_deleted: false,
        });

        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: INSERTED,
            ever_deleted: false
        });

        test_splitable_methods_valid(YjsSpan {
            id: (10..15).into(),
            origin_left: 20,
            origin_right: 30,
            state: DELETED_ONCE,
            ever_deleted: false
        });
    }

    #[ignore]
    #[test]
    fn print_size() {
        dbg!(std::mem::size_of::<YjsSpan>());
    }
}
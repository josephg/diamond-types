use std::fmt::Debug;
use std::ptr::NonNull;

use rle::{HasLength, MergableSpan, SplitableSpan};

use content_tree::*;
use rle::Searchable;
use crate::rev_span::TimeSpanRev;
use crate::list::merge::DocRangeIndex;
use crate::list::merge::markers::Marker::{DelTarget, InsPtr};
use crate::list::merge::yjsspan2::YjsSpan2;
use crate::list::operation::InsDelTag;

// TODO: Consider refactoring this to be a single enum. Put len in InsPtr and use .len(). But this
// might make the code way slower.

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Marker {
    /// For inserts, we store a pointer to the leaf node containing the inserted item. This is only
    /// used for inserts so we don't need to modify multiple entries when the inserted item is
    /// moved.
    InsPtr(NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>),

    /// For deletes we name the delete's target. Note this contains redundant information - since
    /// we already have a length field.
    DelTarget(TimeSpanRev),
}

/// So this struct is a little weird. Its designed this way so I can reuse content-tree for two
/// different structures:
///
/// - When we enable and disable inserts, we need a marker (index) into the b-tree node in the range
///   tree containing that entry. This lets us find things in O(log n) time, which improves
///   performance for large merges. (Though at a cost of extra bookkeeping overhead for small
///   merges).
/// - For deletes, we need to know the delete's target. Ie, which corresponding insert inserted the
///   item which was deleted by this edit.
///
/// The cleanest implementation of this would store a TimeSpan for the ID of this edit instead of
/// just storing a length field. And we'd use a variant of the content-tree which uses absolutely
/// positioned items like a normal b-tree with RLE. But I don't have an implementation of that. So
/// instead we end up with this slightly weird structure.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry {
    pub len: usize,
    pub inner: Marker,
}

impl Marker {
    pub(super) fn tag(&self) -> InsDelTag {
        match self {
            Marker::InsPtr(_) => InsDelTag::Ins,
            Marker::DelTarget(_) => InsDelTag::Del
        }
    }
}

impl HasLength for MarkerEntry {
    fn len(&self) -> usize {
        self.len as usize
    }
}

impl SplitableSpan for Marker {
    fn truncate(&mut self, at: usize) -> Self {
        match self {
            InsPtr(_) => *self,
            Marker::DelTarget(target) => DelTarget(target.truncate(at)),
        }
    }
}

impl SplitableSpan for MarkerEntry {
    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at;
        self.len = at;
        MarkerEntry {
            len: remainder_len,
            inner: self.inner.truncate(at),
        }
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = Self {
            len: at,
            inner: self.inner.truncate_keeping_right(at)
        };
        self.len -= at;
        left
    }
}

impl MergableSpan for Marker {
    fn can_append(&self, other: &Self) -> bool {
        match (self, other) {
            (InsPtr(ptr1), InsPtr(ptr2)) => {
                ptr1 == ptr2
            }
            (DelTarget(t1), DelTarget(t2)) => t1.can_append(t2),
            _ => false,
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (InsPtr(_), InsPtr(_)) => {},
            (DelTarget(t1), DelTarget(t2)) => t1.append(t2),
            _ => {
                panic!("Internal consistency error: Invalid append");
            },
        }
    }

    fn prepend(&mut self, other: Self) {
        match (self, other) {
            (InsPtr(_), InsPtr(_)) => {},
            (DelTarget(t1), DelTarget(t2)) => t1.prepend(t2),
            _ => {
                panic!("Internal consistency error: Invalid prepend");
            },
        }
    }
}

impl MergableSpan for MarkerEntry {
    fn can_append(&self, other: &Self) -> bool {
        self.inner.can_append(&other.inner)
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
        self.inner.append(other.inner);
    }

    fn prepend(&mut self, other: Self) {
        self.len += other.len;
        self.inner.prepend(other.inner);
    }
}

// impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> IndexGet<usize> for MarkerEntry<YjsSpan2, DocRangeIndex, IE, LE> {
//     type Output = NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, IE, LE>>;
//
//     fn index_get(&self, _index: usize) -> Self::Output {
//         self.ptr
//     }
// }



impl Default for MarkerEntry {
    fn default() -> Self {
        MarkerEntry {
            len: 0,
            inner: InsPtr(std::ptr::NonNull::dangling()),
        }
    }
}


// impl MarkerEntry {
//     pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>> {
//         if let InsPtr(ptr) = self.inner {
//             ptr
//         } else {
//             panic!("Internal consistency error: Cannot unwrap delete");
//         }
//     }
// }

impl Searchable for MarkerEntry {
    type Item = Option<NonNull<NodeLeaf<YjsSpan2, DocRangeIndex, DEFAULT_IE, DEFAULT_LE>>>;

    fn get_offset(&self, _loc: Self::Item) -> Option<usize> {
        panic!("Should never be used")
    }

    fn at_offset(&self, _offset: usize) -> Self::Item {
        if let InsPtr(ptr) = self.inner {
            Some(ptr)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use rle::test_splitable_methods_valid;
    use crate::list::merge::markers::Marker::{DelTarget, InsPtr};
    use crate::list::merge::markers::MarkerEntry;
    use crate::rev_span::TimeSpanRev;

    #[test]
    fn marker_split_merge() {
        test_splitable_methods_valid(MarkerEntry {
            len: 10,
            inner: InsPtr(NonNull::dangling())
        });

        test_splitable_methods_valid(MarkerEntry {
            len: 10,
            inner: DelTarget(TimeSpanRev {
                span: (0..10).into(),
                fwd: true,
            })
        });

        test_splitable_methods_valid(MarkerEntry {
            len: 10,
            inner: DelTarget(TimeSpanRev {
                span: (0..10).into(),
                fwd: false,
            })
        });
    }
}
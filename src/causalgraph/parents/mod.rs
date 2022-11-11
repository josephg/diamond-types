/// This stores the parents information, and contains a bunch of tools for interacting with the
/// parents information.

pub(crate) mod tools;
mod scope;
mod check;

use std::iter::once;
use smallvec::{SmallVec, smallvec};

use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::{Frontier, LV};

use crate::rle::{RleKeyed, RleVec};
use crate::dtrange::DTRange;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use crate::frontier::{clone_smallvec, local_frontier_is_root};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Parents {
    pub(crate) entries: RleVec<ParentsEntryInternal>,

    // The index of all items with ROOT as a direct parent.
    pub(crate) root_child_indexes: SmallVec<[usize; 2]>,
}

impl Parents {
    pub fn parents_at_time(&self, time: LV) -> Frontier {
        let entry = self.entries.find_packed(time);
        entry.with_parents(time, |p| p.into())
    }

    #[allow(unused)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(unused)]
    pub fn num_entries(&self) -> usize {
        self.entries.num_entries()
    }

    // This is mostly for testing.
    #[allow(unused)]
    pub(crate) fn from_entries(entries: &[ParentsEntryInternal]) -> Self {
        Parents {
            entries: RleVec(entries.to_vec()),
            root_child_indexes: entries.iter().enumerate().filter_map(|(i, entry)| {
                if entry.parents.is_root() { Some(i) } else { None }
            }).collect()
        }
    }

    #[allow(unused)]
    pub fn get_next_time(&self) -> usize {
        if let Some(last) = self.entries.last() {
            last.span.end
        } else { 0 }
    }

    /// Insert a new history entry for the specified range of versions, and the named parents.
    ///
    /// This method will try to extend the last entry if it can.
    pub(crate) fn push(&mut self, txn_parents: &[LV], range: DTRange) {
        // dbg!(txn_parents, range, &self.history.entries);
        // Fast path. The code below is weirdly slow, but most txns just append.
        if let Some(last) = self.entries.0.last_mut() {
            if txn_parents.len() == 1
                && txn_parents[0] == last.last_time()
                && last.span.can_append(&range)
            {
                last.span.append(range);
                return;
            }
        }

        // let parents = replace(&mut self.frontier, txn_parents);
        let mut shadow = range.start;
        while shadow >= 1 && txn_parents.contains(&(shadow - 1)) {
            shadow = self.entries.find(shadow - 1).unwrap().shadow;
        }

        let will_merge = if let Some(last) = self.entries.last() {
            // TODO: Is this shadow check necessary?
            // This code is from TxnSpan splitablespan impl. Copying it here is a bit ugly but
            // its the least ugly way I could think to implement this.
            txn_parents.len() == 1 && txn_parents[0] == last.last_time() && shadow == last.shadow
        } else { false };

        // let mut parent_indexes = smallvec![];
        if !will_merge {
            // The item wasn't merged. So we need to go through the parents and wire up children.
            let new_idx = self.entries.0.len();

            if txn_parents.is_empty() {
                self.root_child_indexes.push(new_idx);
            } else {
                for &p in txn_parents {
                    let parent_idx = self.entries.find_index(p).unwrap();
                    // Interestingly the parent_idx array will always end up the same length as parents
                    // because it would be invalid for multiple parents to point to the same entry in
                    // txns. (That would imply one parent is a descendant of another.)
                    // debug_assert!(!parent_indexes.contains(&parent_idx));
                    // parent_indexes.push(parent_idx);

                    let parent_children = &mut self.entries.0[parent_idx].child_indexes;
                    debug_assert!(!parent_children.contains(&new_idx));
                    parent_children.push(new_idx);
                }
            }
        }

        let txn = ParentsEntryInternal {
            span: range,
            shadow,
            parents: txn_parents.into(),
            // parent_indexes,
            child_indexes: smallvec![]
        };

        let did_merge = self.entries.push(txn);
        assert_eq!(will_merge, did_merge);
    }
}

/// This type stores metadata for a run of transactions created by the users.
///
/// Both individual inserts and deletes will use up txn numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParentsEntryInternal {
    pub span: DTRange, // TODO: Make the span u64s instead of usize.

    /// All txns in this span are direct descendants of all operations from span down to shadow.
    /// This is derived from other fields and used as an optimization for some calculations.
    pub shadow: usize,

    /// The parents vector of the first txn in this span. This vector will contain:
    /// - Nothing when the range has "root" as a parent. Usually this is just the case for the first
    ///   entry in history
    /// - One item when its a simple change
    /// - Two or more items when concurrent changes have happened, and the first item in this range
    ///   is a merge operation.
    pub parents: Frontier,

    /// This is a cached list of all the other indexes of items in history which name this item as
    /// a parent.
    pub child_indexes: SmallVec<[usize; 2]>,
}

impl ParentsEntryInternal {
    // pub fn parent_at_offset(&self, at: usize) -> Option<usize> {
    //     if at > 0 {
    //         Some(self.span.start + at - 1)
    //     } else { None } // look at .parents field.
    // }

    pub fn parent_at_time(&self, time: usize) -> Option<usize> {
        if time > self.span.start {
            Some(time - 1)
        } else { None } // look at .parents field.
    }

    pub fn with_parents<F: FnOnce(&[LV]) -> G, G>(&self, time: usize, f: F) -> G {
        if time > self.span.start {
            f(&[time - 1])
        } else {
            f(self.parents.as_ref())
        }
    }

    pub fn clone_parents_at_time(&self, time: usize) -> Frontier {
        if time > self.span.start {
            Frontier::new_1(time - 1)
        } else {
            self.parents.clone()
        }
    }

    pub fn next_child_after(&self, time: usize, parents: &Parents) -> Option<usize> {
        let span: DTRange = (time..self.span.end).into();

        self.child_indexes.iter()
            // First we want to join all of the childrens' parents
            .flat_map(|idx| parents.entries[*idx].parents.iter().copied())
            // But only include the ones which point within the specified range
            .filter(|p| span.contains(*p))
            // And we only care about the first one!
            .min()
    }

    pub fn split_point(&self, time: usize, parents: &Parents) -> usize {
        match self.next_child_after(time, parents) {
            Some(t) => t + 1,
            None => self.span.end,
        }
    }

    // pub fn local_children_at_time(&self, time: usize) ->

    pub fn contains(&self, localtime: usize) -> bool {
        self.span.contains(localtime)
    }

    pub fn last_time(&self) -> usize {
        self.span.last()
    }

    pub fn shadow_contains(&self, time: usize) -> bool {
        debug_assert!(time <= self.last_time());
        time >= self.shadow
    }

    // pub fn parents_at_offset(&self, at: usize) -> SmallVec<[Order; 2]> {
    //     if at > 0 {
    //         smallvec![self.order + at as u32 - 1]
    //     } else {
    //         // I don't like this clone here, but it'll be pretty rare anyway.
    //         self.parents.clone()
    //     }
    // }
}

impl HasLength for ParentsEntryInternal {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl MergableSpan for ParentsEntryInternal {
    fn can_append(&self, other: &Self) -> bool {
        self.span.can_append(&other.span)
            && other.parents.len() == 1
            && other.parents[0] == self.last_time()
            && other.shadow == self.shadow
    }

    fn append(&mut self, other: Self) {
        debug_assert!(other.child_indexes.is_empty());
        self.span.append(other.span);
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(self.child_indexes.is_empty());
        self.span.prepend(other.span);
        self.parents = other.parents;
        debug_assert_eq!(self.shadow, other.shadow);
    }
}

impl RleKeyed for ParentsEntryInternal {
    fn rle_key(&self) -> usize {
        self.span.start
    }
}

/// This is a simplified history entry for exporting and viewing externally.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParentsEntrySimple {
    pub span: DTRange,
    pub parents: Frontier,
}

impl MergableSpan for ParentsEntrySimple {
    fn can_append(&self, other: &Self) -> bool {
        self.span.can_append(&other.span)
            && other.parents.len() == 1
            && other.parents[0] == self.span.last()
    }

    fn append(&mut self, other: Self) {
        self.span.append(other.span);
    }

    fn prepend(&mut self, other: Self) {
        self.span.prepend(other.span);
        self.parents = other.parents;
    }
}

impl HasLength for ParentsEntrySimple {
    fn len(&self) -> usize { self.span.len() }
}

impl SplitableSpanHelpers for ParentsEntrySimple {
    fn truncate_h(&mut self, at: usize) -> Self {
        debug_assert!(at >= 1);

        ParentsEntrySimple {
            span: self.span.truncate(at),
            parents: Frontier::new_1(self.span.start + at - 1)
        }
    }
}

impl From<ParentsEntryInternal> for ParentsEntrySimple {
    fn from(entry: ParentsEntryInternal) -> Self {
        Self {
            span: entry.span,
            parents: entry.parents
        }
    }
}

impl From<&ParentsEntryInternal> for ParentsEntrySimple {
    fn from(entry: &ParentsEntryInternal) -> Self {
        Self {
            span: entry.span,
            parents: entry.parents.clone()
        }
    }
}

// This code works, but its much more complex than just using .iter() in the entries list.

// pub(crate) struct ParentsIter<'a> {
//     history: &'a Parents,
//     idx: usize,
//     offset: usize,
//     end: usize,
// }
//
// impl<'a> Iterator for ParentsIter<'a> {
//     type Item = ParentsEntrySimple;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         // If we hit the end of the list this will be None and return.
//         let e = self.history.entries.0.get(self.idx)?;
//
//         if self.end <= e.span.start { return None; } // End of the requested range.
//
//         self.idx += 1;
//
//         let mut m = ParentsEntrySimple::from(e);
//
//         if self.offset > 0 {
//             m.truncate_keeping_right(self.offset);
//             self.offset = 0;
//         }
//
//         if m.span.end > self.end {
//             m.truncate(self.end - m.span.start);
//         }
//
//         Some(m)
//     }
// }
//
// impl Parents {
//     pub(crate) fn iter_range(&self, range: DTRange) -> ParentsIter<'_> {
//         let idx = self.entries.find_index(range.start).unwrap();
//         let offset = range.start - self.entries.0[idx].rle_key();
//
//         ParentsIter {
//             history: self,
//             idx,
//             offset,
//             end: range.end
//         }
//     }
//
//     pub(crate) fn iter(&self) -> ParentsIter<'_> {
//         ParentsIter {
//             history: self,
//             idx: 0,
//             offset: 0,
//             end: self.get_next_time()
//         }
//     }
// }
impl Parents {
    pub(crate) fn iter_range(&self, range: DTRange) -> impl Iterator<Item = ParentsEntrySimple> + '_ {
        self.entries.iter_range_map(range, |e| e.into())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ParentsEntrySimple> + '_ {
        self.entries.iter().map(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use rle::{MergableSpan, test_splitable_methods_valid};
    use crate::causalgraph::parents::{Parents, ParentsEntrySimple};
    use crate::encoding::ChunkType::CausalGraph;
    use crate::Frontier;
    use super::ParentsEntryInternal;

    #[test]
    fn test_iter_empty() {
        let parents = Parents::new();
        let entries_a = parents.iter().collect::<Vec<_>>();
        let entries_b = parents.iter_range((0..0).into()).collect::<Vec<_>>();
        assert!(entries_a.is_empty());
        assert!(entries_b.is_empty());
    }

    #[test]
    fn test_txn_appends() {
        let mut txn_a = ParentsEntryInternal {
            span: (1000..1010).into(), shadow: 500,
            parents: Frontier::new_1(999),
            child_indexes: smallvec![],
        };
        let txn_b = ParentsEntryInternal {
            span: (1010..1015).into(), shadow: 500,
            parents: Frontier::new_1(1009),
            child_indexes: smallvec![],
        };

        assert!(txn_a.can_append(&txn_b));

        txn_a.append(txn_b);
        assert_eq!(txn_a, ParentsEntryInternal {
            span: (1000..1015).into(), shadow: 500,
            parents: Frontier::new_1(999),
            child_indexes: smallvec![],
        })
    }

    #[test]
    fn txn_entry_valid() {
        test_splitable_methods_valid(ParentsEntrySimple {
            span: (10..20).into(),
            parents: Frontier::new_1(0),
        });
    }

    #[test]
    fn iterator_regression() {
        // There was a bug where this caused a crash.
        let mut parents = Parents::new();
        parents.push(&[], (0..1).into());
        parents.push(&[], (1..2).into());

        for r in parents.iter_range((0..1).into()) {
            // dbg!(&r);
            drop(r);
        }
    }
}
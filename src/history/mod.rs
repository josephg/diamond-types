/// TODO: History is the wrong name here.
///
/// This stores the parents information, and contains a bunch of tools for interacting with the
/// parents information.

pub(crate) mod tools;
mod scope;

use smallvec::{SmallVec, smallvec};

use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::{LocalVersion, Time};

use crate::rle::{RleKeyed, RleVec};
use crate::dtrange::DTRange;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use crate::frontier::{clone_smallvec, local_version_is_root};

#[derive(Debug, Clone)]
pub(crate) struct ScopedHistory {
    pub(crate) created_at: Time,

    /// This isn't a real Version. Its a list of times at which this CRDT was deleted.
    ///
    /// (What do we need this for??)
    pub(crate) deleted_at: LocalVersion,

    pub(crate) owned_times: RleVec<DTRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct History {
    pub(crate) entries: RleVec<HistoryEntry>,

    // The index of all items with ROOT as a direct parent.
    pub(crate) root_child_indexes: SmallVec<[usize; 2]>,
}

impl History {
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
    pub(crate) fn from_entries(entries: &[HistoryEntry]) -> Self {
        History {
            entries: RleVec(entries.to_vec()),
            root_child_indexes: entries.iter().enumerate().filter_map(|(i, entry)| {
                if local_version_is_root(&entry.parents) { Some(i) } else { None }
            }).collect()
        }
    }

    #[allow(unused)]
    pub fn get_next_time(&self) -> usize {
        if let Some(last) = self.entries.last() {
            last.span.end
        } else { 0 }
    }

    /// Insert a new history entry for the specified range of vesrsions, and the named parents.
    ///
    /// This method will try to extend the last entry if it can.
    pub(crate) fn insert(&mut self, txn_parents: &[Time], range: DTRange) {
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
        // TODO: Consider not doing this, and just having 0 mean "start of recorded time" here.
        if shadow == 0 { shadow = usize::MAX; }

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

        let txn = HistoryEntry {
            span: range,
            shadow,
            parents: txn_parents.iter().copied().collect(),
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
/// TODO: Consider renaming this to HistoryEntryInternal or something.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HistoryEntry {
    pub span: DTRange, // TODO: Make the span u64s instead of usize.

    /// All txns in this span are direct descendants of all operations from order down to shadow.
    /// This is derived from other fields and used as an optimization for some calculations.
    pub shadow: usize,

    /// The parents vector of the first txn in this span. This vector will contain:
    /// - Nothing when the range has "root" as a parent. Usually this is just the case for the first
    ///   entry in history
    /// - One item when its a simple change
    /// - Two or more items when concurrent changes have happened, and the first item in this range
    ///   is a merge operation.
    pub parents: SmallVec<[usize; 2]>,

    pub child_indexes: SmallVec<[usize; 2]>,
}

impl HistoryEntry {
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

    pub fn with_parents<F: FnOnce(&[Time]) -> G, G>(&self, time: usize, f: F) -> G {
        if time > self.span.start {
            f(&[time - 1])
        } else {
            f(self.parents.as_slice())
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
        // TODO: Is there a difference between a shadow of 0 and a shadow of usize::MAX?
        self.shadow == usize::MAX || time >= self.shadow
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

impl HasLength for HistoryEntry {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl MergableSpan for HistoryEntry {
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

impl RleKeyed for HistoryEntry {
    fn rle_key(&self) -> usize {
        self.span.start
    }
}

/// This is a simplified history entry for exporting and viewing externally.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MinimalHistoryEntry {
    pub span: DTRange,
    pub parents: SmallVec<[usize; 2]>,
}

impl MergableSpan for MinimalHistoryEntry {
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

impl HasLength for MinimalHistoryEntry {
    fn len(&self) -> usize { self.span.len() }
}

impl SplitableSpanHelpers for MinimalHistoryEntry {
    fn truncate_h(&mut self, at: usize) -> Self {
        debug_assert!(at >= 1);

        MinimalHistoryEntry {
            span: self.span.truncate(at),
            parents: smallvec![self.span.start + at - 1]
        }
    }
}

impl From<HistoryEntry> for MinimalHistoryEntry {
    fn from(entry: HistoryEntry) -> Self {
        Self {
            span: entry.span,
            parents: entry.parents
        }
    }
}

impl From<&HistoryEntry> for MinimalHistoryEntry {
    fn from(entry: &HistoryEntry) -> Self {
        Self {
            span: entry.span,
            parents: clone_smallvec(&entry.parents)
        }
    }
}

pub(crate) struct HistoryIter<'a> {
    history: &'a History,
    idx: usize,
    offset: usize,
    end: usize,
}

impl<'a> Iterator for HistoryIter<'a> {
    type Item = MinimalHistoryEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let e = if let Some(e) = self.history.entries.0.get(self.idx) { e }
        else { return None; };

        self.idx += 1;

        let mut m = MinimalHistoryEntry::from(e);

        if self.offset > 0 {
            m.truncate_keeping_right(self.offset);
            self.offset = 0;
        }

        if m.span.end > self.end {
            m.truncate(self.end - m.span.start);
        }

        Some(m)
    }
}

impl History {
    pub(crate) fn iter_range(&self, range: DTRange) -> HistoryIter<'_> {
        let idx = self.entries.find_index(range.start).unwrap();
        let offset = range.start - self.entries.0[idx].rle_key();

        HistoryIter {
            history: self,
            idx,
            offset,
            end: range.end
        }
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use rle::{MergableSpan, test_splitable_methods_valid};
    use crate::history::MinimalHistoryEntry;
    use super::HistoryEntry;

    #[test]
    fn test_txn_appends() {
        let mut txn_a = HistoryEntry {
            span: (1000..1010).into(), shadow: 500,
            parents: smallvec![999],
            child_indexes: smallvec![],
        };
        let txn_b = HistoryEntry {
            span: (1010..1015).into(), shadow: 500,
            parents: smallvec![1009],
            child_indexes: smallvec![],
        };

        assert!(txn_a.can_append(&txn_b));

        txn_a.append(txn_b);
        assert_eq!(txn_a, HistoryEntry {
            span: (1000..1015).into(), shadow: 500,
            parents: smallvec![999],
            child_indexes: smallvec![],
        })
    }

    #[test]
    fn txn_entry_valid() {
        test_splitable_methods_valid(MinimalHistoryEntry {
            span: (10..20).into(),
            parents: smallvec![0]
        });
    }
}
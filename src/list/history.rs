use smallvec::{SmallVec, smallvec};

use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use crate::list::{clone_smallvec, Time};

use crate::rle::{RleKeyed, RleVec};
use crate::dtrange::DTRange;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use crate::list::frontier::local_version_is_root;


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

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use rle::{MergableSpan, test_splitable_methods_valid};
    use crate::list::history::MinimalHistoryEntry;
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
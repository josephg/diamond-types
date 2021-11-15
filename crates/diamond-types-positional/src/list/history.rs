use smallvec::SmallVec;

use rle::{HasLength, MergableSpan};
use crate::list::Time;

use crate::rle::{RleKeyed, RleVec};
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct History {
    pub(crate) entries: RleVec<HistoryEntry>,

    // The index of all items with ROOT as a direct parent.
    pub(crate) root_child_indexes: SmallVec<[usize; 2]>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    // This is mostly for testing.
    pub fn from_entries(entries: &[HistoryEntry]) -> Self {
        History {
            entries: RleVec(entries.to_vec()),
            root_child_indexes: entries.iter().enumerate().filter_map(|(i, entry)| {
                if entry.parents.len() == 1 && entry.parents[0] == ROOT_TIME {
                    Some(i)
                } else { None }
            }).collect()
        }
    }

    pub fn get_next_time(&self) -> usize {
        if let Some(last) = self.entries.last() {
            last.span.end
        } else { 0 }
    }
}

/// This type stores metadata for a run of transactions created by the users.
///
/// Both individual inserts and deletes will use up txn numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct HistoryEntry {
    pub span: TimeSpan, // TODO: Make the span u64s instead of usize.

    /// All txns in this span are direct descendants of all operations from order down to shadow.
    /// This is derived from other fields and used as an optimization for some calculations.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub shadow: usize,

    /// The parents vector of the first txn in this span. Must contain at least 1 entry (and will
    /// almost always contain exactly 1 entry - the only exception being in the case of concurrent
    /// changes).
    pub parents: SmallVec<[usize; 2]>,

    /// This is a list of the index of other txns which have a parent within this transaction.
    /// TODO: Consider constraining this to not include the next child. Complexity vs memory.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub parent_indexes: SmallVec<[usize; 2]>,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub child_indexes: SmallVec<[usize; 2]>,
}

impl HistoryEntry {
    pub fn parent_at_offset(&self, at: usize) -> Option<usize> {
        if at > 0 {
            Some(self.span.start + at - 1)
        } else { None } // look at .parents field.
    }

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
        self.shadow == ROOT_TIME || time >= self.shadow
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
    // fn truncate(&mut self, _at: usize) -> Self {
    //     unimplemented!("TxnSpan cannot be truncated");
    //     // debug_assert!(at >= 1);
    //     // let at = at as u32;
    //     // let other = Self {
    //     //     order: self.order + at,
    //     //     len: self.len - at,
    //     //     shadow: self.shadow,
    //     //     parents: smallvec![self.order + at - 1],
    //     // };
    //     // self.len = at as u32;
    //     // other
    // }

impl MergableSpan for HistoryEntry {
    fn can_append(&self, other: &Self) -> bool {
        other.parents.len() == 1
            && other.parents[0] == self.last_time()
            && other.shadow == self.shadow
    }

    fn append(&mut self, other: Self) {
        debug_assert!(other.parent_indexes.is_empty());
        self.span.append(other.span);
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(self.parent_indexes.is_empty());
        self.span.prepend(other.span);
        self.parents = other.parents;
        debug_assert_eq!(self.shadow, other.shadow);
    }
}

impl RleKeyed for HistoryEntry {
    fn get_rle_key(&self) -> usize {
        self.span.start
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use rle::MergableSpan;
    use super::HistoryEntry;

    // #[test]
    // fn txn_entry_valid() {
    //     test_splitable_methods_valid(TxnSpan {
    //         order: 1000,
    //         len: 5,
    //         shadow: 999,
    //         parents: smallvec![999]
    //     });
    // }

    #[test]
    fn test_txn_appends() {
        let mut txn_a = HistoryEntry {
            span: (1000..1010).into(), shadow: 500,
            parents: smallvec![999],
            parent_indexes: smallvec![], child_indexes: smallvec![],
        };
        let txn_b = HistoryEntry {
            span: (1010..1015).into(), shadow: 500,
            parents: smallvec![1009],
            parent_indexes: smallvec![], child_indexes: smallvec![],
        };

        assert!(txn_a.can_append(&txn_b));

        txn_a.append(txn_b);
        assert_eq!(txn_a, HistoryEntry {
            span: (1000..1015).into(), shadow: 500,
            parents: smallvec![999],
            parent_indexes: smallvec![], child_indexes: smallvec![],
        })
    }
}
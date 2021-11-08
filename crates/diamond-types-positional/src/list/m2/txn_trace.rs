
use bitvec::prelude::*;
use smallvec::{SmallVec, smallvec};
use crate::rle::RleVec;
use crate::list::branch::{retreat_branch_by, advance_branch_by_known, advance_branch_by};
use std::ops::Range;
use crate::list::{Branch, Time};
use crate::list::history::History;
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;

// So essentially what I'm doing here is a depth first iteration of the time DAG. The trouble is
// that we only want to visit each item exactly once, and we want to minimize the aggregate cost of
// pointlessly traversing up and down the tree.
//
// This is closely related to Edmond's algorithm for finding the minimal spanning arborescence:
// https://en.wikipedia.org/wiki/Edmonds%27_algorithm
//
// The traversal must also obey the ordering rule for time DAGs. No item can be visited before all
// of its parents have been visited.
//
// The code was manually unrolled into an iterator so we could walk it without needing to collect
// this structure to a vec or something.
#[derive(Debug)]
pub(crate) struct OptimizedTxnsIter<'a> {
    // I could hold a slice reference here instead, but it'd be missing the find() methods.
    history: &'a History,

    branch: Branch,
    consumed: BitBox, // Could use markers on txns for this instead?

    // TODO: Might be better to make stack store tuples of (usize, usize) for child index.
    stack: Vec<usize>, // smallvec? This will have an upper bound of the number of txns.

    num_consumed: usize, // For debugging.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxnWalkItem {
    pub(crate) retreat: SmallVec<[TimeSpan; 4]>,
    pub(crate) advance_rev: SmallVec<[TimeSpan; 4]>,
    // txn: &'a TxnSpan,
    pub(crate) parents: SmallVec<[Time; 2]>,
    pub(crate) consume: TimeSpan,
}

impl<'a> OptimizedTxnsIter<'a> {
    pub(crate) fn new(history: &'a History) -> Self {
        Self {
            history,
            // TODO: Refactor to start with branch and stack empty.
            branch: smallvec![ROOT_TIME],
            consumed: bitbox![0; history.entries.len()],
            stack: vec![ROOT_TIME],
            num_consumed: 0,
        }
    }

    // #[inline]
    fn get_next_child(&self, child_idxs: &[usize]) -> Option<usize> {
        // TODO: We actually want to iterate through the children in order from whatever requires us
        // to backtrack the least, to whatever requires the most backtracking. I think iterating in
        // reverse order gets us part way there, but without actual multi user benchmarking data its
        // hard to tell for sure if this helps.
        // for &i in child_idxs.iter().rev() {
        for &i in child_idxs.iter() {
            // println!("  - {}", i);
            if self.consumed[i] { continue; }

            let next = &self.history.entries[i];
            // We're looking for a child where all the parents have been satisfied
            if next.parents.len() == 1 || next.parents.iter().all(|&p| {
                // TODO: Speed this up by caching the index of each parent in each txn
                self.consumed[self.history.entries.find_index(p).unwrap()]
            }) {
                return Some(i);
            }
        }
        None
    }

    fn get_next_child_idx(&self, idx: usize) -> Option<usize> {
        self.get_next_child(if idx == usize::MAX {
            &self.history.root_child_indexes
        } else {
            &self.history.entries[idx].child_indexes
        })
    }
}

impl<'a> Iterator for OptimizedTxnsIter<'a> {
    type Item = TxnWalkItem;

    fn next(&mut self) -> Option<Self::Item> {
        // Find the next item to consume. We'll start with all the children of the top of the
        // stack, and greedily walk up looking for anything which has all its dependencies
        // satisfied.
        let next_idx = loop {
            // A previous implementation handled both of these cases the same way, but it needed a
            // dummy value at the start of self.stack, which caused an allocation when the doc only
            // has one txn span. This approach is a bit more complex, but leaves the stack empty in
            // this case.
            if let Some(&idx) = self.stack.last() {
                // println!("stack top {}!", idx);
                if let Some(next_idx) = self.get_next_child_idx(idx) {
                    break next_idx;
                }

                // TODO: We could retreat branch here. That would have the benefit of not needing as
                // many lookups through txns, but the downside is we'd end up retreating all the way
                // back to the start of the document at the end of the process. Benchmark to see
                // which way is better.

                // println!("pop {}!", idx);
                self.stack.pop();
            } else {
                // The stack was exhausted and we didn't find anything. We're done here.
                debug_assert!(self.consumed.all());
                debug_assert_eq!(self.num_consumed, self.history.entries.len());
                return None;
            }
        };

        assert!(next_idx < self.history.entries.len());
        assert!(!self.consumed[next_idx]);

        let next_txn = &self.history.entries[next_idx];

        let (only_branch, only_txn) = self.history.diff(&self.branch, &next_txn.parents);
        // dbg!((&branch, &next_txn.parents, &only_branch, &only_txn));
        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch by {:?}", span);
            retreat_branch_by(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }
        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", span);
            advance_branch_by(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }

        // println!("consume {} (order {:?})", next_idx, next_txn.as_span());
        advance_branch_by_known(&mut self.branch, &next_txn.parents, next_txn.span);
        // dbg!(&branch);
        self.consumed.set(next_idx, true);
        self.num_consumed += 1;
        self.stack.push(next_idx);

        return Some(TxnWalkItem {
            retreat: only_branch,
            advance_rev: only_txn,
            parents: next_txn.parents.clone(),
            consume: next_txn.span
        });
    }
}

impl History {
    /// This function is for efficiently finding the order we should traverse the time DAG in order to
    /// walk all the changes so we can efficiently save everything to disk. This is needed because if
    /// we simply traverse the txns in the order they're in right now, we can have pathological
    /// behaviour in the presence of multiple interleaved branches. (Eg if you're streaming from two
    /// peers concurrently editing different branches).
    pub(crate) fn txn_spanning_tree_iter(&self) -> OptimizedTxnsIter {
        OptimizedTxnsIter::new(self)
    }
}


#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::list::history::HistoryEntry;
    use super::*;

    #[test]
    fn iter_span_for_empty_doc() {
        let history = History::new();
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        assert!(walk.is_empty());
    }

    #[test]
    fn iter_span_from_root() {
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(), shadow: 0,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (10..30).into(), shadow: 0,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            }
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (10..30).into(),
            },
        ]);
    }

    #[test]
    fn fork_and_join() {
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(), shadow: 0,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            HistoryEntry {
                span: (10..30).into(), shadow: 10,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            HistoryEntry {
                span: (30..50).into(), shadow: 0,
                parents: smallvec![9, 29],
                parent_indexes: smallvec![0, 1], child_indexes: smallvec![]
            },
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (10..30).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(0..10).into()],
                parents: smallvec![9, 29],
                consume: (30..50).into(),
            },
        ]);

        // dbg!(walk);
    }

    #[test]
    fn two_chains() {
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..1).into(), shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            HistoryEntry {
                span: (1..2).into(), shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![], child_indexes: smallvec![3]
            },
            HistoryEntry {
                span: (2..3).into(), shadow: 2,
                parents: smallvec![0],
                parent_indexes: smallvec![0], child_indexes: smallvec![4]
            },
            HistoryEntry {
                span: (3..4).into(), shadow: 3,
                parents: smallvec![1],
                parent_indexes: smallvec![1], child_indexes: smallvec![4]
            },
            HistoryEntry {
                span: (4..5).into(), shadow: ROOT_TIME,
                parents: smallvec![2, 3],
                parent_indexes: smallvec![2, 3], child_indexes: smallvec![]
            },
        ]);

        // history.traverse_txn_spanning_tree();
        let iter = OptimizedTxnsIter::new(&history);
        // for item in iter {
        //     dbg!(item);
        // }

        assert!(iter.eq(std::array::IntoIter::new([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (0..1).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![0],
                consume: (2..3).into(),
            },

            TxnWalkItem {
                retreat: smallvec![(2..3).into(), (0..1).into()],
                advance_rev: smallvec![],
                parents: smallvec![ROOT_TIME],
                consume: (1..2).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![1],
                consume: (3..4).into(),
            },

            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(2..3).into(), (0..1).into()],
                parents: smallvec![2, 3],
                consume: (4..5).into(),
            },
        ])));
    }
}
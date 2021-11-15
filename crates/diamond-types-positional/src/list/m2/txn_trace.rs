
use smallvec::{SmallVec, smallvec};
use crate::rle::RleVec;
use crate::list::branch::{retreat_branch_by, advance_branch_by_known, advance_branch_by, branch_is_sorted};
use std::ops::Range;
use rle::{HasLength, SplitableSpan};
use crate::list::{Branch, Time};
use crate::list::history::{History, HistoryEntry};
use crate::list::history_tools::{ConflictSpans, DiffResult};
use crate::localtime::TimeSpan;
use crate::ROOT_TIME;

#[derive(Debug)]
// struct VisitEntry<'a> {
struct VisitEntry {
    span: TimeSpan,
    txn_idx: usize,
    // entry: &'a HistoryEntry,
    visited: bool,
    parent_idxs: SmallVec<[usize; 4]>,
}


fn find_entry_idx(input: &SmallVec<[VisitEntry; 4]>, time: Time) -> Option<usize> {
    input.as_slice().binary_search_by(|e| {
        // Is this the right way around?
        e.span.partial_cmp_time(time).reverse()
    }).ok()
}

fn spans_to_entries(history: &History, spans: &[TimeSpan]) -> SmallVec<[VisitEntry; 4]> {
    // This method could be removed - its sort of unnecessary. The find_conflicting process
    // scans txns to find the range we need to work over. It does so in reverse order. But we
    // need the txns themselves.
    //
    // It would be faster to just return the list of txns, though thats kinda uglier from an API
    // standpoint.
    //
    // ... Something to consider.

    let mut result = smallvec![];

    // for mut span_remaining in spans.iter().copied().filter(|s| !s.is_empty()) {
    for mut span_remaining in spans.iter().copied() {
        debug_assert!(!span_remaining.is_empty());

        let mut i = history.entries.find_index(span_remaining.start).unwrap();
        // let mut offset = history.entries[i].
        while !span_remaining.is_empty() {
            let e = &history.entries[i];
            debug_assert!(span_remaining.start >= e.span.start);

            let offset = Time::min(span_remaining.len(), e.span.end - span_remaining.start);
            let span = span_remaining.truncate_keeping_right(offset);

            // We don't care about any parents outside of the input spans.
            let parent_idxs = e.parents.iter()
                .filter(|t| **t != ROOT_TIME)
                .map(|t| find_entry_idx(&result, *t))
                .flatten()
                .collect();

            result.push(VisitEntry {
                span,
                txn_idx: i,
                // entry: e,
                visited: false,
                parent_idxs
            });
            i += 1;
        }
    }
    result
}

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

    // TODO: Remove this. Use markers on txns or something instead.
    // consumed: BitBox,

    input: SmallVec<[VisitEntry; 4]>,
    // input: SmallVec<[VisitEntry<'a>; 4]>,
    // visit_spans: SmallVec<[TimeSpan; 4]>,

    /// List of input_idx
    to_process: SmallVec<[usize; 4]>, // smallvec? This will have an upper bound of the number of txns.

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
    pub(crate) fn new_all(history: &'a History) -> Self {
        let mut spans = smallvec![];
        if history.get_next_time() > 0 {
            // Kinda gross. Only add the span if the document isn't empty.
            spans.push((0..history.get_next_time()).into());
        }

        Self::new(history, ConflictSpans {
            common_branch: smallvec![ROOT_TIME],
            spans
        }, None)
    }

    /// If starting_branch is not specified, the iterator starts at conflict.common_branch.
    pub(crate) fn new(history: &'a History, conflict: ConflictSpans, starting_branch: Option<Branch>) -> Self {
        debug_assert!(branch_is_sorted(&conflict.common_branch));

        let input = spans_to_entries(history, &conflict.spans);
        let mut to_process = smallvec![];

        for time in &conflict.common_branch {
            // result.push_children(*time);

            let txn_indexes = if *time == ROOT_TIME {
                &history.root_child_indexes
            } else {
                let txn = history.entries.find_packed(*time);

                if *time < txn.span.last() {
                    // Add this txn itself.
                    if let Some(i) = find_entry_idx(&input, *time + 1) {
                        to_process.push(i);
                    }
                }

                &txn.child_indexes
            }.as_slice();

            // This is gross. This code is duplicated in push_children (below), but I can't easily
            // call push_children because it would violate the borrow checker.
            for idx in txn_indexes.iter().rev() {
                let child_span_start = history.entries[*idx].span.start;
                if let Some(i) = find_entry_idx(&input, child_span_start) {
                    to_process.push(i);
                }
            }
        }

        let branch = starting_branch.unwrap_or(conflict.common_branch);

        let mut result = Self {
            history,
            branch,
            input,
            to_process,
            num_consumed: 0
        };

        result
    }

    fn push_children(&mut self, child_txn_idxs: &[usize]) {
        // self.to_process.extend(... ?)
        // TODO: Consider removing .rev() here. I think its faster without it.
        for idx in child_txn_idxs.iter().rev() {
            let txn = &self.history.entries[*idx];
            if let Some(i) = find_entry_idx(&self.input, txn.span.start) {
                self.to_process.push(i);
            }
        }
    }

    pub fn into_branch(self) -> Branch {
        self.branch
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
            if let Some(idx) = self.to_process.pop() {
                let e = &self.input[idx];
                // Visit this entry if it hasn't been visited but all of its parents have been
                // visited. Note if our parents haven't been visited yet, we'll throw it out here.
                // But thats ok, because we should always visit it via another path in that case.
                if !e.visited && e.parent_idxs.iter().all(|i| self.input[*i].visited) {
                    break idx;
                }
            } else {
                // The stack was exhausted and we didn't find anything. We're done here.
                // dbg!(&self);
                debug_assert!(self.input.iter().all(|e| e.visited));
                debug_assert_eq!(self.num_consumed, self.input.len());
                return None;
            }
        };

        let input_entry = &mut self.input[next_idx];
        let next_txn = &self.history.entries[input_entry.txn_idx];

        // let (only_branch, only_txn) = if let Some(p) = next_txn.parent_at_time(input_entry.span.start) {
        //     self.history.diff(&self.branch, &[p])
        // } else {
        //     self.history.diff(&self.branch, &next_txn.parents)
        // };

        let parents = if let Some(p) = next_txn.parent_at_time(input_entry.span.start) {
            smallvec![p]
        } else {
            next_txn.parents.clone()
        };

        // dbg!(&self.branch, &next_txn.parents);
        let DiffResult {
            only_a: only_branch,
            only_b: only_txn,
            ..
        } = self.history.diff(&self.branch, &parents);
        // let (only_branch, only_txn) = self.history.diff(&self.branch, &next_txn.parents);
        // dbg!((&branch, &next_txn.parents, &only_branch, &only_txn));
        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch by {:?}", range);
            retreat_branch_by(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }
        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", range);
            advance_branch_by(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }

        // println!("consume {} (order {:?})", next_idx, next_txn.as_span());
        let input_span = input_entry.span;
        // advance_branch_by_known(&mut self.branch, &next_txn.parents, input_span);
        advance_branch_by_known(&mut self.branch, &parents, input_span);
        // dbg!(&branch);
        // self.consumed.set(next_idx, true);
        input_entry.visited = true;
        self.num_consumed += 1;
        // self.to_process.push(next_idx);
        self.push_children(next_txn.child_indexes.as_slice());

        return Some(TxnWalkItem {
            retreat: only_branch,
            advance_rev: only_txn,
            parents,
            consume: input_span,
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
        OptimizedTxnsIter::new_all(self)
    }

    pub(crate) fn conflicting_txns_iter(&self, a: &[Time], b: &[Time]) -> OptimizedTxnsIter {
        let conflict = self.find_conflicting(a, b);
        // dbg!(&conflict);
        OptimizedTxnsIter::new(self, conflict, None)
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
        // dbg!(&walk);

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
        let iter = OptimizedTxnsIter::new_all(&history);
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

    #[test]
    fn iter_txn_middle() {
        // regression
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(),
                shadow: ROOT_TIME,
                parents: smallvec![ROOT_TIME],
                parent_indexes: smallvec![],
                child_indexes: smallvec![]
            },
        ]);

        let conflict = history.find_conflicting(&[5], &[6]);
        assert_eq!(conflict, ConflictSpans {
            common_branch: smallvec![5],
            spans: smallvec![(6..7).into()],
        });
        let iter = OptimizedTxnsIter::new(&history, conflict, None);
        // dbg!(&iter);

        assert!(iter.eq(std::array::IntoIter::new([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![5],
                consume: (6..7).into(),
            }
        ])));
    }
}
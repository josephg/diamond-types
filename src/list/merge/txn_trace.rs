/// This is a helper library for iterating through the time DAG (oplog) in a depth-first order.
/// This is better than naive local-time order because if operations are structured like this:
///
/// ```text
///   1
///  / \
/// 2  |
/// |  3
/// 4  |
/// |  5
/// ...
/// ```
///
/// Then traversal (and hence, merge) time complexity will be linear instead of quadratic.

use smallvec::{SmallVec, smallvec};
use crate::list::frontier::*;
use rle::{HasLength, SplitableSpan};
use crate::list::{LocalVersion, clone_smallvec, Time};
use crate::list::history::History;
use crate::dtrange::DTRange;

#[derive(Debug)]
// struct VisitEntry<'a> {
struct VisitEntry {
    span: DTRange,
    txn_idx: usize,
    // entry: &'a HistoryEntry,
    visited: bool,
    parent_idxs: SmallVec<[usize; 4]>,
}


fn find_entry_idx(input: &SmallVec<[VisitEntry; 4]>, time: Time) -> Option<usize> {
    input.as_slice().binary_search_by(|e| {
        // Is this the right way around?
        e.span.partial_cmp_time(time).reverse()
        // e.span.partial_cmp_time(time)
    }).ok()
}

fn check_rev_sorted(spans: &[DTRange]) {
    let mut last_end = None;
    for s in spans.iter().rev() {
        if let Some(end) = last_end {
            assert!(s.start >= end);
        }
        last_end = Some(s.end);
    }
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
pub(crate) struct SpanningTreeWalker<'a> {
    // I could hold a slice reference here instead, but it'd be missing the find() methods.
    history: &'a History,

    frontier: LocalVersion,

    input: SmallVec<[VisitEntry; 4]>,

    /// List of input_idx.
    ///
    /// This is sort of like a call stack of txns we push and pop from as we traverse
    to_process: SmallVec<[usize; 4]>, // smallvec? This will have an upper bound of the number of txns.

    num_consumed: usize, // For debugging.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxnWalkItem {
    pub(crate) retreat: SmallVec<[DTRange; 4]>,
    pub(crate) advance_rev: SmallVec<[DTRange; 4]>,
    // txn: &'a TxnSpan,
    pub(crate) parents: SmallVec<[Time; 2]>,
    pub(crate) consume: DTRange,
}

impl<'a> SpanningTreeWalker<'a> {
    #[allow(unused)]
    pub(crate) fn new_all(history: &'a History) -> Self {
        let mut spans: SmallVec<[DTRange; 4]> = smallvec![];
        if history.get_next_time() > 0 {
            // Kinda gross. Only add the span if the document isn't empty.
            spans.push((0..history.get_next_time()).into());
        }
        spans.reverse();

        Self::new(history, &spans, smallvec![])
    }

    pub(crate) fn new(history: &'a History, rev_spans: &[DTRange], start_at: LocalVersion) -> Self {
        if cfg!(debug_assertions) {
            check_rev_sorted(rev_spans);
        }

        let mut input = smallvec![];
        let mut to_process = smallvec![];

        for mut span_remaining in rev_spans.iter().rev().copied() {
            debug_assert!(!span_remaining.is_empty());

            let mut i = history.entries.find_index(span_remaining.start).unwrap();
            // let mut offset = history.entries[i].
            while !span_remaining.is_empty() {
                let txn = &history.entries[i];
                debug_assert!(span_remaining.start >= txn.span.start);

                let offset = Time::min(span_remaining.len(), txn.span.end - span_remaining.start);
                let span = span_remaining.truncate_keeping_right(offset);

                // We don't care about any parents outside of the input spans.
                let parent_idxs: SmallVec<[usize; 4]> = txn.parents.iter()
                    .filter_map(|t| find_entry_idx(&input, *t))
                    .collect();

                if parent_idxs.is_empty() {
                    to_process.push(input.len());
                }

                input.push(VisitEntry {
                    span,
                    txn_idx: i,
                    // entry: e,
                    visited: false,
                    parent_idxs,
                });

                i += 1;
            }
        }

        // I don't think this is needed, but it means we iterate in a sorted order.
        to_process.reverse();

        assert!(rev_spans.is_empty() || !to_process.is_empty());

        Self {
            history,
            frontier: start_at,
            input,
            to_process,
            num_consumed: 0,
        }
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

    pub fn into_frontier(self) -> LocalVersion {
        self.frontier
    }
}

impl<'a> Iterator for SpanningTreeWalker<'a> {
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

        let parents = if let Some(p) = next_txn.parent_at_time(input_entry.span.start) {
            smallvec![p]
        } else {
            clone_smallvec(&next_txn.parents)
        };

        let (only_branch, only_txn) = self.history.diff(&self.frontier, &parents);

        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch {:?} by {:?}", &self.branch, range);
            retreat_frontier_by(&mut self.frontier, self.history, *range);
            // println!(" -> {:?}", &self.branch);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            check_frontier(&self.frontier, self.history);
        }

        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", range);
            advance_frontier_by(&mut self.frontier, self.history, *range);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            check_frontier(&self.frontier, self.history);
        }

        // println!("consume {} (order {:?})", next_idx, next_txn.as_span());
        let input_span = input_entry.span;
        advance_frontier_by_known_run(&mut self.frontier, &parents, input_span);

        input_entry.visited = true;
        self.num_consumed += 1;
        self.push_children(next_txn.child_indexes.as_slice());

        Some(TxnWalkItem {
            retreat: only_branch,
            advance_rev: only_txn,
            parents,
            consume: input_span,
        })
    }
}

impl History {
    /// This function is for efficiently finding the order we should traverse the time DAG in order to
    /// walk all the changes so we can efficiently save everything to disk. This is needed because if
    /// we simply traverse the txns in the order they're in right now, we can have pathological
    /// behaviour in the presence of multiple interleaved branches. (Eg if you're streaming from two
    /// peers concurrently editing different branches).
    #[allow(unused)] // Used by testing at least.
    pub(crate) fn txn_spanning_tree_iter(&self) -> SpanningTreeWalker {
        SpanningTreeWalker::new_all(self)
    }

    // Works, but unused.
    // pub(crate) fn known_conflicting_txns_iter(&self, conflict: ConflictZone) -> OptimizedTxnsIter {
    //     OptimizedTxnsIter::new(self, &conflict.spans, conflict.common_ancestor)
    // }

    // Works, but unused.
    // pub(crate) fn conflicting_txns_iter(&self, a: &[Time], b: &[Time]) -> OptimizedTxnsIter {
    //     self.known_conflicting_txns_iter(self.find_conflicting_simple(a, b))
    // }

    pub(crate) fn optimized_txns_between(&self, from: &[Time], to: &[Time]) -> SpanningTreeWalker {
        let (_a, txns) = self.diff(from, to);
        // _a might always be empty.
        SpanningTreeWalker::new(self, &txns, from.into())
    }
}


#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::list::history::HistoryEntry;
    use crate::list::history_tools::ConflictZone;
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
                parents: smallvec![],
                child_indexes: smallvec![]
            },
            HistoryEntry {
                span: (10..30).into(), shadow: 0,
                parents: smallvec![],
                child_indexes: smallvec![]
            }
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![],
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: smallvec![],
                consume: (10..30).into(),
            },
        ]);
    }

    #[test]
    fn fork_and_join() {
        let history = History::from_entries(&[
            HistoryEntry {
                span: (0..10).into(), shadow: 0,
                parents: smallvec![],
                child_indexes: smallvec![2]
            },
            HistoryEntry {
                span: (10..30).into(), shadow: 10,
                parents: smallvec![],
                child_indexes: smallvec![2]
            },
            HistoryEntry {
                span: (30..50).into(), shadow: 0,
                parents: smallvec![9, 29],
                child_indexes: smallvec![]
            },
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![],
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: smallvec![],
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
            HistoryEntry { // a
                span: (0..1).into(), shadow: usize::MAX,
                parents: smallvec![],
                child_indexes: smallvec![2]
            },
            HistoryEntry { // b
                span: (1..2).into(), shadow: usize::MAX,
                parents: smallvec![],
                child_indexes: smallvec![3]
            },
            HistoryEntry { // a
                span: (2..3).into(), shadow: 2,
                parents: smallvec![0],
                child_indexes: smallvec![4]
            },
            HistoryEntry { // b
                span: (3..4).into(), shadow: 3,
                parents: smallvec![1],
                child_indexes: smallvec![4]
            },
            HistoryEntry { // a+b
                span: (4..5).into(), shadow: usize::MAX,
                parents: smallvec![2, 3],
                child_indexes: smallvec![]
            },
        ]);

        // dbg!(history.optimized_txns_between(&[3], &[4]).collect::<Vec<_>>());

        // history.traverse_txn_spanning_tree();
        let iter = SpanningTreeWalker::new_all(&history);
        // for item in iter {
        //     dbg!(item);
        // }

        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![],
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
                parents: smallvec![],
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
                shadow: usize::MAX,
                parents: smallvec![],
                child_indexes: smallvec![]
            },
        ]);

        let conflict = history.find_conflicting_simple(&[5], &[6]);
        assert_eq!(conflict, ConflictZone {
            common_ancestor: smallvec![5],
            spans: smallvec![(6..7).into()],
        });
        let iter = SpanningTreeWalker::new(&history, &conflict.spans, conflict.common_ancestor);
        // dbg!(&iter);

        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: smallvec![5],
                consume: (6..7).into(),
            }
        ])));
    }
}
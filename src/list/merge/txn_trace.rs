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
///
/// **Implementor's notes**:
/// This structure isn't very fit for purpose. Its designed to work in conjunction with the causal
/// graph methods for finding the conflicting set, but it essentially does a second (forward)
/// traversal of the data set with an honestly pretty crap traversal implementation.
///
/// A much better implementation would combine the method to find conflicting items with the logic
/// here, and do a much more efficient traversal. But I want to replace the whole algorithm that
/// does merging at the moment at some point anyway, so I'd rather do that all in one go and rewrite
/// this code when I do.

use std::mem::take;
use smallvec::{SmallVec, smallvec};
use crate::frontier::*;
use rle::{HasLength, SplitableSpan};
use crate::causalgraph::parents::Parents;
use crate::dtrange::DTRange;
use crate::frontier::clone_smallvec;
use crate::{Frontier, LV};

#[derive(Debug)]
// struct VisitEntry<'a> {
struct VisitEntry {
    span: DTRange,
    txn_idx: usize,
    // entry: &'a HistoryEntry,
    visited: bool,
    parents: Frontier,
    parent_idxs: SmallVec<[usize; 4]>,
    child_idxs: SmallVec<[usize; 4]>,
}


fn find_entry_idx(input: &SmallVec<[VisitEntry; 4]>, time: LV) -> Option<usize> {
    input.as_slice().binary_search_by(|e| {
        e.span.partial_cmp_time(time).reverse()
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
    history: &'a Parents,

    frontier: Frontier,

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
    pub(crate) parents: Frontier,
    pub(crate) consume: DTRange,
}

impl<'a> SpanningTreeWalker<'a> {
    #[allow(unused)]
    pub(crate) fn new_all(history: &'a Parents) -> Self {
        let mut spans: SmallVec<[DTRange; 4]> = smallvec![];
        if history.get_next_time() > 0 {
            // Kinda gross. Only add the span if the document isn't empty.
            spans.push((0..history.get_next_time()).into());
        }
        // spans.reverse(); // Unneeded - there's 0 or 1 item in the spans list.

        Self::new(history, &spans, Frontier::root())
    }

    // TODO: It'd be cleaner to pass in spans as an Iterator<Item=DTRange>.
    pub(crate) fn new(history: &'a Parents, rev_spans: &[DTRange], start_at: Frontier) -> Self {
        // println!("\n----- NEW TRAVERSAL -----");

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
                debug_assert!(span_remaining.start >= txn.span.start && span_remaining.start < txn.span.end);

                let offset = LV::min(span_remaining.len(), txn.span.end - span_remaining.start);
                let span = span_remaining.truncate_keeping_right(offset);

                // dbg!(span_remaining.start);
                let parents: Frontier = txn.clone_parents_at_time(span.start);

                // We don't care about any parents outside of the input spans.
                let parent_idxs: SmallVec<[usize; 4]> = parents.iter()
                    .filter_map(|t| find_entry_idx(&input, *t))
                    .collect();

                // println!("TXN {i} span {:?} (remaining {:?}) parents {:?} idxs {:?}", span, span_remaining, &parents, &parent_idxs);

                if parent_idxs.is_empty() {
                    to_process.push(input.len());
                }

                input.push(VisitEntry {
                    span,
                    txn_idx: i,
                    
                    // entry: e,
                    visited: false,
                    parents,
                    parent_idxs,
                    child_idxs: smallvec![] // We can't process these yet.
                });

                i += 1;
            }
        }

        // Now populate the child_idxs.
        for i in 0..input.len() {
            let VisitEntry {
                span, txn_idx, ..
            } = input[i];

            let txn = &history.entries[txn_idx];
            // input[i].child_idxs = txn.child_indexes.iter()
            //     .filter(|i| history.entries[**i]
            //     .map(|i| history.entries[*i].span.start)
            //     .filter_map(|t| find_entry_idx(&input, t))
            //     .collect();

            for history_child_idx in txn.child_indexes.iter() {
                let child_txn = &history.entries[*history_child_idx];
                if let Some(child_idx) = find_entry_idx(&input, child_txn.span.start) {

                    // Only add it if the parents names something within span.
                    if child_txn.parents.iter().any(|p| span.contains(*p)) {
                        input[i].child_idxs.push(child_idx);
                    }
                }
            }
        }

        // dbg!(&input);
        // dbg!(&to_process);

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

    pub fn into_frontier(self) -> Frontier {
        self.frontier
    }

    fn check(&self) {
        if cfg!(debug_assertions) {
            for p in &self.to_process {
                let e = &self.input[*p];
                debug_assert!(!e.visited);
                debug_assert!(e.parent_idxs.iter().all(|i| self.input[*i].visited));
            }

            // dbg!(&self.to_process);
            for i in 0..self.to_process.len() {
                // Everything should be in here exactly once!
                for j in 0..self.to_process.len() {
                    if i != j {
                        assert_ne!(self.to_process[i], self.to_process[j]);
                    }
                }
            }
        }
    }
}

impl<'a> Iterator for SpanningTreeWalker<'a> {
    type Item = TxnWalkItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.check();

        // Find the next item to consume. This is super sloppy. We'll preferentially process all
        // non-merge commits first. Then prefer anything at the end of to_process. This should be
        // rewritten to use a priority queue.
        let next_idx = if let Some(&idx) = self.to_process.last() {
            let e = &self.input[idx];
            if e.parents.len() >= 2 {
                // Try and find something with no parents to expand first.
                if let Some((ii, &i)) = self.to_process.iter().enumerate().rfind(|(ii, i)| {
                    self.input[**i].parents.len() < 2
                }) {
                    self.to_process.swap_remove(ii);
                    i
                } else {
                    self.to_process.pop();
                    idx
                }
            } else {
                self.to_process.pop();
                idx
            }
        } else {
            // We're done here.
            debug_assert!(self.input.iter().all(|e| e.visited));
            debug_assert_eq!(self.num_consumed, self.input.len());
            return None;
        };


        // println!("Expanding idx {next_idx}");

        // let input_entry = &mut self.input[next_idx];
        let input_entry = &mut self.input[next_idx];
        input_entry.visited = true;
        let child_idxs = take(&mut input_entry.child_idxs);
        let parents = take(&mut input_entry.parents);
        let span = input_entry.span;
        drop(input_entry);

        // dbg!(&child_idxs);

        // let parents = &input_entry.parents;
        let (only_branch, only_txn) = self.history.diff(self.frontier.as_ref(), parents.as_ref());

        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch {:?} by {:?}", &self.branch, range);
            self.frontier.retreat(self.history, *range);
            // println!(" -> {:?}", &self.branch);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            self.frontier.check(self.history);
        }

        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", range);
            self.frontier.advance(self.history, *range);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            self.frontier.check(self.history);
        }

        // println!("consume {} (order {:?})", next_idx, next_txn.as_span());
        let input_span = span;
        self.frontier.advance_by_known_run(parents.as_ref(), input_span);

        self.num_consumed += 1;

        'outer: for c in child_idxs {
            if self.input[c].visited { continue; }
            for p in &self.input[c].parent_idxs {
                if !self.input[*p].visited {
                    continue 'outer;
                }
            }
            self.to_process.push(c);
        }

        self.check();

        Some(TxnWalkItem {
            retreat: only_branch,
            advance_rev: only_txn,
            // parents: parents.iter().copied().collect(), // TODO: clean this
            parents,
            consume: input_span,
        })
    }
}

impl Parents {
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

    pub(crate) fn optimized_txns_between(&self, from: &[LV], to: &[LV]) -> SpanningTreeWalker {
        let (_a, txns) = self.diff(from, to);
        // _a might always be empty.
        SpanningTreeWalker::new(self, &txns, from.into())
    }
}


#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::causalgraph::parents::ParentsEntryInternal;
    use crate::causalgraph::parents::tools::ConflictZone;
    use super::*;

    #[test]
    fn iter_span_for_empty_doc() {
        let history = Parents::new();
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        assert!(walk.is_empty());
    }

    #[test]
    fn iter_span_from_root() {
        let history = Parents::from_entries(&[
            ParentsEntryInternal {
                span: (0..10).into(), shadow: 0,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![]
            },
            ParentsEntryInternal {
                span: (10..30).into(), shadow: 0,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![]
            }
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (10..30).into(),
            },
        ]);
    }

    #[test]
    fn fork_and_join() {
        let history = Parents::from_entries(&[
            ParentsEntryInternal {
                span: (0..10).into(), shadow: 0,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![2]
            },
            ParentsEntryInternal {
                span: (10..30).into(), shadow: 10,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![2]
            },
            ParentsEntryInternal {
                span: (30..50).into(), shadow: 0,
                parents: Frontier(smallvec![9, 29]),
                child_indexes: smallvec![]
            },
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (10..30).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(0..10).into()],
                parents: Frontier(smallvec![9, 29]),
                consume: (30..50).into(),
            },
        ]);

        // dbg!(walk);
    }

    #[test]
    fn two_chains() { // Sounds like the name of a rap song.
        let history = Parents::from_entries(&[
            ParentsEntryInternal { // a
                span: (0..1).into(), shadow: usize::MAX,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![2]
            },
            ParentsEntryInternal { // b
                span: (1..2).into(), shadow: usize::MAX,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![3]
            },
            ParentsEntryInternal { // a
                span: (2..3).into(), shadow: 2,
                parents: Frontier(smallvec![0]),
                child_indexes: smallvec![4]
            },
            ParentsEntryInternal { // b
                span: (3..4).into(), shadow: 3,
                parents: Frontier(smallvec![1]),
                child_indexes: smallvec![4]
            },
            ParentsEntryInternal { // a+b
                span: (4..5).into(), shadow: usize::MAX,
                parents: Frontier(smallvec![2, 3]),
                child_indexes: smallvec![]
            },
        ]);

        // dbg!(history.optimized_txns_between(&[3], &[4]).collect::<Vec<_>>());
        // history.traverse_txn_spanning_tree();

        // let iter = SpanningTreeWalker::new_all(&history);
        // for item in iter {
        //     dbg!(item);
        // }

        let iter = SpanningTreeWalker::new_all(&history);
        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (0..1).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![0]),
                consume: (2..3).into(),
            },

            TxnWalkItem {
                retreat: smallvec![(2..3).into(), (0..1).into()],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![]),
                consume: (1..2).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![1]),
                consume: (3..4).into(),
            },

            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(2..3).into(), (0..1).into()],
                parents: Frontier(smallvec![2, 3]),
                consume: (4..5).into(),
            },
        ])));
    }

    #[test]
    fn iter_txn_middle() {
        // regression
        let history = Parents::from_entries(&[
            ParentsEntryInternal {
                span: (0..10).into(),
                shadow: usize::MAX,
                parents: Frontier(smallvec![]),
                child_indexes: smallvec![]
            },
        ]);

        let conflict = history.find_conflicting_simple(&[5], &[6]);
        assert_eq!(conflict, ConflictZone {
            common_ancestor: Frontier(smallvec![5]),
            spans: smallvec![(6..7).into()],
        });
        let iter = SpanningTreeWalker::new(&history, &conflict.spans, conflict.common_ancestor);
        // dbg!(&iter);

        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier(smallvec![5]),
                consume: (6..7).into(),
            }
        ])));
    }
}
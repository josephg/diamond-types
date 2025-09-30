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
///
/// And what do you know! It happened. So this is now only used for optimizing the oplog order when
/// encoding. TODO: Replace this file entirely with something simpler based off ConflictGraph.

use std::mem::take;
use smallvec::{SmallVec, smallvec};
use rle::{HasLength, SplitableSpan};
use crate::causalgraph::graph::Graph;
use crate::dtrange::DTRange;
use crate::{Frontier, LV};

#[derive(Debug)]
// struct VisitEntry<'a> {
struct VisitEntry {
    span: DTRange,
    txn_idx: usize,
    // entry: &'a HistoryEntry,
    parents: Frontier,
    parent_idxs: SmallVec<usize, 4>,
    child_idxs: SmallVec<usize, 4>,
    visited: bool,
}


fn find_entry_idx(input: &SmallVec<VisitEntry, 4>, time: LV) -> Option<usize> {
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
// The traversal must obey the ordering rule for causal graphs. No item can be visited before all
// of its parents have been visited.
//
// The code was manually unrolled into an iterator so we could walk it without needing to collect
// this structure to a vec or something.
#[derive(Debug)]
pub(crate) struct SpanningTreeWalker<'a> {
    // I could hold a slice reference here instead, but it'd be missing the find() methods.
    subgraph: &'a Graph,

    frontier: Frontier,

    input: SmallVec<VisitEntry, 4>,

    /// List of input_idx.
    ///
    /// This is sort of like a call stack of txns we push and pop from as we traverse
    to_process: SmallVec<usize, 4>, // smallvec? This will have an upper bound of the number of txns.

    num_consumed: usize, // For debugging.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxnWalkItem {
    pub(crate) retreat: SmallVec<DTRange, 4>,
    pub(crate) advance_rev: SmallVec<DTRange, 4>,


    // txn: &'a TxnSpan,
    pub(crate) parents: Frontier,
    pub(crate) consume: DTRange,
}

impl<'a> SpanningTreeWalker<'a> {
    #[allow(unused)]
    pub(crate) fn new_all(graph: &'a Graph) -> Self {
        let mut spans: SmallVec<DTRange, 4> = smallvec![];
        if graph.get_next_time() > 0 {
            // Kinda gross. Only add the span if the document isn't empty.
            spans.push((0..graph.get_next_time()).into());
        }
        // spans.reverse(); // Unneeded - there's 0 or 1 item in the spans list.

        Self::new(graph, &spans, Frontier::root())
    }

    // TODO: It'd be cleaner to pass in spans as an Iterator<Item=DTRange>.
    pub(crate) fn new(graph: &'a Graph, rev_spans: &[DTRange], start_at: Frontier) -> Self {
        // println!("\n----- NEW TRAVERSAL -----");

        if cfg!(debug_assertions) {
            check_rev_sorted(rev_spans);
        }

        let mut input = smallvec![];
        let mut to_process = smallvec![];

        for mut span_remaining in rev_spans.iter().rev().copied() {
            debug_assert!(!span_remaining.is_empty());

            let mut i = graph.entries.find_index(span_remaining.start).unwrap();
            // let mut offset = history.entries[i].
            while !span_remaining.is_empty() {
                let txn = &graph.entries[i];
                debug_assert!(span_remaining.start >= txn.span.start && span_remaining.start < txn.span.end);

                let offset = LV::min(span_remaining.len(), txn.span.end - span_remaining.start);
                let span = span_remaining.truncate_keeping_right(offset);

                // dbg!(span_remaining.start);
                let parents: Frontier = txn.clone_parents_at_version(span.start);

                // We don't care about any parents outside of the input spans.
                let parent_idxs: SmallVec<usize, 4> = parents.iter()
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
            for p_i in 0..input[i].parent_idxs.len() {
                let p = input[i].parent_idxs[p_i];
                input[p].child_idxs.push(i);
            }
        }

        // dbg!(&input);
        // dbg!(&to_process);

        // I don't think this is needed, but it means we iterate in a sorted order.
        to_process.reverse();

        assert!(rev_spans.is_empty() || !to_process.is_empty());

        Self {
            subgraph: graph,
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

    //noinspection RsDropRef
    fn next(&mut self) -> Option<Self::Item> {
        self.check();

        // Find the next item to consume. This is super sloppy. We'll preferentially process all
        // non-merge commits first. Then prefer anything at the end of to_process. This should be
        // rewritten to use a priority queue.
        let next_idx = if let Some(&idx) = self.to_process.last() {
            let e = &self.input[idx];
            if e.parents.len() >= 2 {
                // Try and find something with no parents to expand first.
                if let Some((ii, &i)) = self.to_process.iter().enumerate().rfind(|(_ii, i)| {
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
        // input_entry is not used after this point.

        // dbg!(&child_idxs);

        // let parents = &input_entry.parents;
        let (only_branch, only_txn) = self.subgraph.diff_rev(self.frontier.as_ref(), parents.as_ref());

        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch {:?} by {:?}", &self.branch, range);
            self.frontier.retreat(self.subgraph, *range);
            // println!(" -> {:?}", &self.branch);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            self.frontier.check(self.subgraph);
        }

        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", range);
            self.frontier.advance(self.subgraph, *range);
            // dbg!(&branch);
        }

        if cfg!(debug_assertions) {
            self.frontier.check(self.subgraph);
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

impl Graph {
    /// This function is for efficiently finding the order we should traverse the time DAG in order to
    /// walk all the changes so we can efficiently save everything to disk. This is needed because if
    /// we simply traverse the txns in the order they're in right now, we can have pathological
    /// behaviour in the presence of multiple interleaved branches. (Eg if you're streaming from two
    /// peers concurrently editing different branches).
    #[allow(unused)] // Used by testing at least.
    pub(crate) fn txn_spanning_tree_iter(&self) -> SpanningTreeWalker<'_> {
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

    pub(crate) fn optimized_txns_between(&self, from: &[LV], to: &[LV]) -> SpanningTreeWalker<'_> {
        let (_a, txns) = self.diff_rev(from, to);
        // _a might always be empty.
        SpanningTreeWalker::new(self, &txns, from.into())
    }
}


#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use smallvec::smallvec;
    use crate::causalgraph::graph::GraphEntrySimple;
    use crate::causalgraph::graph::tools::ConflictZone;
    use crate::list::ListOpLog;
    use crate::listmerge::plan::M1PlanAction;
    use super::*;

    #[test]
    fn iter_span_for_empty_doc() {
        let graph = Graph::new();
        let walk = graph.txn_spanning_tree_iter().collect::<Vec<_>>();
        assert!(walk.is_empty());
    }

    #[test]
    fn iter_span_from_root() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..10).into(), parents: Frontier::root() },
            GraphEntrySimple { span: (10..30).into(), parents: Frontier::root() }
        ]);
        let walk = graph.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (10..30).into(),
            },
        ]);
    }

    #[test]
    fn fork_and_join() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..10).into(), parents: Frontier::root() },
            GraphEntrySimple { span: (10..30).into(), parents: Frontier::root() },
            GraphEntrySimple { span: (30..50).into(), parents: Frontier::from_sorted(&[9, 29]) },
        ]);
        let walk = graph.txn_spanning_tree_iter().collect::<Vec<_>>();
        // dbg!(&walk);

        assert_eq!(walk, [
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (0..10).into(),
            },
            TxnWalkItem {
                retreat: smallvec![(0..10).into()],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (10..30).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(0..10).into()],
                parents: Frontier::from_sorted(&[9, 29]),
                consume: (30..50).into(),
            },
        ]);

        // dbg!(walk);
    }

    #[test]
    fn two_chains() { // Sounds like the name of a rap song.
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..1).into(), parents: Frontier::root() }, // a
            GraphEntrySimple { span: (1..2).into(), parents: Frontier::root() }, // b
            GraphEntrySimple { span: (2..3).into(), parents: Frontier::from_sorted(&[0]) }, // a
            GraphEntrySimple { span: (3..4).into(), parents: Frontier::from_sorted(&[1]) }, // b
            GraphEntrySimple { span: (4..5).into(), parents: Frontier::from_sorted(&[2, 3]) }, // a+b
        ]);

        // dbg!(history.optimized_txns_between(&[3], &[4]).collect::<Vec<_>>());
        // history.traverse_txn_spanning_tree();

        // let iter = SpanningTreeWalker::new_all(&history);
        // for item in iter {
        //     dbg!(item);
        // }

        let iter = SpanningTreeWalker::new_all(&graph);
        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (0..1).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::from_sorted(&[0]),
                consume: (2..3).into(),
            },

            TxnWalkItem {
                retreat: smallvec![(2..3).into(), (0..1).into()],
                advance_rev: smallvec![],
                parents: Frontier::root(),
                consume: (1..2).into(),
            },
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::from_sorted(&[1]),
                consume: (3..4).into(),
            },

            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![(2..3).into(), (0..1).into()],
                parents: Frontier::from_sorted(&[2, 3]),
                consume: (4..5).into(),
            },
        ])));
    }

    #[test]
    fn iter_txn_middle() {
        // regression
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..10).into(), parents: Frontier::root() },
        ]);

        let conflict = graph.find_conflicting_simple(&[5], &[6]);
        assert_eq!(conflict, ConflictZone {
            common_ancestor: Frontier::from_sorted(&[5]),
            rev_spans: smallvec![(6..7).into()],
        });
        let iter = SpanningTreeWalker::new(&graph, &conflict.rev_spans, conflict.common_ancestor);
        // dbg!(&iter);

        assert!(iter.eq(IntoIterator::into_iter([
            TxnWalkItem {
                retreat: smallvec![],
                advance_rev: smallvec![],
                parents: Frontier::from_sorted(&[5]),
                consume: (6..7).into(),
            }
        ])));
    }


    // #[test]
    // #[ignore]
    // fn print_simple_plan() {
    //     let g = Graph::from_simple_items(&[
    //         GraphEntrySimple { span: (0..1).into(), parents: Frontier::root() },
    //         GraphEntrySimple { span: (1..2).into(), parents: Frontier::new_1(0) },
    //         GraphEntrySimple { span: (2..3).into(), parents: Frontier::new_1(0) },
    //         GraphEntrySimple { span: (3..4).into(), parents: Frontier::from_sorted(&[1, 2]) },
    //     ]);
    //
    //     // let iter = SpanningTreeWalker::new_all(&g);
    //     // iter.dbg_print();
    // }

    #[test]
    #[ignore]
    fn print_file_plan() {
        let mut bytes = vec![];
        // File::open("benchmark_data/git-makefile.dt").unwrap().read_to_end(&mut bytes).unwrap();
        // File::open("benchmark_data/node_nodecc.dt").unwrap().read_to_end(&mut bytes).unwrap();
        File::open("benchmark_data/clownschool.dt").unwrap().read_to_end(&mut bytes).unwrap();
        // File::open("benchmark_data/friendsforever.dt").unwrap().read_to_end(&mut bytes).unwrap();
        let o = ListOpLog::load_from(&bytes).unwrap();
        let cg = &o.cg;

        // let iter = SpanningTreeWalker::new_all(&cg.graph);
        // iter.dbg_print2();
        //
        // let iter = SpanningTreeWalker::new_all(&cg.graph);
        // let mut cost_estimate = 0;
        // for i in iter {
        //     // cost_estimate += i.consume.len();
        //     // cost_estimate += i.retreat.iter().map(|range| range.len()).sum::<usize>();
        //     // cost_estimate += i.advance_rev.iter().map(|range| range.len()).sum::<usize>();
        //     // cost_estimate += o.estimate_cost(i.consume);
        //
        //     // cost_estimate += i.retreat.iter().map(|range| o.estimate_cost(*range)).sum::<usize>();
        //     // cost_estimate += i.advance_rev.iter().map(|range| o.estimate_cost(*range)).sum::<usize>();
        // }
        // println!("Cost estimate {cost_estimate}");
        // node_nodecc Cost estimate 1103811 / 63696
        // git-makefile Cost estimate 1128743 / 50680

        // -----

        let (plan, _) = cg.graph.make_m1_plan(Some(&o.operations), &[], cg.version.as_ref(), true);

        let mut cost_estimate = 0;
        let mut clears = 0;
        let mut ff_len = 0;
        let mut apply_len = 0;
        for a in plan.0.iter() {
            match a {
                M1PlanAction::Retreat(span) | M1PlanAction::Advance(span) => {
                    cost_estimate += o.estimate_cost(*span);
                }
                M1PlanAction::Clear => { clears += 1; }
                M1PlanAction::Apply(span) => {
                    // cost_estimate += o.estimate_cost(*span);
                    apply_len += o.estimate_cost(*span);
                }
                M1PlanAction::FF(span) => {
                    // cost_estimate += o.estimate_cost(*span);
                    ff_len += o.estimate_cost(*span);
                }
                M1PlanAction::BeginOutput => {}
            }
        }
        println!("plan length {} (vs graph len {})", plan.0.len(), cg.graph.entries.0.len());
        println!("New cost estimate {cost_estimate}. Clears: {clears}");
        println!("ff_len {ff_len} / apply_len {apply_len}");
        plan.dbg_print();
    }
}
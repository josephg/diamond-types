//! This is a POC for what an action plan would look like using the current list merging algorithm
//! instead of the new one.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use rle::{AppendRle, MergableSpan};
use crate::{CausalGraph, DTRange, Frontier, LV};
use crate::causalgraph::graph::Graph;
use crate::listmerge2::ConflictSubgraph;
use crate::causalgraph::graph::tools::DiffFlag;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum M1PlanAction {
    Retreat(DTRange),
    Advance(DTRange),
    Clear,
    Apply(DTRange),
    FF(DTRange),
    BeginOutput,
}

impl MergableSpan for M1PlanAction {
    fn can_append(&self, other: &Self) -> bool {
        use M1PlanAction::*;
        match (self, other) {
            (Retreat(r1), Retreat(r2)) => r2.can_append(r1),
            (Advance(r1), Advance(r2))
                | (FF(r1), FF(r2))
                | (Apply(r1), Apply(r2)) => r1.can_append(r2),
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        use M1PlanAction::*;
        match (self, other) {
            (Retreat(r1), Retreat(r2)) => { r1.start = r2.start },
            (Advance(r1), Advance(r2))
            | (FF(r1), FF(r2))
            | (Apply(r1), Apply(r2)) => r1.append(r2),
            _ => unreachable!()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct M1Plan(pub Vec<M1PlanAction>);


#[derive(Debug, Clone, Default)]
pub(crate) struct M1EntryState {
    // index: Option<Index>, // Primary index for merges / backup index for forks.
    next: usize, // Starts at 0. 0..parents.len() is where we scan parents, then we scan children.
    // // emitted_this_span: bool,
    // children_needing_index: usize, // For forks

    // children_visited: usize,
    parents_satisfied: usize,

    visited: bool,
    critical_path: bool,

    // children: SmallVec<[usize; 2]>,
}


// struct SubgraphChildren(Vec<SmallVec<[usize; 2]>>);

impl ConflictSubgraph<M1EntryState> {
    // This method is adapted from the equivalent method in the causal graph code.
    fn diff_trace<F: FnMut(usize, DiffFlag)>(&self, from_idx: usize, after: bool, to_idx: usize, mut visit: F) {
        use DiffFlag::*;
        // Sorted highest to lowest.
        let mut queue: BinaryHeap<Reverse<(usize, DiffFlag)>> = BinaryHeap::new();
        if after {
            queue.push(Reverse((from_idx, OnlyA)));
        } else {
            for p in &self.entries[from_idx].parents {
                queue.push(Reverse((*p, OnlyA)));
            }
        }

        for p in &self.entries[to_idx].parents {
            queue.push(Reverse((*p, OnlyB)));
        }

        let mut num_shared_entries = 0;

        while let Some(Reverse((idx, mut flag))) = queue.pop() {
            if flag == Shared { num_shared_entries -= 1; }

            // dbg!((ord, flag));
            while let Some(Reverse((peek_idx, peek_flag))) = queue.peek() {
                if *peek_idx == idx {
                    // The peeked item is the same as idx. Merge and drop it.
                    // 3 cases if peek_flag != flag. We set flag = Shared in all cases.
                    if *peek_flag != flag { flag = Shared; }
                    if *peek_flag == Shared { num_shared_entries -= 1; }
                    queue.pop();
                } else { break; }
            }

            let entry = &self.entries[idx];
            if flag != Shared {
                visit(idx, flag);
            }

            // mark_run(containing_txn.span.start, idx, flag);
            for p_idx in entry.parents.iter() {
                queue.push(Reverse((*p_idx, flag)));
                if flag == Shared { num_shared_entries += 1; }
            }

            // If there's only shared entries left, abort.
            if queue.len() == num_shared_entries { break; }
        }
    }



    // This function does a BFS through the graph, setting the state appropriately.
    // fn prepare(&mut self) -> SubgraphChildren {
    fn prepare(&mut self) {
        // if self.0.is_empty() { return SubgraphChildren(vec![]); }
        if self.entries.is_empty() { return; }

        // For each item, this calculates whether the item is on the critical path.
        let mut queue: BinaryHeap<Reverse<usize>> = BinaryHeap::new();
        if self.entries[self.a_root].flag == DiffFlag::OnlyA {
            queue.push(Reverse(self.a_root));
        }
        if self.entries[self.b_root].flag == DiffFlag::OnlyB {
            queue.push(Reverse(self.b_root));
        }
        // queue.push(Reverse(self.a_root));
        // queue.push(Reverse(self.b_root));

        while let Some(Reverse(idx)) = queue.pop() {
            let e = &mut self.entries[idx];

            while let Some(Reverse(peek_idx)) = queue.peek() {
                // This is needed to avoid items seeming to be concurrent with themselves.
                if *peek_idx == idx {
                    queue.pop();
                } else { break; }
            }
            // println!("idx {idx} queue {:?}", queue);
            e.state.critical_path = queue.is_empty();
            queue.extend(e.parents.iter().copied().map(|i| Reverse(i)));
        }

        // And sort the parents to hit the lowest spans first.
        // for e in self.entries.iter_mut() {
        //     e.parents.reverse();
        // }
        // self.dbg_print();
    }

    // fn make_m1_plan(&mut self) -> M1Plan {
    //     let mut actions = vec![];
    //     if self.0.is_empty() { return M1Plan(actions); }
    //
    //     let mut stack: Vec<usize> = vec![];
    //     // let mut stack_with_more_children = 0;
    //
    //     let children = self.prepare();
    //
    //     let mut current_idx = self.0.len() - 1;
    //
    //     let mut last_processed_after: bool = false;
    //     let mut last_processed_idx: usize = 0; // Might be cleaner to start this at None or something.
    //
    //     let mut nonempty_spans_remaining = self.0.iter()
    //         .filter(|e| !e.span.is_empty())
    //         .count();
    //
    //     'outer: loop {
    //         dbg!(current_idx);
    //         // Borrowing immutably to please the borrow checker.
    //         let e = &self.0[current_idx];
    //
    //         debug_assert_eq!(e.state.parents_satisfied, e.parents.len());
    //
    //         if !e.state.visited {
    //             // assert_eq!(e.state.visited, false);
    //             debug_assert!(e.parents.iter().all(|p| self.0[*p].state.visited), "Have not visited all parents");
    //             debug_assert_eq!(e.state.children_visited, 0);
    //
    //             if e.parents.as_slice() != &[last_processed_idx] {
    //                 // Merge parents together.
    //                 if e.parents.len() >= 2 {
    //                     // let len_start = actions.len();
    //
    //                     let mut advances: SmallVec<[DTRange; 2]> = smallvec![];
    //                     let mut retreats: SmallVec<[DTRange; 2]> = smallvec![];
    //                     self.diff_trace(last_processed_idx, last_processed_after, current_idx, |idx, flag| {
    //                         let list = match flag {
    //                             DiffFlag::OnlyA => &mut retreats,
    //                             DiffFlag::OnlyB => &mut advances,
    //                             DiffFlag::Shared => { return; }
    //                         };
    //                         let span = self.0[idx].span;
    //                         if !span.is_empty() {
    //                             list.push(span);
    //                         }
    //                     });
    //
    //                     if !retreats.is_empty() {
    //                         actions.extend(retreats.into_iter().map(M1PlanAction::Retreat));
    //                     }
    //                     if !advances.is_empty() {
    //                         // .rev() here because diff visits everything in reverse order.
    //                         actions.extend(advances.into_iter().rev().map(M1PlanAction::Advance));
    //                     }
    //                 }
    //             }
    //
    //             // println!("Processing {current_idx}");
    //
    //             if !e.span.is_empty() {
    //                 actions.push(if e.state.critical_path {
    //                     M1PlanAction::FF(e.span)
    //                 } else {
    //                     M1PlanAction::Apply(e.span)
    //                 });
    //
    //                 // We can stop as soon as we've processed all the spans.
    //                 nonempty_spans_remaining -= 1;
    //                 if nonempty_spans_remaining == 0 { break 'outer; } // break;
    //             }
    //
    //
    //             last_processed_after = true;
    //             last_processed_idx = current_idx;
    //
    //             // We shouldn't get here because we should have stopped as soon as we've seen
    //             // everything.
    //             debug_assert!(e.num_children > 0);
    //             // Essentially, go down.
    //             // if e.num_children == 0 { // Equivalent to current_idx == 0.
    //             //     // There is only 1 entry with no children: index 0.
    //             //     debug_assert_eq!(current_idx, 0);
    //             //     self.0[0].state.visited = true; // Gross.
    //             //
    //             //     // println!("Done");
    //             //     break 'outer;
    //             // }
    //
    //             self.0[current_idx].state.visited = true;
    //             for c in &children.0[current_idx] {
    //                 self.0[*c].state.parents_satisfied += 1;
    //             }
    //
    //             // stack.push(current_idx);
    //             // stack_with_more_children += 1;
    //         }
    //
    //         // Ok, now we need to find the next item to visit. We'll walk back up the stack, looking
    //         // for the next child with all of *its* parents visited.
    //         loop {
    //             let e = &self.0[current_idx];
    //             if e.state.children_visited < e.num_children {
    //                 // Look for a child with all of its parents visited.
    //                 let ch = &children.0[current_idx];
    //                 if let Some(&next_idx) = ch.iter().find(|&p| {
    //                     let e2 = &self.0[*p];
    //                     !e2.state.visited && e2.state.parents_satisfied == e2.parents.len()
    //                 }) {
    //                     // next_idx is the index of a child of current_index with its parents
    //                     // satisfied. Lets go there next.
    //                     println!("Found child {} of {}", next_idx, current_idx);
    //                     self.0[current_idx].state.children_visited += 1;
    //                     stack.push(current_idx);
    //                     current_idx = next_idx;
    //                     continue 'outer;
    //                 }
    //             }
    //
    //             // debug_assert_eq!(last_processed_idx, current_idx);
    //             // debug_assert_eq!(last_processed_after, true);
    //             println!("Retreat {}", current_idx);
    //             if !e.span.is_empty() {
    //                 actions.push(M1PlanAction::Retreat(e.span));
    //             }
    //
    //             last_processed_idx = current_idx;
    //             last_processed_after = false;
    //
    //             current_idx = stack.pop().unwrap();
    //             // println!("back to {}", current_idx);
    //         }
    //     }
    //
    //     M1Plan(actions)
    // }

    pub(super) fn make_m1_plan(&mut self) -> M1Plan {
        let mut actions = vec![];

        // The flag for b_root will only be OnlyB if we're adding something to the graph.
        // if self.entries.is_empty() || self.entries[self.b_root].flag != DiffFlag::OnlyB {
        if self.entries.is_empty() {
            return M1Plan(actions);
        }
        self.prepare();

        let mut nonempty_spans_remaining = self.entries.iter()
            .filter(|e| !e.span.is_empty())
            .count();

        let mut last_processed_after: bool = false;
        let mut last_processed_idx: usize = self.entries.len() - 1; // Might be cleaner to start this at None or something.

        // Basically, process a_root then b_root.
        let mut current_idx = self.a_root;
        let mut stack: Vec<usize> = vec![];

        let mut done_b = false;

        let mut dirty = false;

        'outer: loop {
            // println!("{current_idx} / {:?}", stack);

            // Borrowing immutably to please the borrow checker.
            let e = &self.entries[current_idx];

            assert_eq!(e.state.visited, false);

            // There's two things we could do here:
            // 1. Go up to one of our parents
            // 2. Visit this item and go down.

            let parents_len = e.parents.len();
            // Go to the next unvisited parent.
            let mut e_next = e.state.next;
            while e_next < parents_len {
                let p = e.parents[e_next];
                if self.entries[p].state.visited { // But it might have already been visited.
                    // g[current_idx].state.next += 1;
                    e_next += 1;
                } else {
                    // Go up and process this child.
                    self.entries[current_idx].state.next = e_next + 1;
                    stack.push(current_idx);
                    current_idx = p;
                    // println!("Bumping to parent {current_idx}");
                    continue 'outer;
                }
            }

            // Ok, process this element.
            let e = &mut self.entries[current_idx];
            e.state.next = e_next;
            // debug_assert_eq!(e.state.next, e.parents.len());
            // println!("Processing {current_idx} {:?}", e.span);
            e.state.visited = true;
            let e = &self.entries[current_idx];

            // Process this span.
            if !e.span.is_empty() {
                if e.state.critical_path {
                    if dirty {
                        actions.push(M1PlanAction::Clear);
                        dirty = false;
                    }
                    actions.push_rle(M1PlanAction::FF(e.span));
                } else {
                    // Note we only advance & retreat if the item is not on the critical path.
                    // If we're on the critical path, the clear operation will flush everything
                    // anyway.
                    let mut advances: SmallVec<[DTRange; 2]> = smallvec![];
                    let mut retreats: SmallVec<[DTRange; 2]> = smallvec![];
                    self.diff_trace(last_processed_idx, last_processed_after, current_idx, |idx, flag| {
                        let list = match flag {
                            DiffFlag::OnlyA => &mut retreats,
                            DiffFlag::OnlyB => &mut advances,
                            DiffFlag::Shared => { return; }
                        };
                        let span = self.entries[idx].span;
                        if !span.is_empty() {
                            list.push(span);
                        }
                    });

                    if !retreats.is_empty() {
                        actions.extend_rle(retreats.into_iter().map(M1PlanAction::Retreat));
                    }
                    if !advances.is_empty() {
                        // .rev() here because diff visits everything in reverse order.
                        actions.extend_rle(advances.into_iter().rev().map(M1PlanAction::Advance));
                    }

                    dirty = true;
                    actions.push_rle(M1PlanAction::Apply(e.span));
                }

                // We can stop as soon as we've processed all the spans.
                nonempty_spans_remaining -= 1;
                if nonempty_spans_remaining == 0 { break 'outer; } // break;

                last_processed_after = true;
                last_processed_idx = current_idx;
            }

            // Then go down again.
            if let Some(next_idx) = stack.pop() {
                current_idx = next_idx;
            } else if !done_b {
                // println!("DOING B");
                current_idx = self.b_root;
                actions.push(M1PlanAction::BeginOutput);
                done_b = true;
            } else {
                panic!("Should have stopped");
                // break;
            }
        }

        M1Plan(actions)
    }
}

impl Graph {
    pub(crate) fn make_m1_plan(&self, a: &[LV], b: &[LV]) -> (M1Plan, Frontier) {
        if self.frontier_contains_frontier(a, b) {
            // Nothing to merge. Do nothing.
            return (M1Plan(vec![]), Frontier::root());
        }

        let mut sg = self.make_conflict_graph_between(a, b);
        (sg.make_m1_plan(), sg.base_version)
    }
}

impl M1Plan {
    fn dbg_check(&self, common_ancestor: &[LV], a: &[LV], b: &[LV], graph: &Graph) {
        // dbg!(self, a, b);
        assert!(self.0.iter().filter(|&&a| a == M1PlanAction::BeginOutput).count() <= 1);

        let mut current: Frontier = common_ancestor.into();
        let mut max: Frontier = common_ancestor.into();
        let mut cleared_version: Frontier = common_ancestor.into();
        let mut seen_begin_output = false;

        for action in &self.0 {
            match action {
                M1PlanAction::BeginOutput => {
                    // The "current version" at this point must be a.
                    assert_eq!(seen_begin_output, false);
                    seen_begin_output = true;
                    assert_eq!(max.as_ref(), a);
                }
                M1PlanAction::Apply(span) | M1PlanAction::FF(span) => {
                    assert!(!span.is_empty());

                    // The span must NOT be in the max set.
                    assert!(!graph.frontier_contains_version(max.as_ref(), span.start));

                    // The cleared version must be a parent of this version.
                    assert!(graph.frontier_contains_frontier(&[span.start], cleared_version.as_ref()));

                    if let M1PlanAction::FF(_) = action {
                        graph.with_parents(span.start, |parents| {
                            assert_eq!(parents, current.as_ref());
                            assert_eq!(parents, max.as_ref());
                        });

                        // If we're fast forwarding, the cleared version comes too.
                        cleared_version.replace_with_1(span.last());
                        // And the current and max versions do too.
                        max.replace_with_1(span.last());
                        current.replace_with_1(span.last());
                    } else {
                        graph.with_parents(span.start, |parents| {
                            assert_eq!(parents, current.as_ref()); // Current == the new item's parents.
                            // And the span is a child of max.
                            assert!(graph.frontier_contains_frontier(max.as_ref(), parents));
                        });
                        max.advance(graph, *span);
                        current.advance(graph, *span);
                    }
                }
                M1PlanAction::Retreat(span) => {
                    assert!(!span.is_empty());

                    assert!(graph.frontier_contains_frontier(&[span.start], cleared_version.as_ref()));

                    // The span must be in the max set already - because we've visited this span already.
                    assert!(graph.frontier_contains_version(max.as_ref(), span.last()));
                    // And it must be in current too.
                    assert!(graph.frontier_contains_version(current.as_ref(), span.last()));

                    // We can't just retreat any range though. The span needs to be "at the end" of the
                    // current version. The last version of the span must be in the frontier.
                    assert!(current.0.iter().any(|v| *v == span.last()));

                    current.retreat(graph, *span);
                }
                M1PlanAction::Advance(span) => {
                    assert!(!span.is_empty());

                    assert!(graph.frontier_contains_frontier(&[span.start], cleared_version.as_ref()));

                    // The span must be in the max set already - because we've visited this span already.
                    assert!(graph.frontier_contains_version(max.as_ref(), span.last()));

                    // But the span must not be in the current. All the parents should be though.
                    assert!(!graph.frontier_contains_version(current.as_ref(), span.start));
                    graph.with_parents(span.start, |parents| {
                        assert!(graph.frontier_contains_frontier(current.as_ref(), parents));
                    });
                    current.advance(graph, *span);
                }
                M1PlanAction::Clear => {
                    // The current version is discarded when a clear operation happens, since we
                    // throw out the internal data structure.
                    cleared_version = max.clone();
                    current = max.clone();
                }
            }
        }

        let final_version = graph.find_dominators_2(a, b);
        assert_eq!(max, final_version);
    }

    pub(crate) fn dbg_print(&self) {
        let mut i = 0;
        for a in self.0.iter() {
            match a {
                M1PlanAction::Retreat(span) => {
                    println!("{i}: --- deactivate {:?}", span);
                }
                M1PlanAction::Advance(span) => {
                    println!("{i}: +++ reactivate {:?}", span);
                }
                M1PlanAction::Clear => {
                    println!("{i}: ========== CLEAR =========");
                }
                M1PlanAction::Apply(span) => {
                    println!("{i}: Add {:?}", span);
                }
                M1PlanAction::FF(span) => {
                    println!("{i}: FF {:?}", span);
                }
                M1PlanAction::BeginOutput => {
                    println!("{i}: ========== BEGIN OUTPUT =========");
                }
            }
            i += 1;
        }
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::causalgraph::graph::{Graph, GraphEntrySimple};
    use crate::causalgraph::graph::random_graphs::with_random_cgs;
    use crate::causalgraph::graph::tools::DiffFlag;
    use crate::Frontier;
    use crate::listmerge2::{ConflictGraphEntry, ConflictSubgraph};

    #[test]
    fn test_merge1_simple_graph() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 2.into(), parents: Frontier::new_1(0) },
        ]);

        let mut g = graph.make_conflict_graph_between(&[], &[1, 2]);
        // g.dbg_print();
        g.dbg_check();

        g.prepare();
        let critical_path: Vec<_> = g.entries.iter()
            .map(|e| e.state.critical_path)
            .collect();
        assert_eq!(&critical_path, &[true, false, false, true, true]);

        let plan = g.make_m1_plan();
        // dbg!(&plan);
        plan.dbg_check(g.base_version.as_ref(), &[], &[1, 2], &graph);
    }

    #[test]
    fn test_simple_graph_2() {
        // Same as above, but this time with an extra entry after the concurrent zone.
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 2.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 3.into(), parents: Frontier::from_sorted(&[1, 2]) },
        ]);

        let mut g = graph.make_conflict_graph_between(&[], &[3]);
        // g.dbg_print();

        g.dbg_check();

        g.prepare();
        let critical_path: Vec<_> = g.entries.iter()
            .map(|e| e.state.critical_path)
            .collect();

        // dbg!(critical_path);
        assert_eq!(&critical_path, &[true, true, false, false, true, true]);

        let plan = g.make_m1_plan();
        // plan.dbg_print();
        plan.dbg_check(g.base_version.as_ref(), &[], &[3], &graph);
    }

    #[test]
    fn fuzz_m1_plans() {
        with_random_cgs(3232, (100, 10), |(_i, _k), cg, frontiers| {
            // Iterate through the frontiers, and [root -> cg.version].
            for (_j, fs) in std::iter::once([Frontier::root(), cg.version.clone()].as_slice())
                .chain(frontiers.windows(2))
                .enumerate()
            {
                // println!("{_i} {_k} {_j}");

                let (a, b) = (fs[0].as_ref(), fs[1].as_ref());

                // println!("\n\n");
                // dbg!(&cg.graph);
                // println!("f: {:?} + {:?}", a, b);

                // Alternatively:
                // let plan = cg.graph.make_m1_plan(a, b);
                let mut subgraph = cg.graph.make_conflict_graph_between(a, b);
                subgraph.dbg_check();
                subgraph.dbg_check_conflicting(&cg.graph, a, b);

                let plan = subgraph.make_m1_plan();
                // subgraph.dbg_print();
                // plan.dbg_print();
                // dbg!(&plan);
                plan.dbg_check(subgraph.base_version.as_ref(), a, b, &cg.graph);
            }
        });
    }

}
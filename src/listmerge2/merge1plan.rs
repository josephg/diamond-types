//! This is a POC for what an action plan would look like using the current list merging algorithm
//! instead of the new one.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use bumpalo::collections::CollectIn;
use smallvec::{SmallVec, smallvec};
use rle::{AppendRle, HasLength, HasRleKey, MergableSpan};
use crate::{CausalGraph, DTRange, Frontier, LV};
use crate::causalgraph::graph::Graph;
use crate::listmerge2::{ConflictGraphEntry, ConflictSubgraph};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::list::ListOpLog;
use crate::list::op_metrics::ListOpMetrics;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum M1PlanAction {
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
pub struct M1Plan(pub Vec<M1PlanAction>);

type Metrics = RleVec<KVPair<ListOpMetrics>>;

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

    // DIRTY.
    // children: SmallVec<[usize; 2]>,

    cost_here: usize,
    subtree_cost: usize,
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
    }

    fn calc_costs(&mut self, children: &[SmallVec<[usize; 2]>], metrics: Option<&Metrics>) {
        // There's a tradeoff here. We can figure out the cost for each span using the operation
        // log, which looks up how many actual operations the span crosses. Doing so carries a
        // small but measurable improvement in merging performance because we can optimize the
        // children better. But it takes a little longer. Eh.
        if let Some(metrics) = metrics {
            let mut idx = self.base_version.0.iter().min().and_then(|&lv| {
                metrics.find_index(lv).ok()
            }).unwrap_or(0);

            for (i, e) in self.entries.iter_mut().enumerate().rev() {
                if e.span.is_empty() { continue; }
                while idx < metrics.0.len() && metrics[idx].end() <= e.span.start {
                    idx += 1;
                }
                // idx = metrics.find_index(e.span.start).unwrap();

                let start_idx = idx;
                let last = e.span.last();
                // while idx < metrics.0.len() && metrics[idx].end() <= last {
                //     idx += 1;
                // }
                idx = metrics.find_index(last).unwrap();

                e.state.cost_here = idx - start_idx + 1;
                // assert_eq!(e.state.cost_here, estimate_cost(e.span, metrics));

                if i == self.b_root {
                    e.state.cost_here += usize::MAX / 2;
                }
            }
        } else {
            for (i, e) in self.entries.iter_mut().enumerate().rev() {
                // Just use the span estimate.
                e.state.cost_here = e.span.len();
                if i == self.b_root {
                    e.state.cost_here += usize::MAX / 2;
                }
            }
        }

        // Ok, now go through and scan each child.

        // The philosophy here is this:
        // - When navigating the graph, we want to visit small subtrees before large subtrees. This
        //   makes navigation faster.
        // - To do that, we need to calculate the cost of each subtree in the graph. Then we'll sort
        //   the children indexes in that order, which will mean we iterate through the graph from
        //   smallest subtree to largest.

        // We want to go from lowest to highest spans, but the list is reversed
        // (highest to lowest), so we don't need to double-reverse it.
        // Each item in the queue is (index, uncounted).
        // Queue is defined here so we can reuse the queue's memory allocation each iteration.
        let mut queue: BinaryHeap<(usize, bool)> = BinaryHeap::new();

        for idx in 0..self.entries.len() { // From the latest in the graph to the earliest.
            // println!("\n\nIDX {idx}");
            // Pushing the parents instead of just this item. Eh.
            let mut aggregate_cost = self.entries[idx].state.cost_here;

            let ch = &children[idx];
            // dbg!(ch);
            if ch.len() == 1 {
                self.entries[idx].state.subtree_cost = aggregate_cost + self.entries[ch[0]].state.subtree_cost;
                continue;
            }

            let Some(&max_idx) = ch.iter().max_by_key(|i| self.entries[**i].state.cost_here) else {
                self.entries[idx].state.subtree_cost = aggregate_cost;
                continue; // The child list is empty. We have nothing to do here!
            };

            aggregate_cost += self.entries[max_idx].state.subtree_cost;

            // let mut queue: BinaryHeap<(usize, bool)> = ch.iter()
            //     .map(|&i| (i, i != max_idx))
            //     .collect();
            debug_assert!(queue.is_empty());
            queue.extend(ch.iter()
                .map(|&i| (i, i != max_idx)));

            let mut uncounted_remaining = ch.len() - 1;

            while let Some((i, mut uncounted)) = queue.pop() {
                // dbg!((i, uncounted, uncounted_remaining));
                if uncounted { uncounted_remaining -= 1; }

                while let Some((peek_i, peek_uncounted)) = queue.peek() {
                    if *peek_i == i {
                        if *peek_uncounted { uncounted_remaining -= 1; }
                        else { uncounted = false; }
                        queue.pop();
                    } else { break; }
                }

                if uncounted {
                    // Count it!
                    let e2 = &self.entries[i];
                    aggregate_cost += e2.state.cost_here;
                }

                // If we've counted everything, stop here.

                let ch = &children[i];
                if uncounted {
                    uncounted_remaining += ch.len();
                } else if uncounted_remaining == 0 { break; }
                // println!("cost += idx {i} : cost: {}", e2.state.cost_here);
                queue.extend(ch.iter().map(|&i| (i, uncounted)));
            }

            // println!("Aggregate {aggregate_cost}");
            self.entries[idx].state.subtree_cost = aggregate_cost;
            queue.clear();
        }
    }

    pub(super) fn m1_plan_2(&mut self, metrics: Option<&Metrics>) -> M1Plan {
        let mut actions = vec![];
        if self.entries.is_empty() {
            return M1Plan(actions);
        }

        let mut nonempty_spans_remaining = self.entries.iter()
            .filter(|e| !e.span.is_empty())
            .count();

        let mut a_spans_remaining = self.entries.iter()
            .filter(|e| !e.span.is_empty() && e.flag != DiffFlag::OnlyB)
            .count();

        if nonempty_spans_remaining == a_spans_remaining {
            // There's no spans with B. Just bail - no plan needed in this case.
            return M1Plan(actions);
        }

        let mut children: Vec<SmallVec<[usize; 2]>> = vec![smallvec![]; self.entries.len()];
        for (i, e) in self.entries.iter().enumerate() {
            for &p in e.parents.iter() {
                children[p].push(i);
            }
        }

        self.prepare();
        self.calc_costs(&children, metrics);
        // let rng = &mut rand::thread_rng();
        for c in children.iter_mut() {
            // Lowest cost to highest cost.
            c.sort_unstable_by_key(|&i| self.entries[i].state.subtree_cost);
            // c.shuffle(rng);
        }

        fn teleport(g: &ConflictSubgraph<M1EntryState>, actions: &mut Vec<M1PlanAction>, target_idx: usize, last_processed_after: bool, last_processed_idx: usize) {
            let mut advances: SmallVec<[DTRange; 2]> = smallvec![];
            let mut retreats: SmallVec<[DTRange; 2]> = smallvec![];
            g.diff_trace(last_processed_idx, last_processed_after, target_idx, |idx, flag| {
                let list = match flag {
                    DiffFlag::OnlyA => &mut retreats,
                    DiffFlag::OnlyB => &mut advances,
                    DiffFlag::Shared => { return; }
                };
                let span = g.entries[idx].span;
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
        }


        // self.dbg_print();
        // dbg!(&children);

        let mut activated = false;
        let mut dirty = false;

        // The last entry is the shared child.
        let mut current_idx = self.entries.len() - 1;
        debug_assert_eq!(self.entries[current_idx].flag, DiffFlag::Shared);
        let mut stack: Vec<usize> = vec![];

        let mut last_processed_after: bool = false;
        let mut last_processed_idx: usize = self.entries.len() - 1; // Might be cleaner to start this at None or something.

        // We'll dummy-visit the first item.
        let e = &mut self.entries[current_idx];
        debug_assert!(e.span.is_empty());
        debug_assert_eq!(e.state.visited, false);
        e.state.visited = true;
        // let e = &self.entries[current_idx];
        for &c in &children[current_idx] {
            self.entries[c].state.parents_satisfied += 1;
        }

        // Its awkward, but we need to traverse all the items which are marked as common or OnlyA
        // before we touch any items marked as OnlyB. Every time we see an OnlyB item, we'll push
        // it to this list. And then in "phase 2", we'll zip straight here and go from here.
        let mut b_children: SmallVec<[usize; 2]> = smallvec![];

        'outer: loop {
            let e = &mut self.entries[current_idx];
            // println!("Looping on {current_idx} / stack: {:?}", &stack);
            // println!("e children {:?}, next {}", &children[current_idx], e.state.next);

            if !e.state.visited {
                // println!("Visit {current_idx}");
                e.state.visited = true;

                // And drop &mut. I wish I could just say let e = &*e, but the borrowck doesn't
                // understand that.
                let e = &self.entries[current_idx];

                // Process this span.
                if !e.span.is_empty() {
                    if !activated && e.flag == DiffFlag::OnlyB {
                        activated = true;
                        actions.push(M1PlanAction::BeginOutput);
                    }

                    if e.state.critical_path {
                        // It doesn't make sense to FF in the A / common section.
                        debug_assert_eq!(e.flag, DiffFlag::OnlyB);

                        if dirty {
                            actions.push(M1PlanAction::Clear);
                            dirty = false;
                            // TODO: Consider also clearing the stack here. We won't be back.
                        }
                        actions.push_rle(M1PlanAction::FF(e.span));
                    } else {
                        // Note we only advance & retreat if the item is not on the critical path.
                        // If we're on the critical path, the clear operation will flush everything
                        // anyway.
                        teleport(self, &mut actions, current_idx, last_processed_after, last_processed_idx);
                        actions.push_rle(M1PlanAction::Apply(e.span));
                        dirty = true;
                    }

                    // We can stop as soon as we've processed all the spans.
                    nonempty_spans_remaining -= 1;
                    if nonempty_spans_remaining == 0 { break 'outer; } // break;

                    if e.flag != DiffFlag::OnlyB {
                        a_spans_remaining -= 1;
                    }

                    last_processed_after = true;
                    last_processed_idx = current_idx;
                }

                for c in &children[current_idx] {
                    self.entries[*c].state.parents_satisfied += 1;
                }
            }

            // Afterwards we'll try to go down to one of our children. Failing that, we'll go up
            // to the next item in the stack.

            let c = &mut children[current_idx];
            // println!("Children: {:?}", c);

            let e = &self.entries[current_idx];
            if e.state.next < c.len() {
                // Look for a child we can visit.
                for i in e.state.next..c.len() {
                    let next_idx = c[i];
                    let e2 = &self.entries[next_idx];
                    debug_assert_eq!(e2.state.visited, false);

                    // This is a merge, but we haven't covered all the merge's parents.
                    if e2.state.parents_satisfied != e2.parents.len() { continue; }

                    if a_spans_remaining > 0 && e2.flag == DiffFlag::OnlyB {
                        // We'll come back to this node later. More A stuff first!
                        b_children.push(next_idx);
                        continue;
                    }

                    // println!("Going down to visit {next_idx}");
                    // We went down. Visit this child.
                    let e = &mut self.entries[current_idx];
                    c.swap(e.state.next, i);
                    e.state.next += 1;

                    if e.state.next < c.len() { // We won't come back this way.
                        stack.push(current_idx);
                    }
                    current_idx = next_idx;

                    continue 'outer;
                }
            }

            // println!("No children viable. Going up.");

            // We couldn't go down any more with this item. Go up instead.
            if let Some(next_idx) = stack.pop() {
                current_idx = next_idx;
            } else if let Some(idx) = b_children.pop() {
                debug_assert_eq!(a_spans_remaining, 0);
                current_idx = idx;
                // println!("Popping b child {current_idx} / {:?}", b_children);
                // println!("{:?}", self.entries[current_idx]);

                // teleport(self, &mut actions, idx, last_processed_after, last_processed_idx);
                // last_processed_idx = current_idx;
                // last_processed_after = false;
            } else {
                // println!("spans remaining {}", nonempty_spans_remaining);
                // self.dbg_print();
                panic!("Should have stopped");
                // break;
            }

        }


        M1Plan(actions)
    }


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
            return (M1Plan(vec![]), a.into());
        }

        let mut sg = self.make_conflict_graph_between(a, b);
        (sg.make_m1_plan(), sg.base_version)
    }

    pub(crate) fn make_m1_plan_2(&self, metrics: Option<&Metrics>, a: &[LV], b: &[LV]) -> (M1Plan, Frontier) {
        if self.frontier_contains_frontier(a, b) {
            // Nothing to merge. Do nothing.
            return (M1Plan(vec![]), a.into());
        }

        let mut sg = self.make_conflict_graph_between(a, b);
        // sg.dbg_print();
        (sg.m1_plan_2(metrics), sg.base_version)
    }
}

impl M1Plan {
    pub(crate) fn dbg_check(&self, common_ancestor: &[LV], a: &[LV], b: &[LV], graph: &Graph) {
        if self.0.is_empty() {
            // It would be better to make this stricter, and require an empty plan if a contains b.
            assert!(graph.frontier_contains_frontier(a, b));
            return;
        }
        // if graph.frontier_contains_frontier(a, b) {
        //     // We shouldn't do anything in this case.
        //     assert!(self.0.is_empty());
        //     return;
        // }

        // dbg!(self, a, b);
        assert!(self.0.iter().filter(|&&a| a == M1PlanAction::BeginOutput).count() <= 1);

        let mut current: Frontier = common_ancestor.into();
        let mut max: Frontier = common_ancestor.into();
        let mut cleared_version: Frontier = common_ancestor.into();
        let mut started_output = false;

        for (_i, action) in self.0.iter().enumerate() {
            // println!("{_i}: {:?}", action);
            match action {
                M1PlanAction::BeginOutput => {
                    // The "current version" at this point must be a.
                    assert_eq!(started_output, false);
                    started_output = true;
                    assert_eq!(max.as_ref(), a);
                }
                M1PlanAction::Apply(span) | M1PlanAction::FF(span) => {
                    assert!(!span.is_empty());

                    if !started_output {
                        // Until we start output, spans should all be within a.
                        assert!(graph.frontier_contains_version(a, span.start));
                        // println!("fcv {:?} {}", a, span.start);
                    }

                    // The span must NOT be in the max set.
                    assert!(!graph.frontier_contains_version(max.as_ref(), span.start));

                    // The cleared version must be a parent of this version.
                    assert!(graph.frontier_contains_frontier(&[span.start], cleared_version.as_ref()));

                    if let M1PlanAction::FF(_) = action {
                        // FF doesn't make sense before we've started output.
                        // TODO: RE-enable this!
                        // assert_eq!(started_output, true);

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
    fn test_simple_graph_2_2() {
        // Same as above, but this time with an extra entry after the concurrent zone.
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: (2..4).into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 4.into(), parents: Frontier::from_sorted(&[1, 3]) },
        ]);

        // let mut g = graph.make_conflict_graph_between(&[], &[3]);
        let mut g = graph.make_conflict_graph_between(&[1], &[4]);
        let plan = g.m1_plan_2(None);
        plan.dbg_print();
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

    #[test]
    fn fuzz_m1_plans_2() {
        with_random_cgs(3232, (100, 10), |(_i, _k), cg, frontiers| {
        // with_random_cgs(2231, (100, 3), |(_i, _k), cg, frontiers| {
            // Iterate through the frontiers, and [root -> cg.version].
            for (_j, fs) in std::iter::once([Frontier::root(), cg.version.clone()].as_slice())
                .chain(frontiers.windows(2))
                .enumerate()
            {
                // println!("{_i}, {_k}, {_j}");
                // if _j != 0 { continue; }

                let (a, b) = (fs[0].as_ref(), fs[1].as_ref());

                // println!("\n\n");
                // if (_i, _k, _j) == (1, 2, 1) {
                //     println!("\n\n\nOOO");
                //
                //     #[cfg(feature = "dot_export")]
                //     cg.generate_dot_svg("dot.svg");
                // }
                // dbg!(&cg.graph);
                // println!("f: {:?} + {:?}", a, b);

                // Alternatively:
                // let plan = cg.graph.make_m1_plan(a, b);
                let mut subgraph = cg.graph.make_conflict_graph_between(a, b);
                subgraph.dbg_check();
                subgraph.dbg_check_conflicting(&cg.graph, a, b);

                // subgraph.dbg_print();
                let plan = subgraph.m1_plan_2(None);
                // plan.dbg_print();
                // dbg!(&plan);
                plan.dbg_check(subgraph.base_version.as_ref(), a, b, &cg.graph);
            }
        });
    }

}

// #[ignore]
// #[test]
// fn lite_bench() {
//     let bytes = std::fs::read(format!("benchmark_data/git-makefile.dt")).unwrap();
//     let oplog = ListOpLog::load_from(&bytes).unwrap();
//     let (plan, common) = oplog.make_plan_2();
//
//     for _i in 0..100 {
//         // oplog.checkout_tip();
//         oplog.checkout_tip_2(plan.clone(), common.as_ref());
//     }
// }

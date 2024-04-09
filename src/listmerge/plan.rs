//! This is a POC for what an action plan would look like using the current list merging algorithm
//! instead of the new one.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Not;
use bumpalo::collections::CollectIn;
use smallvec::{SmallVec, smallvec};
use rle::{AppendRle, HasLength, HasRleKey, MergableSpan};
use crate::{CausalGraph, DTRange, Frontier, LV};
use crate::causalgraph::graph::conflict_subgraph::{ConflictGraphEntry, ConflictSubgraph};
use crate::causalgraph::graph::Graph;
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

    child_base: usize,
    child_end: usize,

    new_cost_here: u128,
    new_cost_estimate: u32,
}


// struct SubgraphChildren(Vec<SmallVec<[usize; 2]>>);

type DiffTraceHeap = BinaryHeap<Reverse<(usize, DiffFlag)>>;

impl ConflictSubgraph<M1EntryState> {
    // #[inline(never)]
    // This method is adapted from the equivalent method in the causal graph code.
    // fn diff_trace<F: FnMut(DiffFlag, DTRange)>(&self, queue: &mut DiffTraceHeap, from_idx: usize, after: bool, to_idx: usize, mut visit: F) {
    fn diff_trace<F: FnMut(DiffFlag, DTRange)>(&self, queue: &mut DiffTraceHeap, from_idx: usize, to_idx: usize, mut visit: F) {
        use DiffFlag::*;
        // println!("T {from_idx}-{to_idx}");

        // Sorted highest to lowest.
        // let mut queue: BinaryHeap<Reverse<(usize, DiffFlag)>> = BinaryHeap::new();

        if from_idx != usize::MAX { // from_idx is usize::MAX at first. We start with nothing.
            // println!("{}", self.entries[to_idx].parents.as_ref() == &[from_idx]);
            queue.push(Reverse((from_idx, OnlyA)));
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
            // println!("  {idx} {:?} {:?}", flag, entry.span);

            if flag == Shared {
                // If there's only shared entries left, abort.
                // If the flag isn't shared, we're about to add a bunch more non-shared entries to
                // the queue.
                if queue.len() == num_shared_entries { break; }
                num_shared_entries += entry.parents.len();
            } else {
                visit(flag, entry.span);
            }

            // mark_run(containing_txn.span.start, idx, flag);
            for p_idx in entry.parents.iter() {
                queue.push(Reverse((*p_idx, flag)));
                // if flag == Shared { num_shared_entries += 1; }
            }
        }

        queue.clear();
    }

    // #[inline(never)]
    // This function does a BFS through the graph, setting critical_path.
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

    fn get_children<'a>(children: &'a [usize], e: &ConflictGraphEntry<M1EntryState>) -> &'a [usize] {
        &children[e.state.child_base..e.state.child_end]
    }
    fn get_children_mut<'a, 'b>(children: &'a mut [usize], e: &'b ConflictGraphEntry<M1EntryState>) -> &'a mut [usize] {
        &mut children[e.state.child_base..e.state.child_end]
    }

    // #[inline(never)]
    fn calc_costs_accurately(&mut self, children: &[usize], metrics: Option<&Metrics>) {
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

            // let ch = &children[idx];
            let ch = Self::get_children(children, &self.entries[idx]);
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

                // let ch = &children[i];
                let ch = Self::get_children(children, &self.entries[i]);
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

    // #[inline(never)]
    fn calc_costs_estimate(&mut self, children: &[usize], _metrics: Option<&Metrics>) {
        // There's a tradeoff here. We can figure out the cost for each span using the operation
        // log, which looks up how many actual operations the span crosses. Doing so carries a
        // small but measurable improvement in merging performance because we can optimize the
        // children better. But it takes a little longer. Eh.
        if false { //let Some(metrics) = metrics {
            // todo!()
        } else {
            let min_lv = self.base_version.0.first().copied().unwrap_or(0);
            let Some(max_lv) = self.entries
                .iter()
                .filter_map(|e| {
                    if e.span.is_empty() { None }
                    else { Some(e.span.end) }
                })
                .max() else { return; };

            // The plan is to have a 128 bit wide bit array where 1 bit is assigned each time any
            // item is visited with something in that slot.
            let w = max_lv - min_lv;
            let lv_per_bit = w.div_ceil(127);
            // dbg!(min_lv, max_lv, lv_per_bit);

            for (i, e) in self.entries.iter_mut().enumerate().rev() {
                // Just use the span estimate.
                if !e.span.is_empty() {
                    // This is kind of sloppy. What I want is 0b0000111100000 with 1 bits set
                    // between positions 0-127. At least 1 bit should always be set by this if the
                    // span is not empty.
                    let start = ((e.span.start - min_lv) / lv_per_bit) as u32;
                    let end = ((e.span.end - min_lv + lv_per_bit - 1) / lv_per_bit) as u32; // Basically, ceil.

                    e.state.new_cost_here = 1u128.wrapping_shl(end)
                        .wrapping_sub(1u128 << start);

                    if i == self.b_root {
                        e.state.new_cost_here |= 1u128 << 127;
                    } else {
                        e.state.new_cost_here &= (1u128 << 127).not();
                    }
                    // println!("{i} span: {:?} root {} cost {:#b}", e.span, i == self.b_root, e.state.new_cost_here);
                } else { e.state.new_cost_here = 0; }
            }
        }

        for i in 0..self.entries.len() { // From the latest in the graph to the earliest.
            let e = &self.entries[i];

            let mut cost = e.state.new_cost_here;
            // Include the cost of all children. We do this in order, so its recursive.
            for &c in Self::get_children(children, e) {
                cost |= self.entries[c].state.new_cost_here;
            }

            // println!("cost {i} = {cost:#b}");
            self.entries[i].state.new_cost_here = cost;

            // If the top bit is set, keep it in the cost estimate.
            self.entries[i].state.new_cost_estimate = cost.count_ones()
                + ((cost & (1u128 << 127)) >> (128-32)) as u32;
        }
    }

    pub(crate) fn make_m1_plan(mut self, metrics: Option<&Metrics>, allow_ff: bool) -> (M1Plan, Frontier) {
        let mut actions = vec![];
        if self.entries.is_empty() {
            return (M1Plan(actions), self.base_version);
        }

        let mut nonempty_spans_remaining = self.entries.iter()
            .filter(|e| !e.span.is_empty())
            .count();

        let mut a_spans_remaining = self.entries.iter()
            .filter(|e| !e.span.is_empty() && e.flag != DiffFlag::OnlyB)
            .count();

        if nonempty_spans_remaining == a_spans_remaining {
            // There's no spans with B. Just bail - no plan needed in this case.
            return (M1Plan(actions), self.base_version);
        }


        // This is horrible. Basically, I need to know the children for each entry. The
        // make_conflict_graph_between code could just calculate this directly, but I'm not using it
        // to do so because I'm an idiot. So instead, there's this vomit comet of code.
        //
        // Previously I used a vec<smallvec<>>, but packing everything into a single big array is
        // slightly faster.
        let mut num_children: Vec<usize> = vec![0; self.entries.len()];
        let mut total_children = 0;
        for e in self.entries.iter() {
            for &p in e.parents.iter() {
                num_children[p] += 1;
                total_children += 1;
            }
        }

        // let mut children: Vec<usize> = Vec::with_capacity(total_children);
        let mut children: Vec<usize> = vec![0; total_children];
        let mut n = 0;
        for (i, e) in self.entries.iter_mut().enumerate() {
            e.state.child_base = n;
            n += num_children[i];
            e.state.child_end = n;
            num_children[i] = 0;
        }
        for (i, e) in self.entries.iter().enumerate() {
            for &p in e.parents.iter() {
                // children[p].push(i);
                let e2 = &self.entries[p];
                debug_assert!(e2.state.child_base + num_children[p] < e2.state.child_end);
                children[e2.state.child_base + num_children[p]] = i;
                num_children[p] += 1;
            }
        }

        self.prepare();

        // We can scan & reorder the graph here in order to traverse it more efficiently. This
        // isn't always a win. Sometimes its faster to spend the cycles processing a slightly
        // inefficient graph, than try and optimise the graph too much first.
        const OPT_EXACT: bool = false;
        if OPT_EXACT {
            // self.calc_costs(&children, None);
            self.calc_costs_accurately(&children, metrics);
            // let rng = &mut rand::thread_rng();

            for e in self.entries.iter() {
                Self::get_children_mut(&mut children, e)
                    .sort_unstable_by_key(|&i| self.entries[i].state.subtree_cost);
            }
        }

        // This second algorithm is much faster, but less accurate.
        const OPT_ESTIMATE: bool = true;
        if OPT_ESTIMATE {
            self.calc_costs_estimate(&children, metrics);

            for e in self.entries.iter() {
                Self::get_children_mut(&mut children, e)
                    .sort_unstable_by_key(|&i| self.entries[i].state.new_cost_estimate);
            }
        }

        let mut queue = DiffTraceHeap::new();

        fn teleport(queue: &mut DiffTraceHeap, g: &ConflictSubgraph<M1EntryState>, actions: &mut Vec<M1PlanAction>, to_idx: usize, from_idx: usize) {
            // Fast case.
            let to_entry_parents = &g.entries[to_idx].parents;
            if to_entry_parents.as_ref() == &[from_idx] { return; }

            // Retreats must appear first in the action list. We'll cache any advance actions in
            // this vec, and push retreats to the action list immediately.
            let mut advances: SmallVec<[DTRange; 4]> = smallvec![];

            g.diff_trace(queue, from_idx, to_idx, |flag, span: DTRange| {
                if !span.is_empty() {
                    match flag {
                        DiffFlag::OnlyA => {
                            // There's so much reversing happening here. We'll visit spans in
                            // reverse order, but ::Retreat's MergeSpan also understands that they
                            // are reversed. It all cancels out and works out ok.
                            // println!("T R {:?}", span);
                            actions.push_rle(M1PlanAction::Retreat(span));
                        },
                        DiffFlag::OnlyB => { advances.push_reversed_rle(span); },
                        DiffFlag::Shared => {}
                    }
                }
            });

            if !advances.is_empty() {
                // .rev() here because diff visits everything in reverse order.
                // println!("T A {:?}", advances);
                actions.extend(advances.into_iter().rev().map(M1PlanAction::Advance));
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

        // let mut last_processed_after: bool = false;
        // let mut last_processed_idx: usize = self.entries.len() - 1; // Might be cleaner to start this at None or something.
        let mut last_processed_idx = usize::MAX;

        // We'll dummy-visit the first item.
        let e = &mut self.entries[current_idx];
        debug_assert!(e.span.is_empty());
        debug_assert_eq!(e.state.visited, false);
        e.state.visited = true;
        // let e = &self.entries[current_idx];
        for &c in Self::get_children(&children, &self.entries[current_idx]) {
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

                    if e.state.critical_path && allow_ff {
                        // It doesn't make sense to FF in the A / common section.
                        debug_assert_eq!(e.flag, DiffFlag::OnlyB);

                        if dirty {
                            actions.push(M1PlanAction::Clear);
                            dirty = false;
                            // TODO: Consider also clearing the stack here. We won't be back.
                        }
                        // push_rle is just as correct here, but the rle merging case seems to never
                        // happen.
                        actions.push(M1PlanAction::FF(e.span));
                    } else {
                        // Note we only advance & retreat if the item is not on the critical path.
                        // If we're on the critical path, the clear operation will flush everything
                        // anyway.
                        // println!("TELE {last_processed_idx} -> {current_idx}");
                        // teleport(&mut queue, &self, &mut actions, current_idx, if last_processed_after { last_processed_idx } else { usize::MAX });
                        teleport(&mut queue, &self, &mut actions, current_idx, last_processed_idx);
                        actions.push_rle(M1PlanAction::Apply(e.span));
                        dirty = true;
                    }

                    // We can stop as soon as we've processed all the spans.
                    nonempty_spans_remaining -= 1;
                    if nonempty_spans_remaining == 0 { break 'outer; } // break;

                    if e.flag != DiffFlag::OnlyB {
                        a_spans_remaining -= 1;
                    }

                    // last_processed_after = true;
                    last_processed_idx = current_idx;
                }

                for &c in Self::get_children(&children, &self.entries[current_idx]) {
                    self.entries[c].state.parents_satisfied += 1;
                }
            }

            let e = &self.entries[current_idx];
            // Afterwards we'll try to go down to one of our children. Failing that, we'll go up
            // to the next item in the stack.

            // let c = &mut children[current_idx];
            let c = Self::get_children_mut(&mut children, e);
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
                // let e = &mut self.entries[current_idx];
                // if e.parents.len() == 1 && last_processed_idx == current_idx {
                //     println!("UP {current_idx} -> {next_idx} span {:?} p {:?}", e.span, e.parents);
                //     if current_idx == 3 {
                //         println!();
                //     }
                //     if !e.span.is_empty() { actions.push_rle(M1PlanAction::Retreat(e.span)); }
                //     last_processed_idx = next_idx;
                // }
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

        (M1Plan(actions), self.base_version)
    }
}

impl Graph {
    pub(crate) fn make_m1_plan(&self, metrics: Option<&Metrics>, a: &[LV], b: &[LV], allow_ff: bool) -> (M1Plan, Frontier) {
        if self.frontier_contains_frontier(a, b) {
            // Nothing to merge. Do nothing.
            return (M1Plan(vec![]), a.into());
        }

        let sg = self.make_conflict_graph_between(a, b);
        // sg.dbg_print();
        sg.make_m1_plan(metrics, allow_ff)
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

    #[test]
    fn test_merge1_simple_graph() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 2.into(), parents: Frontier::new_1(0) },
        ]);

        let g = graph.make_conflict_graph_between(&[], &[1, 2]);
        // g.dbg_print();
        g.dbg_check();

        let (plan, base_version) = g.make_m1_plan(None, true);
        // dbg!(&plan);
        plan.dbg_check(base_version.as_ref(), &[], &[1, 2], &graph);
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

        let g = graph.make_conflict_graph_between(&[], &[3]);
        // g.dbg_print();
        g.dbg_check();

        let (plan, base_version) = g.make_m1_plan(None, true);
        // plan.dbg_print();
        plan.dbg_check(base_version.as_ref(), &[], &[3], &graph);
    }

    #[test]
    fn fuzz_m1_plans() {
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
                let subgraph = cg.graph.make_conflict_graph_between(a, b);
                subgraph.dbg_check();
                subgraph.dbg_check_conflicting(&cg.graph, a, b);

                // subgraph.dbg_print();
                let (plan, base_version) = subgraph.make_m1_plan(None, true);
                // plan.dbg_print();
                // dbg!(&plan);
                plan.dbg_check(base_version.as_ref(), a, b, &cg.graph);

                // And check that if we don't allow fast-forwarding the plan still works.
                let subgraph = cg.graph.make_conflict_graph_between(a, b);
                let (plan2, base_version) = subgraph.make_m1_plan(None, false);
                plan2.dbg_check(base_version.as_ref(), a, b, &cg.graph);
            }
        });
    }

}

#[ignore]
#[test]
fn lite_bench() {
    let bytes = std::fs::read(format!("benchmark_data/clownschool.dt")).unwrap();
    // let bytes = std::fs::read(format!("benchmark_data/git-makefile.dt")).unwrap();
    // let bytes = std::fs::read(format!("benchmark_data/node_nodecc.dt")).unwrap();
    // let bytes = std::fs::read(format!("benchmark_data/friendsforever.dt")).unwrap();
    let oplog = ListOpLog::load_from(&bytes).unwrap();
    let (plan, _common) = oplog.cg.graph.make_m1_plan(None, &[], oplog.cg.version.as_ref(), true);
    // let (plan, _common) = oplog.cg.graph.make_m1_plan(None, &[], &[113], true);
    // let (plan, _common) = oplog.cg.graph.make_m1_plan(None, &[], &[10000], true);

    // dbg!(&plan);

    // for _i in 0..100 {
    //     // oplog.checkout_tip();
    //     oplog.checkout_tip_2(plan.clone(), common.as_ref());
    // }

    dbg!(plan.0.len());
}

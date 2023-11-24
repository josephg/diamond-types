use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::take;
use bumpalo::Bump;
use smallvec::{SmallVec, smallvec};
use rle::{HasLength, MergableSpan};
use crate::{DTRange, Frontier, LV};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::{ConflictSubgraph, Index};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ApplyAction {
    pub span: DTRange,
    pub index: Index,
    pub update_other_indexes: SmallVec<[Index; 2]>,
    pub insert_items: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum MergePlanAction {
    Apply(ApplyAction),
    ClearInsertedItems,
    ForkIndex { src: Index, dest: Index },
    DropIndex(Index),
    // MaxIndex(Index, SmallVec<[Index; 2]>),
    MaxIndex(Index, SmallVec<[Index; 2]>),
}
use MergePlanAction::*;
use crate::causalgraph::graph::Graph;
use crate::frontier::is_sorted_slice;


#[derive(Debug, Clone)]
pub(crate) struct MergePlan {
    pub actions: Vec<MergePlanAction>,
    pub indexes_used: usize,
}

#[derive(Debug, Clone, Default)]
pub(super) struct EntryState {
    index: Option<Index>, // Primary index for merges / backup index for forks.
    next: usize, // Starts at 0. 0..parents.len() is where we scan parents, then we scan children.
    // emitted_this_span: bool,
    children_needing_index: usize, // For forks

    visited: bool,
    merge_max_with: SmallVec<[usize; 2]>,
}


// fn borrow_2<T>(slice: &mut [T], a_idx: usize, b_idx: usize) -> (&mut T, &mut T) {
//     // Utterly awful.
//     assert_ne!(a_idx, b_idx);
//     let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };
//     // a<b.
//     let (start, end) = slice.split_at_mut(b_idx);
//     let a = &mut start[a_idx];
//     let b = &mut end[0];
//
//     return (a, b);
// }

// fn emit(action: MergePlanAction) {
//     // dbg!(action);
//     println!("Action {:?}", action)
// }

impl ConflictSubgraph<EntryState> {
    // This method is adapted from the equivalent method in the causal graph code.
    fn diff_trace<F: FnMut(usize)>(&self, idx: usize, mut visit: F) {
        assert!(self.entries[idx].parents.len() >= 2);

        use DiffFlag::*;
        // Sorted highest to lowest.
        let mut queue: BinaryHeap<Reverse<(usize, DiffFlag)>> = self.entries[idx].parents
            .iter().enumerate()
            .map(|(idx, e)| {
                Reverse((*e, if idx == 0 { OnlyA } else { OnlyB }))
            })
            .collect();

        // dbg!(&queue);

        // let (first, rest) = self.ops[idx].parents.split_first().unwrap();
        // queue.push(Reverse((*first, OnlyA)));
        // for b_ord in rest {
        //     queue.push(Reverse((*b_ord, OnlyB)));
        // }

        let mut num_shared_entries = 0;

        while let Some(Reverse((idx, mut flag))) = queue.pop() {
            if flag == Shared { num_shared_entries -= 1; }

            // dbg!((ord, flag));
            while let Some(Reverse((peek_idx, peek_flag))) = queue.peek() {
                if *peek_idx != idx { break; } // Normal case.
                else {
                    // 3 cases if peek_flag != flag. We set flag = Shared in all cases.
                    if *peek_flag != flag { flag = Shared; }
                    if *peek_flag == Shared { num_shared_entries -= 1; }
                    queue.pop();
                }
            }

            // let entry = &self.ops[idx];
            // if flag == OnlyA && *peek_flag == OnlyB && entry.state.visited {
            //     println!("Need to MAX!");
            //     visit(idx);
            //     // entry.state.children_needing_index += 1;
            //     // self.ops[a].state.merge_max_with.push(idx);
            // }

            let entry = &self.entries[idx];
            if flag == OnlyB && entry.state.visited {
                // Oops!
                // println!("Need to MAX!");
                visit(idx);
                flag = Shared;
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

    pub(crate) fn calc_children_needing_index(&mut self) {
        // TODO: Merge this with plan_first_pass.
        // Iterating with an explicit index to keep the borrowck happy.
        for i in 0..self.entries.len() {
            let entry = &self.entries[i];
            if let Some(&first_parent) = entry.parents.first() {
                self.entries[first_parent].state.children_needing_index += 1;
            }
        }

        if !self.entries.is_empty() {
            self.entries[0].state.children_needing_index += 1;
        }
    }

    fn plan_first_pass(&mut self, b: &Bump) {
        use bumpalo::collections::Vec as BumpVec;

        if self.entries.is_empty() { return; }
        let mut stack = vec![];
        let mut current_idx = 0;

        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
        enum Movement {
            Up(usize),
            Down
        }
        use Movement::*;
        let mut last_direction = Up(usize::MAX);

        // let mut maxes: BumpVec<(usize, BumpVec<usize>)> = bumpalo::vec![in b];
        let mut maxes: BumpVec<(usize, SmallVec<[usize; 2]>)> = bumpalo::vec![in b];

        loop {
            // println!("Visiting idx {current_idx} from {:?}", last_direction);
            let mut e = &mut self.entries[current_idx];

            let next_direction = 'block: {
                // *** Deal with the entry's parents and merging logic ***
                let parents_len = e.parents.len();

                if e.state.next == 0 || e.state.next < parents_len {
                    assert_eq!(e.state.visited, false);
                    if parents_len == 0 {
                        // Hack. Needed so if the root item has multiple children, we don't try to
                        // use the root index with all of them.
                        e.state.next = 1;
                    } else {
                        // Visit the next parent.
                        match last_direction {
                            Up(_) => { // We came up. Keep going up to our first parent.
                                assert_eq!(e.state.next, 0);
                                // We can't be at the root because parents_len > 0.
                                break 'block Up(e.parents[0]);
                            }

                            Down => {
                                if e.state.next == 0 && parents_len >= 2 {
                                    // println!("CHECK! {current_idx}");

                                    // We've come down to a merge from the first branch. Ideally,
                                    // at this point the algorithm below would add the current index
                                    // to the active index set and keep going. But for this merge,
                                    // we need to check that that will actually work.
                                    self.diff_trace(current_idx, |i| {
                                        // println!("{current_idx} -> {i}");

                                        if let Some(v) = maxes.last_mut()
                                            .and_then(|(idx, v)| if *idx == current_idx { Some(v) } else { None })
                                        {
                                            v.push(i);
                                        } else {
                                            // maxes.push((current_idx, bumpalo::vec![in b; i]));
                                            maxes.push((current_idx, smallvec![i]));
                                        }
                                    });
                                    e = &mut self.entries[current_idx];
                                }

                                // While processing the parents, we increment next when the parent is
                                // *complete*. This is unlike the children, where we increment next when
                                // the command is issued to descend to that child.
                                e.state.next += 1;

                                if e.state.next < parents_len { // Not the last.
                                    // Visit the next parent.
                                    break 'block Up(e.parents[e.state.next]);
                                }
                                // Otherwise flow down.
                            }
                        }
                    };

                    // We reach here, we've visited all of this node's parents for the first time.
                    // println!("Visited idx {current_idx}");
                    e.state.visited = true;
                }

                Down
            };

            last_direction = next_direction;
            match next_direction {
                Up(next) => {
                    // Save current and index_wanted for this node. We need to restore both later
                    // when we go back down the tree (since we descend based on the ascent).
                    stack.push(current_idx);
                    current_idx = next;
                }
                Down => {
                    if let Some(next) = stack.pop() {
                        current_idx = next;
                    } else { break; };
                }
            }
        }

        assert_eq!(last_direction, Down);

        for op in self.entries.iter_mut() {
            assert!(op.state.visited);
            op.state.visited = false;
            op.state.next = 0;
        }

        // dbg!(&maxes.len());
        for (i, v) in maxes {
            for vv in v.iter() {
                self.entries[*vv].state.children_needing_index += 1;
            }
            self.entries[i].state.merge_max_with = v;
        }
    }

    /// The merge execution plan is essentially a fancy traversal of the causal graph.
    ///
    /// The plan:
    ///
    /// - Starts from the root (or some shared point in time)
    /// - Visits all operations at least once
    /// - Tracks a set of indexes
    pub(super) fn make_plan(&mut self) -> MergePlan {
        let bump = Bump::new();

        if self.entries.is_empty() {
            return MergePlan { actions: vec![], indexes_used: 0 };
        }

        self.plan_first_pass(&bump);
        self.calc_children_needing_index();

        // TODO: Use a bump alo for all of these vectors.
        let mut actions = vec![];

        let mut stack = vec![];
        let mut index_stack: Vec<Index> = vec![];
        // let mut indexes_state: Vec<Option<Frontier>> = vec![Some(Frontier::root())];

        let g = &mut self.entries;

        // Up from some child, or down with an index.
        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
        enum Movement {
            Up { next: usize, needs_index: bool },
            Down(Option<Index>)
        }
        use Movement::*;

        let mut current_idx = 0;
        let mut index_wanted = true;
        let mut last_direction = Up {
            next: usize::MAX,
            needs_index: false
        };

        let root_index = 0;
        let mut next_index = 1;
        let mut free_index_stack: SmallVec<[Index; 8]> = smallvec![];

        // Concurrency tracks the number of extra, unexplored concurrent paths to this one as we
        // go down.
        let mut concurrency: usize = 0;
        let mut list_contains_content = false;

        loop {
            let mut e = &mut g[current_idx];

            // println!("idx {current_idx} / Last {:?} / span here {}", last_direction, e.span.start);
            // dbg!(&last_direction, &e);

            // The entry is essentially in one of 4 different states:
            // 1. We haven't visited all the parents yet
            // 2. We've visited all the parents, but none of the children (do span and flow down)
            // 3. We haven't visited all the children yet
            // 4. We're totally done.

            let next_direction = 'block: {
                // *** Deal with the entry's parents and merging logic ***
                let parents_len = e.parents.len();

                let index = if e.state.next == 0 || e.state.next < parents_len {
                    // We hit this branch in the if if we haven't yet processed this span.
                    // Check if there are more parents to visit (and if so visit them), and if not,
                    // process the span now and flow on.

                    let index = if parents_len == 0 {
                        // Hack. Needed so if the root item has multiple children, we don't try to
                        // use the root index with all of them.
                        e.state.next = 1;
                        root_index
                    } else {
                        // Visit the next parent.
                        match last_direction {
                            Up { .. } => { // We came up. Keep going up to our first parent.
                                assert_eq!(e.state.next, 0);
                                // We can't be at the root because parents_len > 0.
                                break 'block Up {
                                    next: e.parents[0],
                                    needs_index: true
                                };
                            }

                            Down(down_index) => {
                                // While processing the parents, we increment next when the parent is
                                // *complete*. This is unlike the children, where we increment next when
                                // the command is issued to descend to that child.
                                e.state.next += 1;

                                if parents_len == 1 {
                                    // No merge. Just use the index from our parent and continue to
                                    // children.
                                    down_index.unwrap()
                                } else {
                                    // parents_len >= 2. Iterate through all children and merge.
                                    if e.state.next == 1 { // First time down.
                                        assert!(e.state.index.is_none());

                                        // Store the merger's primary index
                                        let down_index = down_index.unwrap();
                                        e.state.index = Some(down_index);
                                        // println!("Pushing index {down_index}");
                                        debug_assert_eq!(false, index_stack.contains(&down_index));
                                        index_stack.push(down_index);

                                        if !e.state.merge_max_with.is_empty() {
                                            // println!("MERGE WITH {:?}", e.state.merge_max_with);
                                            let mut merge_with_indexes: SmallVec<[usize; 2]> = smallvec![];
                                            let mut drop: SmallVec<[usize; 2]> = smallvec![];

                                            for i in take(&mut e.state.merge_max_with) {
                                                let e2 = &mut g[i];
                                                let index = e2.state.index.unwrap();
                                                merge_with_indexes.push(index);
                                                e2.state.children_needing_index -= 1;
                                                if e2.state.children_needing_index == 0 {
                                                    drop.push(index);
                                                }
                                            }
                                            e = &mut g[current_idx]; // needed for borrowck.
                                            // println!("Emit {:?}", MaxIndex(down_index, merge_with_indexes.clone()));
                                            actions.push(MaxIndex(down_index, merge_with_indexes));

                                            for i in drop {
                                                // println!("Emit {:?}", DropIndex(i));
                                                actions.push(DropIndex(i));
                                                free_index_stack.push(i);
                                            }
                                        }

                                        // We are guaranteed to go up to another parent now.
                                    } else { // Not the first.
                                        // We've just come from one of the parents.
                                        debug_assert!(e.state.index.is_some());
                                        debug_assert!(down_index.is_none());
                                    }

                                    if e.state.next < parents_len { // Not the last.
                                        // Visit the next parent.
                                        break 'block Up {
                                            next: e.parents[e.state.next],
                                            needs_index: false
                                        };
                                    } else { // The last.
                                        // We've visited all the parents. Flow the logic down.
                                        let primary_index = e.state.index.take().unwrap();
                                        // println!("Popping index {primary_index}");
                                        let s = index_stack.pop();
                                        assert_eq!(Some(primary_index), s);

                                        // Once a merge has happened, we can sometimes just clear
                                        // the list completely of all content - since nothing that
                                        // happens next can possibly be relevant.
                                        if list_contains_content && index_stack.is_empty() && concurrency == 0 {
                                            actions.push(ClearInsertedItems);
                                            list_contains_content = false;
                                        }

                                        primary_index
                                    }
                                }
                            }
                        }
                    };


                    // We reach here if any/all merges are complete for the first time. Since this
                    // is the first time coming through here, process the span if we need to.
                    if !e.span.is_empty() {
                        // println!("Emit {:?}", Apply(ApplyAction {
                        //     span: e.span,
                        //     index,
                        //     update_other_indexes: index_stack.iter().copied().collect(),
                        //     insert_items: concurrency > 0,
                        // }));

                        let mut update_other_indexes: SmallVec<[Index; 2]> = index_stack.iter().copied().collect();
                        update_other_indexes.sort_unstable();

                        actions.push(Apply(ApplyAction {
                            span: e.span,
                            index,
                            update_other_indexes,
                            insert_items: concurrency > 0,
                        }));
                        list_contains_content = true;
                    }

                    // This logic feels wrong, but I think its right.
                    if e.num_children >= 2 { concurrency += e.num_children - 1; }

                    if index_wanted {
                        index
                    } else {
                        if e.state.children_needing_index == 0 {
                            assert_eq!(index_wanted, false);
                            // println!("Emit {:?}", DropIndex(index));
                            actions.push(DropIndex(index));
                            // indexes_state[index].take().unwrap();
                            free_index_stack.push(index);
                            // dbg!(&indexes_state);
                            // println!("Drop index {index}");
                        } else {
                            // Our next child doesn't care about this index anyway. As an optimization,
                            // backup the current index and we'll send nothing below.
                            e.state.index = Some(index);
                        }
                        break 'block Down(None);
                    }
                } else { // Later children.
                    // If we hit this branch, we've already processed the all the parents and the
                    // span (if any). And we've descended to the first child. So we're just looking
                    // at subsequent children at this point. Usually we just punt straight back down
                    // when we process them.
                    concurrency -= 1;

                    // Grab an index from the backup index if we need one.
                    if index_wanted {
                        e.state.index.take().unwrap()
                    } else {
                        // I didn't even want your stupid index anyway.
                        //
                        // This happens when we've already been visited, and a merge's non-primary
                        // arm visits this node.
                        break 'block Down(None);
                    }
                };

                debug_assert_eq!(index_wanted, true);
                debug_assert!(e.state.children_needing_index > 0);
                e.state.children_needing_index -= 1;

                // Check if we need to backup the index for subsequent children
                if e.state.children_needing_index > 0 {
                    // Yep. Back 'er up.
                    let backup_index = free_index_stack.pop().unwrap_or_else(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });
                    e.state.index = Some(backup_index);
                    // println!("Emit {:?}", ForkIndex(index, backup_index));
                    actions.push(ForkIndex {
                        src: index,
                        dest: backup_index
                    });
                    // dbg!(&indexes_state);
                    // if indexes_state.len() == backup_index {
                    //     indexes_state.resize(backup_index + 1, None);
                    // }
                    // assert!(indexes_state[backup_index].is_none());
                    // indexes_state[backup_index] = indexes_state[index].clone();
                }

                Down(Some(index))
            };

            last_direction = next_direction;
            match next_direction {
                Up { next, needs_index } => {
                    // Save current and index_wanted for this node. We need to restore both later
                    // when we go back down the tree (since we descend based on the ascent).
                    stack.push((current_idx, index_wanted));
                    current_idx = next;
                    index_wanted = needs_index;
                }
                Down(_index) => {
                    if let Some((next, next_index_wanted)) = stack.pop() {
                        current_idx = next;
                        index_wanted = next_index_wanted;
                    } else { break; };
                }
            }
        }

        assert_eq!(concurrency, 0);
        assert_eq!(last_direction, Down(Some(0)));

        for op in self.entries.iter() {
            assert_eq!(op.state.children_needing_index, 0);
        }

        // println!("Done {:?}", last_direction);
        MergePlan {
            actions,
            indexes_used: next_index,
        }
    }
}

impl MergePlan {
    pub(crate) fn dbg_print(&self) {
        println!("Plan with {} steps, using {} indexes", self.actions.len(), self.indexes_used);
        for (i, action) in self.actions.iter().enumerate() {
            println!("{i}: {:?}", action);
        }
    }

    pub(crate) fn dbg_check(&self, _deep: bool) {
        if self.actions.is_empty() { return; }

        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
        enum IndexState {
            Free,
            InUse {
                used: bool,
                forked_at: usize,
            },
        }

        let mut index_state = vec![IndexState::Free; self.indexes_used];
        index_state[0] = IndexState::InUse { used: false, forked_at: usize::MAX };

        for (i, action) in self.actions.iter().enumerate() {
            // dbg!(&action);
            match action {
                ForkIndex { src: _, dest: new_index } => {
                    index_state[*new_index] = IndexState::InUse { used: false, forked_at: i };
                }

                Apply(apply_action) => {
                    match &mut index_state[apply_action.index] {
                        IndexState::Free => { panic!("Invalid plan: Using dropped index"); }
                        IndexState::InUse { used, .. } => {
                            *used = true;
                        }
                    }

                    // The other indexes slice must be sorted.
                    assert!(is_sorted_slice::<true, _>(apply_action.update_other_indexes.as_slice()));
                }

                DropIndex(index) => {
                    match index_state[*index] {
                        IndexState::Free => { panic!("Invalid plan: Dropping freed index"); }
                        IndexState::InUse { used: false, forked_at } => {
                            println!("Redundant fork! {index} forked at {forked_at} / dropped at {i}");
                        },
                        _ => {}
                    }
                    index_state[*index] = IndexState::Free;
                }
                MaxIndex(into, from) => {
                    match &mut index_state[*into] {
                        IndexState::Free => { panic!("Invalid plan: MaxIndex into an unused index"); }
                        _ => {},
                    }

                    for f in from {
                        match &mut index_state[*f] {
                            IndexState::Free => { panic!("Invalid plan: MaxIndex using an unused index"); }
                            IndexState::InUse { used, .. } => {
                                *used = true;
                            },
                        }
                    }
                }
                _ => {}
            }
        }

        let (first, rest) = index_state.split_first().unwrap();
        assert_ne!(first, &IndexState::Free);
        assert!(rest.iter().all(|s| s == &IndexState::Free));
    }

    pub(crate) fn simulate_plan(&self, graph: &Graph, start_frontier: &[LV]) {
        if self.indexes_used == 0 {
            assert!(self.actions.is_empty());
            return;
        }

        let mut index_state = vec![None; self.indexes_used];
        index_state[0] = Some(Frontier::from(start_frontier));

        for action in self.actions.iter() {
            match action {
                Apply(ApplyAction { span, index, update_other_indexes, insert_items: _ }) => {
                    if !span.is_empty() {
                        let actual_parents = graph.parents_at_version(span.start);

                        // The designated index must exactly match the parents of the span we're applying.
                        assert_eq!(index_state[*index].as_ref(), Some(&actual_parents), "Parents of {} do not match index", span.start);
                        index_state[*index] = Some(Frontier::new_1(span.last()));

                        for idx in update_other_indexes.iter() {
                            let frontier = index_state[*idx].as_mut().unwrap();

                            for p in actual_parents.iter() {
                                assert!(graph.frontier_contains_version(frontier.as_ref(), *p));
                            }
                            frontier.advance_by_known_run(actual_parents.as_ref(), *span);
                        }
                    }

                    // TODO: Check insert_items.
                }
                ForkIndex { src: i1, dest: i2 } => {
                    let state = index_state[*i1].clone();
                    assert!(state.is_some());
                    assert!(index_state[*i2].is_none());
                    index_state[*i2] = state;
                }
                DropIndex(index) => {
                    let state = index_state[*index].take();
                    assert!(state.is_some());
                }
                ClearInsertedItems => {} // TODO!
                MaxIndex(dest, src) => {
                    assert!(!src.contains(dest));

                    let mut new_f = index_state[*dest].take().unwrap();
                    for s in src {
                        let other_f = index_state[*s].as_ref().unwrap().as_ref();
                        assert_ne!(new_f.as_ref(), other_f);
                        new_f.merge_union(other_f, graph);
                    }
                    index_state[*dest] = Some(new_f);
                }
            }
        }
    }

    pub(crate) fn cost_estimate<F: Fn(DTRange) -> usize>(&self, estimate_fn: F) {
        let mut cost = 0;
        let mut forks = 0;
        let mut maxes = 0;

        for action in self.actions.iter() {
            match action {
                Apply(apply) => {
                    // cost += apply.span.len() * (1 + apply.update_other_indexes.len())
                    // cost += apply.span.len();// * (1 + apply.update_other_indexes.len())
                    cost += estimate_fn(apply.span);
                    // cost += estimate_fn(apply.span) * (1 + apply.update_other_indexes.len());
                }
                ForkIndex { src: _, dest: _ } => { forks += 1; }
                MaxIndex(_, with) => { maxes += with.len(); }
                ClearInsertedItems => {}
                DropIndex(_) => {}
            }
        }
        println!("spans: {cost}, forks: {forks} maxes {maxes}");
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::causalgraph::graph::GraphEntrySimple;
    use crate::listmerge2::ConflictGraphEntry;
    use super::*;

    #[test]
    fn test_trivial_graphs() {
        let mut g = ConflictSubgraph { entries: vec![], base_version: Frontier::root(), a_root: usize::MAX, b_root: usize::MAX };

        g.dbg_check();
        let plan = g.make_plan();
        assert!(plan.actions.is_empty());

        let _graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() }
        ]);

        let mut g = ConflictSubgraph {
            entries: vec![
                ConflictGraphEntry {
                    parents: smallvec![],
                    span: (0..1).into(),
                    num_children: 0,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
            ],
            base_version: Frontier::root(), a_root: usize::MAX, b_root: usize::MAX
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.dbg_print();
    }

    #[test]
    fn test_simple_graph() {
        let _graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: 2.into(), parents: Frontier::new_1(0) },
        ]);

        let mut g = ConflictSubgraph {
            entries: vec![
                ConflictGraphEntry {
                    parents: smallvec![1, 2],
                    span: (0..0).into(),
                    num_children: 0,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry {
                    parents: smallvec![3],
                    span: 2.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry {
                    parents: smallvec![3],
                    span: 1.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry {
                    parents: smallvec![],
                    span: 0.into(),
                    num_children: 2,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
            ],
            base_version: Frontier::root(), a_root: 3, b_root: 0
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.dbg_print();
    }

    #[test]
    fn diamonds() {
        let mut g: ConflictSubgraph<EntryState> = ConflictSubgraph {
            entries: vec![
                ConflictGraphEntry { // 0 Y
                    parents: smallvec![1, 2],
                    span: Default::default(),
                    num_children: 0,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 1 ACY
                    parents: smallvec![6],
                    span: 4.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 2 D
                    parents: smallvec![3],
                    span: 3.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 3 DY
                    parents: smallvec![4, 5],
                    span: Default::default(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 4 AD
                    parents: smallvec![6],
                    span: 2.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 5 XBD
                    parents: smallvec![7],
                    span: 1.into(),
                    num_children: 1,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 6 XA -> A
                    parents: smallvec![7],
                    span: 0.into(),
                    num_children: 2,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
                ConflictGraphEntry { // 7 X
                    parents: smallvec![],
                    span: Default::default(),
                    num_children: 2,
                    state: Default::default(),
                    flag: DiffFlag::OnlyB,
                },
            ],
            base_version: Frontier::root(), a_root: 7, b_root: 0
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.dbg_check(true);
        // plan.dbg_print();
    }

    //
    // #[test]
    // fn order_matters() {
    //     // This graph has some bad traversals, which won't actually work properly if the order
    //     // isn't carefully figured out.
    //     let mut g = ConflictSubgraph {
    //         ops: vec![
    //             ActionGraphEntry::Ops { // 0 A
    //                 parent: usize::MAX,
    //                 span: Default::default(),
    //                 num_children: 3,
    //                 state: Default::default(),
    //             },
    //             ActionGraphEntry::Ops { // 1 ABD
    //                 parent: 0,
    //                 span: (0..1).into(),
    //                 num_children: 1,
    //                 state: Default::default(),
    //             },
    //             ActionGraphEntry::Ops { // 2 AXE
    //                 parent: 0,
    //                 span: (1..2).into(),
    //                 num_children: 1,
    //                 state: Default::default(),
    //             },
    //             ActionGraphEntry::Ops { // 3 AC
    //                 parent: 0,
    //                 span: (2..3).into(),
    //                 num_children: 2,
    //                 state: Default::default(),
    //             },
    //
    //             ActionGraphEntry::Merge { // 4 D
    //                 parents: smallvec![1,3],
    //                 state: Default::default(),
    //             },
    //             ActionGraphEntry::Merge { // 5 E
    //                 parents: smallvec![2,3],
    //                 state: Default::default(),
    //             },
    //             ActionGraphEntry::Merge { // 6 F
    //                 parents: smallvec![4,5],
    //                 state: Default::default(),
    //             },
    //         ],
    //         last: 6,
    //     };
    //
    //     g.dbg_check();
    //     g.make_plan();
    // }
}

// Action ForkIndex(0, 1)
// Action Apply(ApplyAction { span: T 0..1, measured_in: 0, updating_other_indexes: [] }) // XA -> 0
// Action ForkIndex(0, 2)
// Action Apply(ApplyAction { span: T 2..3, measured_in: 0, updating_other_indexes: [] }) // ACY -> 0
// Action Apply(ApplyAction { span: T 1..2, measured_in: 1, updating_other_indexes: [0] }) // XBD -> 1 {0}
// Action Apply(ApplyAction { span: T 3..4, measured_in: 2, updating_other_indexes: [0, 1] }) // AD -> 2 {0, 1}
// Action DropIndex(2)
// Action Apply(ApplyAction { span: T 4..5, measured_in: 1, updating_other_indexes: [0] }) // DY -> 1 {0}
// Action DropIndex(1)
// Action Apply(ApplyAction { span: T 5..6, measured_in: 0, updating_other_indexes: [] }) // YZ -> 0

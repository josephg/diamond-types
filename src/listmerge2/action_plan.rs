use smallvec::{SmallVec, smallvec};
use rle::MergableSpan;
use crate::{DTRange, Frontier};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::Index;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ApplyAction {
    span: DTRange,
    index: Index,
    update_other_indexes: SmallVec<[Index; 2]>,
    insert_items: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum MergePlanAction {
    Apply(ApplyAction),
    ClearInsertedItems,
    ForkIndex(Index, Index),
    DropIndex(Index),
}
use MergePlanAction::*;


#[derive(Debug, Clone)]
pub(crate) struct MergePlan {
    pub actions: Vec<MergePlanAction>,
    pub indexes_used: usize,
}

#[derive(Debug, Clone, Default)]
pub(super) struct EntryState {
    index: Option<Index>, // Primary index / backup index.
    next: usize, // Starts at 0. 0..parents.len() is where we scan parents, then we scan children.
    // emitted_this_span: bool,
    children_needing_index: usize,
}

#[derive(Debug, Clone)]
pub(super) struct ActionGraphEntry {
    pub parents: SmallVec<[usize; 2]>, // 2+ items.
    pub span: DTRange,
    pub num_children: usize,
    pub state: EntryState,
}

#[derive(Debug, Clone)]
pub(super) struct ConflictSubgraph {
    pub ops: Vec<ActionGraphEntry>,
    pub last: usize,
}


// fn emit(action: MergePlanAction) {
//     // dbg!(action);
//     println!("Action {:?}", action)
// }

impl ConflictSubgraph {
    pub(crate) fn dbg_check(&self) {
        // Things that should be true:
        // - ROOT is referenced exactly once
        // - The last item is the only one without children
        // - num_children is correct

        if self.ops.is_empty() {
            // This is a bit arbitrary.
            assert_eq!(self.last, usize::MAX);
            return;
        }

        // Check root is referenced once
        let root_nodes = self.ops.iter()
            .filter(|e| e.parents.is_empty());
        assert_eq!(root_nodes.count(), 1);

        for (idx, e) in self.ops.iter().enumerate() {
            // println!("{idx}: {:?}", e);
            // println!("contained by {:#?}", self.ops.iter()
            //     .filter(|e| e.parents.contains(&idx))
            //     .collect::<Vec<_>>());

            // Check num_children is correct.
            let contain_me = self.ops.iter()
                .filter(|e| e.parents.contains(&idx));
            assert_eq!(contain_me.count(), e.num_children);

            // Each entry should either have non-zero parents or have operations.
            assert!(!e.span.is_empty() || e.parents.len() != 1);

            if e.num_children == 0 {
                assert_eq!(idx, self.last);
            }
        }

        assert_eq!(self.ops[self.last].num_children, 0);
    }

    pub(crate) fn calc_children_needing_index(&mut self) {
        // Iterating with an explicit index to keep the borrowck happy.
        for i in 0..self.ops.len() {
            let entry = &self.ops[i];
            if let Some(&first_parent) = entry.parents.first() {
                self.ops[first_parent].state.children_needing_index += 1;
            }
        }

        if self.last != usize::MAX {
            self.ops[self.last].state.children_needing_index += 1;
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
        if self.ops.is_empty() {
            return MergePlan { actions: vec![], indexes_used: 0 };
        }

        self.calc_children_needing_index();

        let mut actions = vec![];

        let mut stack = vec![];
        let mut index_stack: Vec<Index> = vec![];

        let g = &mut self.ops;

        // Up from some child, or down with an index.
        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
        enum Movement {
            Up {
                next: usize,
                needs_index: bool
            },
            Down(Option<Index>)
        }
        use Movement::*;

        let mut current = self.last;
        let mut index_wanted = true;
        let mut last_direction = Up {
            next: usize::MAX,
            needs_index: false
        };

        impl Movement {
            fn is_up(&self) -> bool {
                match self {
                    Up { next: _, needs_index: _ } => true,
                    Down(_) => false,
                }
            }
        }

        let root_index = 0;
        let mut next_index = 1;
        let mut free_index_stack: SmallVec<[Index; 8]> = smallvec![];

        // Concurrency tracks the number of extra, unexplored concurrent paths to this one as we
        // go down.
        let mut concurrency: usize = 0;

        loop {
            let e = &mut g[current];

            // println!("Last {:?} / current {} idx {current}", last_direction, e.span.start);
            // dbg!(&last_direction, &e);

            // The entry is essentially in one of 4 different states:
            // 1. We haven't visited all the parents yet
            // 2. We've visited all the parents, but none of the children (do span and flow down)
            // 3. We haven't visited all the children yet
            // 4. We're totally done.

            let next_direction = 'block: {

                // *** Deal with the entry's parents and merging logic ***
                let parents_len = e.parents.len();

                // #[derive(PartialEq, Eq, Clone, Copy)]
                // enum State {
                //     FirstChild(Index),
                //     LaterChild(Option<Index>)
                // }
                // use State::*;

                let (index, first) = if parents_len == 0 && e.state.next == 0 {
                    e.state.next = 1; // Needed so if the first item has multiple children, we don't try to use the root with all of them.
                    (root_index, true)
                    // FirstChild(root_index)
                } else if e.state.next < parents_len {
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
                                // FirstChild(down_index.unwrap())
                                (down_index.unwrap(), true)
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

                                    // We are guaranteed to go up to another parent now.
                                } else { // All but the first.
                                    // We've just come from one of the parents.
                                    debug_assert!(e.state.index.is_some());
                                    debug_assert!(down_index.is_none());
                                }

                                if e.state.next < parents_len { // All but the last.
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
                                    // FirstChild(primary_index)
                                    (primary_index, true)
                                }
                            }
                        }
                    }
                } else { // e.state.next > parents_len.

                    // I hate this else block here, but I can't figure out a nice way to remove it.

                    // We visit the first child by hitting the above case and the logic flows down.
                    // Subsequent children hit *this* case.

                    // The index stores a backup index. Take it.
                    concurrency -= 1;
                    if index_wanted {
                        (e.state.index.take().unwrap(), false)
                    } else {
                        // I didn't even want your stupid index anyway.
                        //
                        // This happens when we've already been visited, and a merge's non-primary
                        // arm visits this node.
                        break 'block Down(None);
                    }
                };

                // We're done dealing with the parents. Process the span if this is the first time
                // continuing to the children.
                if first {
                    if !e.span.is_empty() {
                        actions.push(Apply(ApplyAction {
                            span: e.span,
                            index,
                            update_other_indexes: index_stack.iter().copied().collect(),
                            insert_items: concurrency > 0,
                        }));
                    }

                    // This logic feels wrong..
                    if e.num_children >= 2 { concurrency += e.num_children - 1; }

                    if !index_wanted {
                        if e.state.children_needing_index == 0 {
                            assert_eq!(index_wanted, false);
                            // TODO: Maybe drop the index here and go down None instead?

                            actions.push(DropIndex(index));
                            free_index_stack.push(index);
                            // println!("Drop index {index}");
                        } else {
                            // Our next child doesn't care about this index anyway. As an optimization,
                            // backup the current index and we'll send nothing below.
                            e.state.index = Some(index);
                        }
                        break 'block Down(None);
                    }
                }

                debug_assert_eq!(index_wanted, true);
                e.state.children_needing_index -= 1;

                if e.state.children_needing_index > 0 {
                    // More children need an index after this. Back it up.
                    let backup_index = free_index_stack.pop().unwrap_or_else(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });
                    // println!("Forking {index} -> {backup_index}");
                    e.state.index = Some(backup_index);
                    actions.push(ForkIndex(index, backup_index));
                }

                Down(Some(index))
            };

            // dbg!(&next_step);

            last_direction = next_direction;
            match next_direction {
                Up { next, needs_index } => {
                    stack.push((current, index_wanted));
                    current = next;
                    index_wanted = needs_index;
                }
                Down(_index) => {
                    if let Some((next, next_index_wanted)) = stack.pop() {
                        current = next;
                        index_wanted = next_index_wanted;
                    } else { break; };
                }
            }
        }

        assert_eq!(concurrency, 0);
        assert_eq!(last_direction, Down(Some(0)));

        for op in self.ops.iter() {
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
    pub(crate) fn print_plan(&self) {
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
                ForkIndex(_, new_index) => {
                    index_state[*new_index] = IndexState::InUse { used: false, forked_at: i };
                }

                Apply(apply_action) => {
                    match &mut index_state[apply_action.index] {
                        IndexState::Free => { panic!("Invalid plan: Using dropped index"); }
                        IndexState::InUse { used, .. } => {
                            *used = true;
                        }
                    }
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
                _ => {}
            }
        }

        let (first, rest) = index_state.split_first().unwrap();
        assert_ne!(first, &IndexState::Free);
        assert!(rest.iter().all(|s| s == &IndexState::Free));
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use super::*;

    #[test]
    fn test_trivial_graphs() {
        let mut g = ConflictSubgraph {
            ops: vec![],
            last: usize::MAX,
        };

        g.dbg_check();
        let plan = g.make_plan();
        assert!(plan.actions.is_empty());


        let mut g = ConflictSubgraph {
            ops: vec![
                ActionGraphEntry {
                    parents: smallvec![],
                    span: (0..1).into(),
                    num_children: 0,
                    state: Default::default(),
                },
            ],
            last: 0,
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.print_plan();
    }

    #[test]
    fn test_simple_graph() {
        let mut g = ConflictSubgraph {
            ops: vec![
                ActionGraphEntry {
                    parents: smallvec![],
                    span: (0..1).into(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry {
                    parents: smallvec![0],
                    span: (1..2).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry {
                    parents: smallvec![0],
                    span: (2..3).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry {
                    parents: smallvec![1, 2],
                    span: (0..0).into(),
                    num_children: 0,
                    state: Default::default(),
                }
            ],
            last: 3,
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.print_plan();
    }

    #[test]
    fn diamonds() {
        let mut g = ConflictSubgraph {
            ops: vec![
                ActionGraphEntry { // 0 X
                    parents: smallvec![],
                    span: Default::default(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry { // 1 XA -> A
                    parents: smallvec![0],
                    span: (0..1).into(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry { // 2 XBD
                    parents: smallvec![0],
                    span: (1..2).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry { // 3 AD
                    parents: smallvec![1],
                    span: (2..3).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry { // 4 D, DY
                    parents: smallvec![2, 3],
                    span: (4..5).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry { // 5 ACY
                    parents: smallvec![1],
                    span: (3..4).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry { // 6 Y
                    parents: smallvec![4, 5],
                    span: Default::default(),
                    num_children: 0,
                    state: Default::default(),
                },
            ],
            last: 6,
        };

        g.dbg_check();
        let plan = g.make_plan();
        plan.dbg_check(true);
        plan.print_plan();
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

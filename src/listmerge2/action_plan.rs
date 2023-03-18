use smallvec::{SmallVec, smallvec};
use crate::{DTRange, Frontier};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::Index;

#[derive(Debug, Clone)]
struct ApplyAction {
    span: DTRange,
    measured_in: Index,
    updating_other_indexes: SmallVec<[Index; 2]>,
    insert_items: bool,
}

#[derive(Debug, Clone)]
enum MergePlanAction {
    Apply(ApplyAction),
    DiscardInserts(DTRange),
    ForkIndex(Index, Index),
    DropIndex(Index),
}

#[derive(Debug, Clone, Default)]
pub(super) struct EntryState {
    index: Option<Index>, // Primary index / backup index.
    next_parent_idx: usize,
    next_child: usize,
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


fn emit(action: MergePlanAction) {
    // dbg!(action);
    println!("Action {:?}", action)
}

impl ConflictSubgraph {
    pub(crate) fn dbg_check(&self) {
        // Things that should be true:
        // - ROOT is referenced exactly once
        // - The last item is the only one without children
        // - num_children is correct

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

            if e.num_children == 0 {
                assert_eq!(idx, self.last);
            }
        }

        assert_eq!(self.ops[self.last].num_children, 0);
    }


    /// The merge execution plan is essentially a fancy traversal of the causal graph.
    ///
    /// The plan:
    ///
    /// - Starts from the root (or some shared point in time)
    /// - Visits all operations at least once
    /// - Tracks a set of indexes
    pub(super) fn make_plan(&mut self) {
        if self.ops.is_empty() { return; }

        let mut stack = vec![];
        let mut index_stack = vec![];

        let g = &mut self.ops;

        // Up from some child, or down with an index.
        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
        enum Direction { Up(usize), Down(Index) }
        use Direction::*;

        let mut current = self.last;
        let mut last_direction = Up(usize::MAX);

        impl Direction {
            fn is_up(&self) -> bool {
                match self {
                    Up(_) => true,
                    Down(_) => false,
                }
            }
        }

        let root_index = 0;
        // let mut current_index = None;
        let mut next_index = 1;
        let mut free_index_stack: SmallVec<[Index; 8]> = smallvec![];
        let mut concurrency: usize = 1;

        loop {
            let e = &mut g[current];

            // dbg!(&last_direction, &e);

            // The entry is essentially in one of 4 different states:
            // 1. We haven't visited all the parents yet
            // 2. We've visited all the parents, but none of the children (do span and flow down)
            // 3. We haven't visited all the children yet
            // 4. We're totally done.

            let next_direction = 'block: {
                let parents_len = e.parents.len();

                let index = if parents_len == 0 && e.state.next_child == 0 {
                    root_index
                } else if e.state.next_parent_idx < parents_len {
                    // Visit the next parent.
                    if let Down(down_index) = last_direction {
                        e.state.next_parent_idx += 1;

                        if parents_len >= 2 {
                            if e.state.index.is_none() {
                                // Store the primary index
                                e.state.index = Some(down_index);
                                debug_assert_eq!(false, index_stack.contains(&down_index));
                                index_stack.push(down_index);
                            } else {
                                // We've just come from one of the parents.
                                emit(MergePlanAction::DropIndex(down_index));
                                free_index_stack.push(down_index);
                            }
                        }

                        if e.state.next_parent_idx < parents_len {
                            // Visit the next parent.
                            break 'block Up(e.parents[e.state.next_parent_idx]);
                        } else {
                            // We've visited all the parents. Continue down using this index.
                            if let Some(primary_index) = e.state.index {
                                let s = index_stack.pop();
                                assert_eq!(Some(primary_index), s);
                                primary_index
                            } else {
                                down_index
                            }
                        }
                    } else { // We came up. Hit the first parent.
                        assert_eq!(e.state.next_parent_idx, 0);
                        // We can't be at the root because parents_len > next_parent_idx.
                        break 'block Up(e.parents[0]);
                    }
                } else { // e.state.next_parent_idx == parents_len
                    // To hit this state, we must not have visited all the children yet. (Or there
                    // are no children).

                    // The index stores a backup index. Take it.
                    e.state.index.take().unwrap()
                };

                // Go down to the next child.

                if e.state.next_child == 0 {
                    if !e.span.is_empty() {
                        emit(MergePlanAction::Apply(ApplyAction {
                            span: e.span,
                            measured_in: index,
                            updating_other_indexes: index_stack.iter().copied().collect(),
                            insert_items: concurrency > 1,
                        }));
                    }

                    // This logic feels wrong..
                    if e.num_children > 0 { concurrency += e.num_children - 1; }
                } else {
                    concurrency -= 1;
                }

                debug_assert!(e.num_children == 0 || e.state.next_child < e.num_children);

                // If we'll be visited again, backup the index.
                if e.state.next_child + 1 < e.num_children {
                    // let backup_index = next_index;
                    // next_index += 1;
                    let backup_index = free_index_stack.pop().unwrap_or_else(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });

                    e.state.index = Some(backup_index);
                    emit(MergePlanAction::ForkIndex(index, backup_index));
                }

                e.state.next_child += 1;
                break 'block Down(index);
            };

            // dbg!(&next_step);

            last_direction = next_direction;
            match next_direction {
                Up(next) => {
                    stack.push(current);
                    current = next;
                }
                Down(_index) => {
                    if let Some(next) = stack.pop() {
                        current = next;
                    } else { break; };
                }
            }
        }

        assert_eq!(concurrency, 1);
        assert_eq!(last_direction, Down(0));

        println!("Done {:?}", last_direction);
    }
}


#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use super::*;

    #[test]
    fn test_trivial_graph() {
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
        g.make_plan();
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
        g.make_plan();
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
        g.make_plan();
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

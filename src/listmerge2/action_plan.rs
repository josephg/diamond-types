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
pub(super) struct MergeState {
    primary_index: Option<Index>,
    // path_complete: SmallVec<[bool; 8]>
    next_parent_idx: usize,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ForkState {
    parent_visited: bool,
    children_visited: usize,
    backup_index: Option<Index>,
}

#[derive(Debug, Clone)]
pub(super) enum ActionGraphEntry {
    Merge {
        parents: SmallVec<[usize; 2]>, // 2+ items.
        state: MergeState,
    },
    Ops {
        parent: usize,
        span: DTRange,
        num_children: usize,
        state: ForkState,
    },
}

#[derive(Debug, Clone)]
pub(super) struct ConflictSubgraph {
    ops: Vec<ActionGraphEntry>,
    // first: usize, // Maybe always the last thing?
    last: usize,
}


fn emit(action: MergePlanAction) {
    // dbg!(action);
    println!("Action {:?}", action)
}

impl ConflictSubgraph {
    fn dbg_check(&self) {
        // Things that should be true:
        // - ROOT is referenced exactly once
        // - The last item is the only one without children
        // - num_children is correct
        // - merge entries have 2+ items

        fn has_item(e: &ActionGraphEntry, item: usize) -> bool {
            match e {
                ActionGraphEntry::Merge { parents, .. } => parents.contains(&item),
                ActionGraphEntry::Ops { parent, .. } => *parent == item
            }
        }

        // Check root is referenced once
        let root_nodes = self.ops.iter()
            .filter(|e| has_item(*e, usize::MAX));
        assert_eq!(root_nodes.count(), 1);

        for (idx, e) in self.ops.iter().enumerate() {
            match e {
                ActionGraphEntry::Merge { parents, .. } => {
                    // All merges merge 2 or more items.
                    assert!(parents.len() >= 2);
                    assert!(!parents.contains(&usize::MAX));

                    // And (this is a slow one) merges should be referenced exactly once.
                    let contain_me = self.ops.iter()
                        .filter(|e| has_item(*e, idx));
                    let expected_references = if self.last == idx { 0 } else { 1 };
                    assert_eq!(contain_me.count(), expected_references);
                }
                ActionGraphEntry::Ops { num_children, .. } => {
                    // Check num_children is correct.
                    let contain_me = self.ops.iter()
                        .filter(|e| has_item(*e, idx));
                    assert_eq!(contain_me.count(), *num_children);

                    if *num_children == 0 {
                        assert_eq!(idx, self.last);
                    }
                }
            }
        }
    }


    /// The merge execution plan is essentially a fancy traversal of the causal graph.
    ///
    /// The plan:
    ///
    /// - Starts from the root (or some shared point in time)
    /// - Visits all operations at least once
    /// - Tracks a set of indexes
    pub(super) fn make_plan(&mut self) {
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
            // Work around an intellij bug.
            if false { break; }


            // println!("At node {current} / Dir {:?} concurrency {concurrency}", last_direction);
            assert!(concurrency >= 1);
            // println!("index stack {:?}", index_stack);

            // The traversal is complex because we're essentially doing a postfix traversal from the
            // *last* edit up through the tree. And then, as we *leave* each node in the traversal
            // (going back down the CG again), actions are emitted.

            // I suspect there's a nicer way to factorize this code, somehow. A recursive solution might
            // be nice, but I don't want to make it vulnerable to stack smashing attacks.

            let next_direction = match &mut g[current] {
                ActionGraphEntry::Ops { parent, span, num_children, state } => 'block: {
                    // Split nodes always visit their children in the order that the split node itself
                    // is visited. They're like a mirror.

                    // The way the index should work is this:
                    // 1. The split node gets visited for the first time
                    // 2. We visit the parent, and we get visited again coming *down* with an index.
                    //    (Or the parent is ROOT and we just use root_index).
                    // 3. If we'll be visited again, the index is backed up and used next time.
                    if !state.parent_visited {
                        // This is a bit of a hack. The first time the split entry is visited will be
                        // from below, and we need to make sure we visit the split's parent on the way
                        // up.
                        assert!(last_direction.is_up());
                        // stack.push(current);

                        state.parent_visited = true;

                        // The first time we're visited, ignore all of this and just head up to the
                        // parent.
                        break 'block Up(*parent);
                    }

                    let index = if let Down(index) = last_direction {
                        // First time traversing down.
                        if !span.is_empty() {
                            emit(MergePlanAction::Apply(ApplyAction {
                                span: *span,
                                measured_in: index,
                                updating_other_indexes: index_stack.iter().copied().collect(),
                                insert_items: concurrency > 1,
                            }));
                        }

                        // This logic feels wrong..
                        if *num_children > 0 { concurrency += *num_children - 1; }

                        index
                    } else {
                        concurrency -= 1;
                        state.backup_index.take().unwrap()
                    };

                    // We're going to travel down to one of our children, to whichever child called us.
                    assert!(*num_children == 0 || state.children_visited < *num_children);
                    assert!(state.backup_index.is_none());

                    // If we'll be visited again, backup the index.
                    if state.children_visited + 1 < *num_children {
                        // let backup_index = next_index;
                        // next_index += 1;
                        let backup_index = free_index_stack.pop().unwrap_or_else(|| {
                            let index = next_index;
                            next_index += 1;
                            index
                        });

                        state.backup_index = Some(backup_index);
                        emit(MergePlanAction::ForkIndex(index, backup_index));
                    }

                    state.children_visited += 1;
                    // dbg!(&state);
                    Down(index)
                }

                ActionGraphEntry::Merge { parents, state } => {
                    // A merge node only has 1 child. We'll get an Up event exactly once, and 1 down
                    // event for every item in parents - but only after we go up to those nodes.

                    // A merge node decides on the order it traverses its children, but for simplicity
                    // we do it in parents[0..] order.
                    debug_assert!(parents.len() >= 2);
                    assert!(state.next_parent_idx < parents.len());

                    if let Down(index) = last_direction {
                        // Mark the direction we came in from as complete.
                        state.next_parent_idx += 1;

                        if let Some(primary_index) = state.primary_index {
                            if index != primary_index {
                                // When we're done we'll use primary_index. Everything else can be
                                // dropped.
                                emit(MergePlanAction::DropIndex(index));
                                free_index_stack.push(index);
                            }
                        } else {
                            // The first index we encounter going down *is* our primary index.
                            state.primary_index = Some(index);

                            if parents.len() >= 2 {
                                assert_eq!(false, index_stack.contains(&index));
                                // println!("Pushing index stack {index}");
                                index_stack.push(index);
                            }
                        }
                        // concurrency -= 1;
                    } else {
                        // We came from below. This happens the first time this node is visited.
                        assert!(state.primary_index.is_none());
                        assert_eq!(state.next_parent_idx, 0);
                        // concurrency += parents.len() - 1;
                    }

                    if state.next_parent_idx < parents.len() {
                        // Go up and scan the next parent.
                        Up(parents[state.next_parent_idx])
                    } else {
                        // We've merged all our parents into primary_index. Continue down!
                        let primary_index = state.primary_index.unwrap();

                        // Remove the index from the index_stack.
                        if parents.len() >= 2 {
                            let s = index_stack.pop();
                            assert_eq!(Some(primary_index), s);
                        }

                        // concurrency += 1;

                        // And go down, since we're done here.
                        Down(primary_index)
                    }
                }

            };

            // dbg!(&next_step);

            last_direction = next_direction;
            match next_direction {
                Up(next) => {
                    if next == usize::MAX {
                        // We're at the root. This should only happen once. Don't go anywhere, but
                        // bounce back down.
                        last_direction = Down(root_index);
                    } else {
                        stack.push(current);
                        current = next;
                    }
                }
                Down(_index) => {
                    let Some(next) = stack.pop() else { break; };
                    current = next;
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
                ActionGraphEntry::Ops {
                    // parents: smallvec![],
                    parent: usize::MAX,
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
                ActionGraphEntry::Ops {
                    parent: usize::MAX,
                    span: (0..1).into(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops {
                    parent: 0,
                    span: (1..2).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops {
                    parent: 0,
                    span: (2..3).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Merge {
                    parents: smallvec![1, 2],
                    state: Default::default(),
                }
            ],
            // first: 0,
            last: 3,
        };

        g.dbg_check();
        g.make_plan();
    }

    #[test]
    fn diamonds() {
        let mut g = ConflictSubgraph {
            ops: vec![
                ActionGraphEntry::Ops { // 0 X
                    parent: usize::MAX,
                    span: Default::default(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 1 XA -> A
                    parent: 0,
                    span: (0..1).into(),
                    num_children: 2,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 2 XBD
                    parent: 0,
                    span: (1..2).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 3 AD
                    parent: 1,
                    span: (2..3).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Merge { // 4 D
                    parents: smallvec![2, 3],
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 5 ACY
                    parent: 1,
                    span: (3..4).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 6 DY
                    parent: 4,
                    span: (4..5).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Merge { // 7 Y
                    parents: smallvec![5, 6],
                    state: Default::default(),
                },
            ],
            last: 7,
        };

        g.dbg_check();
        g.make_plan();
    }

    #[test]
    fn order_matters() {
        // This graph has some bad traversals, which won't actually work properly if the order
        // isn't carefully figured out.
        let mut g = ConflictSubgraph {
            ops: vec![
                ActionGraphEntry::Ops { // 0 A
                    parent: usize::MAX,
                    span: Default::default(),
                    num_children: 3,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 1 ABD
                    parent: 0,
                    span: (0..1).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 2 AXE
                    parent: 0,
                    span: (1..2).into(),
                    num_children: 1,
                    state: Default::default(),
                },
                ActionGraphEntry::Ops { // 3 AC
                    parent: 0,
                    span: (2..3).into(),
                    num_children: 2,
                    state: Default::default(),
                },

                ActionGraphEntry::Merge { // 4 D
                    parents: smallvec![1,3],
                    state: Default::default(),
                },
                ActionGraphEntry::Merge { // 5 E
                    parents: smallvec![2,3],
                    state: Default::default(),
                },
                ActionGraphEntry::Merge { // 6 F
                    parents: smallvec![4,5],
                    state: Default::default(),
                },
            ],
            last: 6,
        };

        g.dbg_check();
        g.make_plan();
    }
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

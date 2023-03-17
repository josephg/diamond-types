use smallvec::{SmallVec, smallvec};
use crate::{DTRange, Frontier};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::Index;

#[derive(Debug, Clone)]
struct ApplyAction {
    span: DTRange,
    // insert_items: bool,
    measured_in: Index,
    updating_other_indexes: SmallVec<[Index; 2]>,
}

#[derive(Debug, Clone)]
enum MergePlanAction {
    Apply(ApplyAction),
    DiscardInserts(DTRange),
    ForkIndex(Index, Index),
    DropIndex(Index),
}

#[derive(Debug, Clone, Default)]
struct SplitState {
    parent_visited: bool,
    // next_child: usize,
    children_visited: usize,
    // next_child_idx: usize, // Index into the split's children.
    // fwd_index: Index, // Unset at first.
    backup_index: Option<Index>,
}

#[derive(Debug, Clone, Default)]
struct MergeState {
    primary_index: Option<Index>,
    // path_complete: SmallVec<[bool; 8]>
    next_parent_idx: usize,
}

#[derive(Debug, Clone)]
enum GraphEntry {
    Merge {
        parents: SmallVec<[usize; 2]>, // Could have 0 or 1 items.
        span: DTRange,
        // child: usize,
        state: MergeState,
    },
    Split {
        parent: usize,
        // children: SmallVec<[usize; 2]>,
        num_children: usize,
        state: SplitState,
    },
}

#[derive(Debug, Clone)]
struct ConflictSubgraph {
    ops: Vec<GraphEntry>,
    // first: usize, // Maybe always the last thing?
    last: usize,
}


fn emit(action: MergePlanAction) {
    // dbg!(action);
    println!("Action {:?}", action)
}


/// The merge execution plan is essentially a fancy traversal of the causal graph.
///
/// The plan:
///
/// - Starts from the root (or some shared point in time)
/// - Visits all operations at least once
/// - Tracks a set of indexes
fn make_plan(subgraph: &mut ConflictSubgraph) {
    let mut stack = vec![];
    let mut index_stack = vec![];

    let g = &mut subgraph.ops;

    // Up from some child, or down with an index.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    enum Direction { Up(usize), Down(Index) }
    use Direction::*;

    let mut current = subgraph.last;
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

    loop {
        // Work around an intellij bug.
        if false { break; }

        // println!("At node {current} / Dir {:?}", last_direction);
        // println!("index stack {:?}", index_stack);

        // The traversal is complex because we're essentially doing a postfix traversal from the
        // *last* edit up through the tree. And then, as we *leave* each node in the traversal
        // (going back down the CG again), actions are emitted.

        // I suspect there's a nicer way to factorize this code, somehow. A recursive solution might
        // be nice, but I don't want to make it vulnerable to stack smashing attacks.

        let next_direction = match &mut g[current] {
            GraphEntry::Split { parent, num_children, state } => 'block: {
                // Split nodes always visit their children in the order that the split node itself
                // is visited. They're like a mirror.


                // The way the index should work is this:
                // 1. The split node gets visited for the first time
                // 2. We visit the parent, and we get visited again coming *down* with an index.
                //    (Or the parent is ROOT and we just use root_index).
                // 3. If we'll be visited again, the index is backed up and used next time.
                let index = if !state.parent_visited {
                    // This is a bit of a hack. The first time the split entry is visited will be
                    // from below, and we need to make sure we visit the split's parent on the way
                    // up.
                    assert!(last_direction.is_up());
                    // stack.push(current);

                    state.parent_visited = true;

                    if *parent == usize::MAX {
                        // If the parent is usize::MAX, we're at the root of the tree. Just use
                        // root_index as the index.

                        root_index
                    } else {
                        // The first time we're visited, ignore all of this and just head up to the
                        // parent.
                        break 'block Up(*parent);
                    }
                } else {
                    if let Down(index) = last_direction {
                        index
                    } else {
                        state.backup_index.take().unwrap()
                    }
                };

                // We're going to travel down to one of our children, to whichever child called us.
                assert!(state.children_visited < *num_children);
                assert!(state.backup_index.is_none());

                // If we'll be visited again, backup the index.
                if state.children_visited + 1 < *num_children {
                    let backup_index = next_index;
                    next_index += 1;

                    state.backup_index = Some(backup_index);
                    emit(MergePlanAction::ForkIndex(index, backup_index));
                }

                state.children_visited += 1;
                // dbg!(&state);
                Down(index)
            }

            GraphEntry::Merge { parents, span, state } => {
                // A merge node only has 1 child. We'll get an Up event exactly once, and 1 down
                // event for every item in parents - but only after we go up to those nodes.

                // A merge node decides on the order it traverses its children, but for simplicity
                // we do it in parents[0..] order.
                assert!(parents.is_empty() || state.next_parent_idx < parents.len());

                if let Down(index) = last_direction {
                    // Mark the direction we came in from as complete.
                    state.next_parent_idx += 1;

                    if let Some(primary_index) = state.primary_index {
                        if index != primary_index {
                            // When we're done we'll use primary_index. Everything else can be
                            // dropped.
                            emit(MergePlanAction::DropIndex(index));
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
                } else {
                    // We came from below.
                    assert!(state.primary_index.is_none());
                    assert_eq!(state.next_parent_idx, 0);
                    // state.primary_index = Some(current_index);
                }

                if state.next_parent_idx < parents.len() {
                    // Go up and scan the next parent.
                    Up(parents[state.next_parent_idx])
                } else {
                    // We've merged all our parents into primary_index. Continue down!
                    let primary_index = if parents.is_empty() {
                        // We've hit the root. The primary index is 0 and just go down, ok?
                        // Gross? Maybe. Probably. Mmmm.
                        root_index
                    } else {
                        let primary_index = state.primary_index.unwrap();

                        // Remove the index from the index_stack.
                        if parents.len() >= 2 {
                            let s = index_stack.pop();
                            assert_eq!(Some(primary_index), s);
                        }

                        primary_index
                    };

                    if !span.is_empty() {
                        emit(MergePlanAction::Apply(ApplyAction {
                            span: *span,
                            measured_in: primary_index,
                            updating_other_indexes: index_stack.iter().copied().collect(),
                        }));
                    }

                    // And go down, since we're done here.
                    Down(primary_index)
                }
            }

        };

        // dbg!(&next_step);

        last_direction = next_direction;
        match next_direction {
            Up(next) => {
                stack.push(current);
                current = next;
            }
            Down(_index) => {
                let Some(next) = stack.pop() else { break; };
                current = next;
            }
        }
    }

    println!("Done {:?}", last_direction);
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use super::*;

    #[test]
    fn test_trivial_graph() {
        let mut g = ConflictSubgraph {
            ops: vec![
                GraphEntry::Merge {
                    parents: smallvec![],
                    span: (0..1).into(),
                    state: Default::default(),
                },
            ],
            // first: 0,
            last: 0,
        };

        make_plan(&mut g);
    }

    #[test]
    fn test_simple_graph() {
        let mut g = ConflictSubgraph {
            ops: vec![
                GraphEntry::Split {
                    parent: usize::MAX,
                    num_children: 2,
                    state: Default::default(),
                },
                GraphEntry::Merge {
                    parents: smallvec![0],
                    span: (0..1).into(),
                    state: Default::default(),
                },
                GraphEntry::Merge {
                    parents: smallvec![0],
                    span: (1..2).into(),
                    state: Default::default(),
                },
                GraphEntry::Merge {
                    parents: smallvec![1, 2],
                    span: (0..0).into(),
                    state: Default::default(),
                }
            ],
            // first: 0,
            last: 3,
        };

        make_plan(&mut g);
    }

    #[test]
    fn diamonds() {
        let mut g = ConflictSubgraph {
            ops: vec![
                GraphEntry::Split { // 0 (x)
                    parent: usize::MAX,
                    num_children: 2,
                    state: Default::default(),
                },
                GraphEntry::Merge { // 1 (XA)
                    parents: smallvec![0],
                    span: (0..1).into(),
                    state: Default::default(),
                },
                GraphEntry::Merge { // 2 (XBD)
                    parents: smallvec![0],
                    span: (1..2).into(),
                    state: Default::default(),
                },
                GraphEntry::Split { // 3 (A)
                    parent: 1,
                    num_children: 2,
                    state: Default::default(),
                },
                GraphEntry::Merge { // 4 (ACY)
                    parents: smallvec![3],
                    span: (2..3).into(),
                    state: Default::default(),
                },
                GraphEntry::Merge { // 5 (AD)
                    parents: smallvec![3],
                    span: (3..4).into(),
                    state: Default::default(),
                },
                GraphEntry::Merge { // 6 (D)
                    parents: smallvec![2, 5],
                    span: (4..5).into(), // DY
                    state: Default::default(),
                },
                GraphEntry::Merge { // 7 (Y)
                    parents: smallvec![4, 6],
                    span: (5..6).into(),
                    state: Default::default(),
                },
            ],
            last: 7,
        };

        make_plan(&mut g);
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

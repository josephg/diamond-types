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
    backup_index: Index,
}

#[derive(Debug, Clone, Default)]
struct MergeState {
    primary_index: Option<Index>,
    // path_complete: SmallVec<[bool; 8]>
    next_parent_idx: usize,
}

#[derive(Debug, Clone)]
enum GraphEntry {
    Ops {
        span: DTRange,
        parent: usize,
        child: usize,
    },
    Merge {
        parents: SmallVec<[usize; 2]>,
        child: usize,
        state: MergeState,
    },
    Split {
        parent: usize,
        children: SmallVec<[usize; 2]>,

        state: SplitState,
    },
}

#[derive(Debug, Clone)]
struct ConflictSubgraph {
    ops: Vec<GraphEntry>,
    first: usize, // Maybe always the last thing?
    last: usize,

    // TODO: Replace this with a bitvec.
    // complete: Vec<bool>,
}


fn emit(action: MergePlanAction) {
    // dbg!(action);
    println!("Action {:?}", action)
}

// enum

fn make_plan(subgraph: &mut ConflictSubgraph) {
    let mut stack = vec![];
    let mut index_stack = vec![];

    let g = &mut subgraph.ops;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    enum Direction { Up, Down(Index) }
    use Direction::*;

    let mut current = subgraph.last;
    let mut prev = usize::MAX;

    let mut direction = Up;

    // let mut current_index = None;
    let mut next_index = 1;

    // Only for debugging.
    // let mut complete = vec![false; g.len()];

    loop {
        if current >= g.len() {
            if direction == Up {
                // We're at the root.
                // current_index = 0;
                direction = Down(0);
                current = stack.pop().unwrap();
                // println!("Hit the top of the stack. Starting at node {current}");
            } else {
                // We're done.
                // break;

                // Work around an intellij bug where it thinks the whole loop is unreachable.
                if false { break; }
                // Should break from popping the stack.
                unreachable!();
                // panic!("asdf");
            }
        }

        let c = current;

        // println!("At node {current} / prev {prev}. Dir {:?}", direction);

        match &mut g[current] {
            GraphEntry::Ops { span, parent, child } => {
                if let Down(index) = direction {
                    emit(MergePlanAction::Apply(ApplyAction {
                        span: *span,
                        measured_in: index,
                        updating_other_indexes: index_stack.iter().copied().collect(),
                    }));

                    // Move to this node's child.
                    let Some(next) = stack.pop() else { break; };
                    current = next;
                    assert_eq!(current, *child);
                } else {
                    // Go up.
                    direction = Up;
                    stack.push(current);
                    current = *parent;
                }
            }

            GraphEntry::Merge { parents, child, state } => {
                // A merge node decides on the order it traverses its children; and for simplicity
                // we do it in parents[0..] order.
                assert!(state.next_parent_idx < parents.len());
                if let Down(index) = direction {
                    // Mark the direction we came in from as complete.
                    assert_eq!(prev, parents[state.next_parent_idx]);
                    state.next_parent_idx += 1;

                    if let Some(primary_index) = state.primary_index {
                        if index != primary_index {
                            // When we're done we'll use primary_index. Everything else can be
                            // dropped.
                            emit(MergePlanAction::DropIndex(index));
                        }
                    } else {
                        // The first index we encounter going down is our primary index.
                        state.primary_index = Some(index);
                        assert_eq!(false, index_stack.contains(&index));
                        index_stack.push(index);
                    }
                } else {
                    // We came from below.
                    assert!(state.primary_index.is_none());
                    assert_eq!(state.next_parent_idx, 0);
                    // state.primary_index = Some(current_index);
                }

                if state.next_parent_idx < parents.len() {
                    // Go up and scan the next parent.
                    direction = Up;
                    stack.push(current);
                    current = parents[state.next_parent_idx];
                } else {
                    // We've merged all our parents into primary_index. Continue!
                    let primary_index = state.primary_index.unwrap();

                    // Remove the index from the index_stack.
                    let s = index_stack.pop();
                    assert_eq!(Some(primary_index), s);

                    // And go down, since we're done here.
                    direction = Down(primary_index);
                    let Some(next) = stack.pop() else { break; };
                    current = next;
                    assert_eq!(current, *child);
                }
            }

            GraphEntry::Split { parent, children, state } => {
                // Split nodes always visit their children in the order that the split node itself
                // is visited. They're like a mirror.

                if !state.parent_visited {
                    // This is a bit of a hack. The first time the split entry is visited will be
                    // from below, and we need to make sure we visit the split's parent on the way
                    // up.
                    assert_eq!(direction, Up);
                    stack.push(current);
                    current = *parent;
                    state.parent_visited = true;
                    // state.next_child = prev;
                    // state.children_visited = 1;
                } else {
                    assert!(state.children_visited < children.len());

                    // Check if we need to backup the index.
                    let index = if let Down(index) = direction { index } else {
                        state.backup_index
                    };

                    if state.children_visited + 1 < children.len() {
                        state.backup_index = next_index;
                        next_index += 1;

                        emit(MergePlanAction::ForkIndex(index, state.backup_index));
                    }

                    direction = Down(index);
                    let Some(next) = stack.pop() else { break; };
                    assert!(children.contains(&next));
                    current = next;
                    state.children_visited += 1;
                }
            }
        }
        prev = c;
    }

    println!("Done {:?}", direction);
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use super::*;

    #[test]
    fn test_trivial_graph() {
        let mut g = ConflictSubgraph {
            ops: vec![
                GraphEntry::Ops {
                    span: (0..1).into(),
                    parent: usize::MAX,
                    child: usize::MAX,
                },
            ],
            first: 0,
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
                    children: smallvec![1, 2],
                    state: Default::default(),
                },
                GraphEntry::Ops {
                    span: (0..1).into(),
                    parent: 0,
                    child: 3,
                },
                GraphEntry::Ops {
                    span: (1..2).into(),
                    parent: 0,
                    child: 3,
                },
                GraphEntry::Merge {
                    parents: smallvec![1, 2],
                    child: usize::MAX,
                    state: Default::default(),
                }
            ],
            first: 0,
            last: 3,
        };

        make_plan(&mut g);
    }

    #[test]
    fn diamonds() {
        let mut g = ConflictSubgraph {
            ops: vec![
                GraphEntry::Split { // 0
                    parent: usize::MAX,
                    children: smallvec![1, 2],
                    state: Default::default(),
                },
                GraphEntry::Ops { // 1
                    span: (0..1).into(),
                    parent: 0,
                    child: 3,
                },
                GraphEntry::Ops { // 2
                    span: (1..2).into(),
                    parent: 0,
                    child: 5,
                },
                GraphEntry::Split { // 3
                    parent: 1,
                    children: smallvec![4, 5],
                    state: Default::default(),
                },
                GraphEntry::Ops { // 4
                    span: (2..3).into(),
                    parent: 3,
                    child: 7,
                },
                GraphEntry::Merge { // 5
                    parents: smallvec![2, 3],
                    child: 6,
                    state: Default::default(),
                },
                GraphEntry::Ops { // 6
                    span: (3..4).into(),
                    parent: 5,
                    child: 7,
                },
                GraphEntry::Merge { // 7
                    parents: smallvec![4, 6],
                    child: usize::MAX,
                    state: Default::default(),
                }
            ],
            first: 0,
            last: 7,
        };

        make_plan(&mut g);
    }
}


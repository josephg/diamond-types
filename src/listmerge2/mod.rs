use std::cmp::Ordering;
use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use rle::SplitableSpan;
use crate::causalgraph::graph::{Graph, GraphEntrySimple};
use crate::{DTRange, Frontier, LV};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::frontier::FrontierRef;

type Index = usize;

struct ApplyAction {
    span: DTRange,
    insert_items: bool,
    measured_in: Index,
    updating_other_indexes: SmallVec<[Index; 2]>,
}

enum MergePlanAction {
    Apply(ApplyAction),
    DiscardInserts(DTRange),
    ForkIndex(Index, Index),
    DropIndex(Index),
}


#[derive(Debug, Clone)]
struct GraphNoodle {
    span: DTRange,
    // txn_idx: usize,
    parents: Frontier,
    children: SmallVec<[LV; 2]>,
    flag: DiffFlag,
    // Idx of siblings in this list.
    // parent_idxs: SmallVec<[usize; 4]>,
    // child_idxs: SmallVec<[usize; 4]>,
}

// Sorted highest to lowest (so we sort by the highest item first).
#[derive(Debug, PartialEq, Eq, Clone)]
struct RevSortFrontier(Frontier);

impl Ord for RevSortFrontier {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.iter().rev().cmp(other.0.iter().rev())
    }
}

impl PartialOrd for RevSortFrontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<LV> for RevSortFrontier {
    fn from(v: LV) -> Self {
        Self(Frontier::new_1(v))
    }
}

impl From<&[LV]> for RevSortFrontier {
    fn from(f: FrontierRef) -> Self {
        RevSortFrontier(f.into())
    }
}


impl Graph {
    fn find_conflicting_2(&self, a: &[LV], b: &[LV]) -> (Frontier, Vec<GraphNoodle>) {
        // TODO: Short circuits.

        let mut result = vec![];

        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
        struct QueueEntry {
            f: RevSortFrontier,
            flag: DiffFlag,
            // TODO: Eq, PartialEq, Ord, PartialOrd should ignore children.
            children: SmallVec<[LV; 2]>,
        }

        // The heap is sorted such that we pull the highest items first.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();
        queue.push(QueueEntry {
            f: a.into(),
            flag: DiffFlag::OnlyA,
            children: smallvec![],
        });
        queue.push(QueueEntry {
            f: b.into(),
            flag: DiffFlag::OnlyB,
            children: smallvec![],
        });

        // Loop until we've collapsed the graph down to a single element.
        let frontier: Frontier = loop {
            let QueueEntry {
                f: frontier,
                mut flag,
                mut children
            } = queue.pop().unwrap();
            // dbg!((&time, flag));

            let Some((&v, merged_with)) = frontier.0.0.split_last() else {
                debug_assert!(frontier.0.is_root());
                break frontier.0;
            };

            // Gather identical entries.
            // I could write this with an inner loop and a match statement, but this is shorter and
            // more readable. The optimizer has to earn its keep somehow.
            while let Some(peek_entry) = queue.peek() {
                if peek_entry.f == frontier {
                    // Logic adapted from diff().
                    if peek_entry.flag != flag { flag = DiffFlag::Shared; }

                    for i in peek_entry.children.iter() {
                        if !children.contains(i) {
                            children.push(*i);
                        }
                    }

                    queue.pop();
                } else { break; }
            }

            if queue.is_empty() {
                debug_assert_eq!(flag, DiffFlag::Shared);
                break frontier.0;
            }

            // If this node is a merger, shatter it.
            // We'll deal with v directly this loop iteration.
            for &t in merged_with {
                queue.push(QueueEntry {
                    f: t.into(),
                    flag,
                    children: children.clone(),
                });
            }

            let containing_txn = self.entries.find_packed(v);

            let mut last = v;

            // Consume all other changes within this txn.
            loop {
                if let Some(peek_entry) = queue.peek() {
                    // println!("peek {:?}", &peek_time);
                    // Might be simpler to use containing_txn.contains(peek_time.last).

                    // A bit gross, but the best I can come up with for this logic.

                    let Some(&peek_v) = peek_entry.f.0.0.last() else { break; };
                    if peek_v < containing_txn.span.start { break; }

                    // The next item is within this txn. Consume it.
                    // dbg!((&peek_time, peek_flag));
                    let peek_entry = queue.pop().unwrap();

                    // We've run into a merged item which uses part of this entry.
                    // We've already pushed the necessary span to the result. Do the
                    // normal merge & shatter logic with this item next.
                    if peek_entry.f.0.len() >= 2 {
                        let Some((_, merged_with)) = peek_entry.f.0.0.split_last() else {
                            unreachable!();
                        };

                        for t in merged_with {
                            queue.push(QueueEntry {
                                f: (*t).into(),
                                flag: peek_entry.flag,
                                children: peek_entry.children.clone(),
                            });
                        }
                    }

                    // Only emit inner items when they aren't duplicates.
                    if peek_v == last {
                        // Don't emit, but merge children.
                        for i in peek_entry.children.iter() {
                            if !children.contains(i) {
                                children.push(*i);
                            }
                        }
                    } else if peek_v < last {
                        result.push(GraphNoodle {
                            span: (peek_v + 1..last + 1).into(),
                            parents: Frontier::new_1(peek_v),
                            children,
                            flag,
                        });
                        children = peek_entry.children;
                        children.push(peek_v + 1);
                        last = peek_v;
                    } else { unreachable!() }

                    if peek_entry.flag != flag { flag = DiffFlag::Shared; }
                } else {
                    break;
                }
            }

            // Emit the remainder of this txn.
            result.push(GraphNoodle {
                span: (containing_txn.span.start..last + 1).into(),
                parents: containing_txn.parents.clone(),
                children,
                flag,
            });

            // If this entry has multiple parents, we'll end up pushing a merge here
            // then immediately popping it. This is so we stop at the merge point.
            queue.push(QueueEntry {
                f: containing_txn.parents.as_ref().into(),
                flag,
                children: smallvec![containing_txn.span.start],
            });

            if queue.is_empty() {
                break Frontier::new_1(last);
            }
        };

        result.reverse();
        (frontier, result)
    }
}

impl Graph {
    fn make_execution_plan(&self, from_frontier: &[LV], merging_frontier: &[LV]) -> Vec<MergePlanAction> {
        // This code is modeled off the find_conflicting logic. The broad goal here is to:
        // - Find all the spans which we need to scan & merge
        // - Then make an execution plan to merge them all

        // Before anything else, some simple short circuits for common / simple cases.
        if merging_frontier.is_empty() || from_frontier == merging_frontier { return vec![]; }
        if from_frontier.len() == 1 && merging_frontier.len() == 1 {
            // Check if either operation naively dominates the other. We could do this for more
            // cases, but we may as well use the code below instead.
            let a = from_frontier[0];
            let b = merging_frontier[0];

            if self.is_direct_descendant_coarse(b, a) {
                // b >= a. Just directly apply the new items.
                return vec![MergePlanAction::Apply(ApplyAction {
                    span: (a..b).into(),
                    insert_items: false,
                    measured_in: 0,
                    updating_other_indexes: smallvec![],
                })];
            }
            if self.is_direct_descendant_coarse(a, b) {
                // a >= b. We're already merged. Nothing to do!
                return vec![];
            }
        }

        // Alright; now

        // This is a min-heap
        #[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
        struct QueueEntry {
            idx: usize,
            start: LV,
        }
        // let mut queue = BinaryHeap::<QueueEntry>::new();

        // TODO: Move me into a .collect()
        // for idx in graph.root_child_indexes.iter() {
        //     queue.push(QueueEntry {
        //         idx: *idx,
        //         start: graph.entries[*idx].span.start,
        //     });
        // }
        todo!();
    }
}

#[cfg(test)]
mod test {
    use crate::causalgraph::graph::tools::test::fancy_graph;

    #[test]
    fn foo() {
        let graph = fancy_graph();
        let result = graph.find_conflicting_2(&[8], &[6]);
        dbg!(result);
    }
}
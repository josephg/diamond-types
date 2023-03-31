mod action_plan;
mod test_conversion;

// #[cfg(feature = "dot_export")]
mod dot;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use rle::SplitableSpan;
use crate::causalgraph::graph::{Graph, GraphEntrySimple};
use crate::{CausalGraph, DTRange, Frontier, LV};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::frontier::FrontierRef;
use crate::listmerge2::action_plan::EntryState;

type Index = usize;


#[derive(Debug, Clone)]
struct ActionGraphEntry {
    pub parents: SmallVec<[usize; 2]>, // 2+ items. These are indexes to sibling items, not LVs.
    pub span: DTRange,
    pub num_children: usize,
    pub state: EntryState,
    // flag: DiffFlag,
}

#[derive(Debug, Clone)]
pub(super) struct ConflictSubgraph {
    ops: Vec<ActionGraphEntry>,
    // last: usize,
}


// Sorted highest to lowest (so we compare the highest first).
#[derive(Debug, PartialEq, Eq, Clone)]
struct RevSortFrontier(SmallVec<[LV; 2]>);

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
    fn from(v: LV) -> Self { Self(smallvec![v]) }
}

impl From<&[LV]> for RevSortFrontier {
    fn from(f: &[LV]) -> Self {
        RevSortFrontier(f.into())
    }
}

#[derive(Debug, Clone)]
struct QueueEntry {
    version: LV,
    // merged_with: RevSortFrontier,
    flag: DiffFlag,
    // These are indexes into the output for child items that need their parents updated when
    // they get inserted.
    // child_indexes: SmallVec<[usize; 2]>,
    child_index: usize,
}

impl PartialEq<Self> for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        // self.frontier == other.frontier
        self.version == other.version
    }
}
impl Eq for QueueEntry {}

impl PartialOrd<Self> for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // TODO: Could ditch RevSortFrontier above and just do the special sorting here.
        // self.frontier.cmp(&other.frontier)
        self.version.cmp(&other.version)
    }
}

fn peek_when_matches<T: Ord, F: FnOnce(&T) -> bool>(heap: &BinaryHeap<T>, pred: F) -> Option<&T> {
    if let Some(peeked) = heap.peek() {
        if pred(peeked) {
            return Some(peeked);
        }
    }
    None
}

// fn if_let_and<T>(opt: Option<T>, pred: bool) -> Option<T> {
//     if pred { opt } else { None }
// }

impl CausalGraph {
    fn find_conflicting_all(&self) -> ConflictSubgraph {
        self.graph.find_conflicting_2(&[], self.version.as_ref())
    }
}
impl Graph {
    fn find_conflicting_2(&self, a: &[LV], b: &[LV]) -> ConflictSubgraph {
        // TODO: Short circuits.
        if a == b {
            // Nothing to do here.
            return ConflictSubgraph { ops: vec![] };
        }

        // let mut result: Vec<ActionGraphEntry> = vec![];
        let mut result: Vec<ActionGraphEntry> = vec![];

        // The "final" state needs to be in a single entry, and that entry needs to be at the start
        // of the resulting graph.
        //
        // We're merging b into a. There's essentially 2 cases here:
        //
        // 1. b is a direct descendant of a. The "last" (first) item will be b.
        // 2. a and b have concurrent operations. The last item will be a merge with multiple
        // parents.

        result.push(ActionGraphEntry {
            parents: Default::default(),
            span: Default::default(),
            num_children: 0,
            state: Default::default(),
        });

        // The heap is sorted such that we pull the highest items first.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();
        for &version in a {
            queue.push(QueueEntry { version, flag: DiffFlag::OnlyA, child_index: 0 });
        }
        for &version in b {
            queue.push(QueueEntry { version, flag: DiffFlag::OnlyB, child_index: 0 });
        }

        // let mut end_a: SmallVec<[usize; 2]> = SmallVec::with_capacity(a.len());
        // let mut end_b: SmallVec<[usize; 2]> = SmallVec::with_capacity(b.len());

        let mut root_children: SmallVec<[usize; 2]> = smallvec![];

        let insert_parent = |result: &mut Vec<ActionGraphEntry>, child_index: usize, new_index: usize, _flag: DiffFlag| -> usize {
            // if child_index == usize::MAX {
            //     let first = if flag == DiffFlag::OnlyA { &mut end_a } else { &mut end_b };
            //     first.push(new_index);
            //     0
            // } else {
            //     result[child_index].parents.push(new_index);
            //     1
            // }
            result[child_index].parents.push(new_index);
            // if child_index == 0 { 0 } else { 1 }
            1
        };

        // Loop until we've collapsed the graph down to a single element.
        while let Some(entry) = queue.pop() { // TODO: Replace with a while let Some() = pop() ?
            // println!("pop {:?}", &entry);
            let mut flag = entry.flag;

            let mut new_index = result.len();
            let mut num_children = insert_parent(&mut result, entry.child_index, new_index, flag);

            // Ok, now we're going to prepare all the items which exist within the txn containing v.
            let containing_txn = self.entries.find_packed(entry.version);
            let mut last = entry.version;

            // Consume all other changes within this txn.
            while let Some(peek_entry) = queue.peek() {
                // println!("peek {:?}", &peek_entry);
                // Might be simpler to use containing_txn.contains(peek_time.last).

                // A bit gross, but the best I can come up with for this logic.
                let peek_v = peek_entry.version;
                // let Some((&peek_v, peek_merge)) = peek_entry.frontier.0.0.split_last() else { break; };
                if peek_v < containing_txn.span.start { break; }

                // The next item is within this txn. Consume it.
                let peek_entry = queue.pop().unwrap();

                if peek_v == last {
                    // Just add to new_item.
                    num_children += insert_parent(&mut result, peek_entry.child_index, new_index, flag);
                } else {
                    debug_assert!(peek_v < last);

                    result.push(ActionGraphEntry {
                        parents: smallvec![new_index + 1],
                        span: (peek_v+1 .. last+1).into(),
                        num_children,
                        state: Default::default(),
                    });

                    new_index += 1;
                    num_children = 1 + insert_parent(&mut result, peek_entry.child_index, new_index, flag);
                    last = peek_v
                }

                if peek_entry.flag != flag { flag = DiffFlag::Shared; }
            }

            // Emit the remainder of this txn.
            debug_assert_eq!(result.len(), new_index);
            result.push(ActionGraphEntry {
                parents: smallvec![],
                span: (containing_txn.span.start..last+1).into(),
                num_children,
                state: Default::default(),
            });

            if containing_txn.parents.is_root() {
                // This is annoying. The graph needs to be in a format where exactly one node has
                // the root as "parents". I'm going to insert an explicit parents entry at the end
                // of the operation log. Just mark that this node has root as a parent.
                root_children.push(new_index);
            } else {
                for &p in containing_txn.parents.iter() {
                    queue.push(QueueEntry {
                        version: p,
                        flag,
                        child_index: new_index,
                    });
                }
            }
        };

        if !root_children.is_empty() && root_children.as_ref() != &[result.len() - 1] {
            let root_index = result.len();
            result.push(ActionGraphEntry {
                parents: smallvec![],
                span: Default::default(),
                num_children: root_children.len(),
                state: Default::default(),
            });
            for r in root_children {
                result[r].parents.push(root_index);
            }
        }

        ConflictSubgraph {
            ops: result,
        }
    }
}


#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use crate::causalgraph::graph::{Graph, GraphEntrySimple};
    use crate::causalgraph::graph::tools::test::fancy_graph;
    use crate::{Frontier, LV};
    use crate::list::ListOpLog;

    fn check(graph: &Graph, a: &[LV], b: &[LV]) {
        // dbg!(a, b);
        let mut result = graph.find_conflicting_2(a, b);
        // dbg!(&result);
        result.dbg_check();

        let plan = result.make_plan(&graph);
        plan.simulate_plan(&graph, &[]);
    }

    #[test]
    fn test_from_fancy_graph() {
        let graph = fancy_graph();
        check(&graph, &[], &[]);
        check(&graph, &[0], &[]);
        check(&graph, &[0], &[3]);
        check(&graph, &[0], &[6]);
        check(&graph, &[], &[0, 6]);
        check(&graph, &[], &[0, 3]);
        check(&graph, &[10], &[5]);
        check(&graph, &[], &[5, 10]);
        // let result = graph.find_conflicting_2(&[1], &[2]);
        // let result = graph.find_conflicting_2(&[5], &[9]);
    }

    #[test]
    fn combined_merge() {
        // let graph = Graph::from_simple_items(&[
        //     GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
        //     GraphEntrySimple { span: 1.into(), parents: Frontier::root() },
        //     GraphEntrySimple { span: 2.into(), parents: Frontier::from(0) },
        //     GraphEntrySimple { span: 3.into(), parents: Frontier::from(1) },
        //     GraphEntrySimple { span: 4.into(), parents: Frontier::from(0) },
        //     GraphEntrySimple { span: 5.into(), parents: Frontier::from(1) },
        //
        //
        //     GraphEntrySimple { span: 4.into(), parents: Frontier::from_sorted(&[2, 3]) },
        //     GraphEntrySimple { span: 5.into(), parents: Frontier::from_sorted(&[4, 5]) },
        // ]);

        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: 0.into(), parents: Frontier::root() },
            GraphEntrySimple { span: 1.into(), parents: Frontier::root() },

            GraphEntrySimple { span: 2.into(), parents: Frontier::from_sorted(&[0, 1]) },
            GraphEntrySimple { span: 3.into(), parents: Frontier::from_sorted(&[0, 1]) },
        ]);

        let mut result = graph.find_conflicting_2(&[2], &[3]);
        // let mut result = graph.find_conflicting_2(&[4], &[5]);
        // dbg!(&result);
        result.dbg_check();
        let plan = result.make_plan(&graph);
        plan.dbg_check(true);
        plan.dbg_print();
        plan.simulate_plan(&graph, &[]);
    }

    #[test]
    #[ignore]
    fn make_plan() {
        let mut bytes = vec![];
        File::open("benchmark_data/git-makefile.dt").unwrap().read_to_end(&mut bytes).unwrap();
        // File::open("benchmark_data/node_nodecc.dt").unwrap().read_to_end(&mut bytes).unwrap();
        let o = ListOpLog::load_from(&bytes).unwrap();
        let cg = o.cg;

        // let mut conflict_subgraph = cg.graph.to_test_entry_list();
        let mut conflict_subgraph = cg.graph.find_conflicting_2(&[], cg.version.as_ref());

        conflict_subgraph.dbg_check();
        let plan = conflict_subgraph.make_plan(&cg.graph);

        plan.dbg_check(true);

        // println!("Plan with {} steps, using {} indexes", plan.actions.len(), plan.indexes_used);
        plan.dbg_print();

        plan.simulate_plan(&cg.graph, &[]);
        // for (i, action) in plan.actions[220..230].iter().enumerate() {
        //     println!("{i}: {:?}", action);
        // }
    }
}



/*


            // Gather identical entries.
            //
            // The logic here is a bit awful. The problem is that if two items in the CG have
            // identical parents, we want to merge them both into a single output item with no spans
            // so that the action plan ends up simpler.
            if !merged_with.is_empty() {
                if let Some(peek_entry) = peek_when_matches(&queue, |e| e.frontier == frontier) {
                    // Identical merges detected. new_item above will contain all the parents and no
                    // span.
                    let mut merge_special = new_item;
                    let merge_special_index = new_index;

                    merge_special.parents.push(merge_special_index + 1);
                    while let Some(peek_entry) = peek_when_matches(&queue, |e| e.frontier == frontier) {
                        queue.pop().unwrap();
                        if peek_entry.flag != flag { flag = DiffFlag::Shared; }
                        result[peek_entry.child_index].parents.push(merge_special_index);
                        merge_special.num_children += 1;
                    }

                    result.push(merge_special);

                    for &t in merged_with { // TODO: Refactor to merge this with below.
                        queue.push(QueueEntry { frontier: t.into(), flag, child_index: merge_special_index });
                    }

                    // And we'll add another entry past this with only 1 child again (the merge special).
                    new_index += 1;
                    new_item = ActionGraphEntry {
                        parents: smallvec![],
                        span: Default::default(),
                        num_children: 1,
                        state: Default::default(),
                    };
                } else {
                    // Shatter the other merged_with items. We'll deal with v directly this loop
                    // iteration.
                    for &t in merged_with {
                        queue.push(QueueEntry { frontier: t.into(), flag, child_index });
                    }
                }
            }

 */
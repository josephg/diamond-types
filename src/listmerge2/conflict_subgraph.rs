use std::cmp::Ordering;
use smallvec::{SmallVec, smallvec};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use rle::{AppendRle, ReverseSpan};
use crate::causalgraph::graph::Graph;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::{ConflictGraphEntry, ConflictSubgraph};
use crate::{CausalGraph, DTRange, LV};


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
    version: RevSortFrontier,
    flag: DiffFlag,
    // These are indexes into the output for child items that need their parents updated when
    // they get inserted.
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

impl Graph {
    /// This function generates a special "conflict graph" between two versions that we're merging
    /// together. The conflict graph contains mostly the same data as the causal graph, but its a
    /// bit different:
    ///
    /// - Items are not split by parents. Each item only has children after the last element in the
    ///   span.
    /// - It has ancillary data associated with each item.
    /// - We track the number of children each item contains
    /// - If the same items are independently merged multiple times in the graph, we only merge
    ///   them once here and the merged result is shared.
    ///
    /// This method also contains the complexity of:
    ///
    /// - diff / find_conflicting. The resulting conflict subgraph only contains items which
    ///   are in the difference between parameter frontiers `a` and `b`.
    /// - (soon) subgraph.
    pub(crate) fn make_conflict_graph_between<S: Default>(&self, a: &[LV], b: &[LV]) -> ConflictSubgraph<S> {
        // TODO: Short circuits.
        if a == b { // if self.frontier_contains_frontier(a, b) {
            // Nothing to do here.
            return ConflictSubgraph(vec![]);
        }

        // let mut result: Vec<ActionGraphEntry> = vec![];
        let mut result: Vec<ConflictGraphEntry<S>> = vec![];

        // The "final" state needs to be in a single entry, and that entry needs to be at the start
        // of the resulting graph.
        //
        // We're merging b into a. There's essentially 2 cases here:
        //
        // 1. b is a direct descendant of a. The "last" (first) item will be b.
        // 2. a and b have concurrent operations. The last item will be a merge with multiple
        // parents.

        result.push(ConflictGraphEntry {
            parents: Default::default(),
            span: Default::default(),
            num_children: 0,
            state: Default::default(),
            // The flag for the root element is weird, because it describes the union of A and B.
            // Shared really means "Its in both A and B", but this sort of in neither - only in the
            // merge.
            flag: DiffFlag::Shared,
        });

        let mut root_children: SmallVec<[usize; 2]> = smallvec![];

        // The heap is sorted such that we pull the highest items first.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();

        queue.push(QueueEntry { version: a.into(), flag: DiffFlag::OnlyA, child_index: 0 });
        queue.push(QueueEntry { version: b.into(), flag: DiffFlag::OnlyB, child_index: 0 });
        // if !self.frontier_contains_frontier(b, a) {
        //     queue.push(QueueEntry { version: a.into(), flag: DiffFlag::OnlyA, child_index: 0 });
        // }
        // if !self.frontier_contains_frontier(a, b) {
        //     queue.push(QueueEntry { version: b.into(), flag: DiffFlag::OnlyB, child_index: 0 });
        // }

        // Loop until we've collapsed the graph down to a single element.
        'outer: while let Some(entry) = queue.pop() {
            // println!("pop {:?}", &entry);
            let mut flag = entry.flag;
            let Some((&v, merged_with)) = entry.version.0.split_last() else {
                root_children.push(entry.child_index);
                continue;
            };

            let mut num_children = 1;

            // Regardless of if its a merge or not, we'll mark the entry as pointing here.
            let mut new_index = result.len();
            result[entry.child_index].parents.push(new_index);

            while let Some(peek_entry) = queue.peek() {
                if peek_entry.version == entry.version {
                    if peek_entry.flag != flag { flag = DiffFlag::Shared; }
                    num_children += 1;
                    result[peek_entry.child_index].parents.push(new_index);
                    queue.pop();
                } else { break; }
            }

            // if queue.is_empty() {
            //     // We've hit a common version. Stop here.
            //     debug_assert_eq!(flag, DiffFlag::Shared);
            //     println!("STOP 1");
            //     break;
            // }

            if !merged_with.is_empty() {
                // Merge. We'll make an entry just for this merge because it keeps the logic here
                // more simple.
                let mut process_here = true;
                if let Some(peek_entry) = queue.peek() {
                    if let Some((&peek_v, peek_rest)) = peek_entry.version.0.split_last() {
                        if peek_v == v && !peek_rest.is_empty() { process_here = false; }
                    }
                }

                // Shatter.
                for m in merged_with {
                    queue.push(QueueEntry { version: (*m).into(), flag, child_index: new_index });
                }

                result.push(ConflictGraphEntry { // Push the merge entry.
                    parents: if process_here { smallvec![new_index + 1] } else { smallvec![] },
                    span: Default::default(),
                    num_children,
                    state: Default::default(),
                    flag,
                });

                if !process_here {
                    // Shatter this version too and continue.
                    queue.push(QueueEntry { version: v.into(), flag, child_index: new_index });
                    continue;
                }

                num_children = 1;

                // Then process v (the highest version) using merge_index as a parent.
                new_index += 1;
            }

            // Ok, now we're going to prepare all the items which exist within the txn containing v.
            let containing_txn = self.entries.find_packed(v);
            let mut last = v;
            // let mut start = containing_txn.span.start;

            // Consume all other changes within this txn.
            while let Some(peek_entry) = queue.peek() {
                // println!("peek {:?}", &peek_entry);
                // Might be simpler to use containing_txn.contains(peek_time.last).

                // A bit gross, but the best I can come up with for this logic.
                let Some((&peek_v, remainder)) = peek_entry.version.0.split_last() else { break; };
                if peek_v < containing_txn.span.start { break; } // Flush the rest of this txn.

                if !remainder.is_empty() {
                    debug_assert!(peek_v < last); // Guaranteed thanks to the queue ordering.
                    // There's a merge in the pipe. Push the range from peek_v+1..=last.

                    // Push the range from peek_entry.v to v.
                    result.push(ConflictGraphEntry {
                        parents: smallvec![],
                        span: (peek_v+1 .. last+1).into(),
                        num_children,
                        state: Default::default(),
                        flag,
                    });

                    // We'll process the direct parent of this item after the merger we found in
                    // the queue. This is just in case the merger is duplicated - we need to process
                    // all that stuff before the rest of this txn.
                    queue.push(QueueEntry { version: peek_v.into(), flag, child_index: new_index });
                    continue 'outer;
                }

                // The next item is within this txn. Consume it.
                let peek_entry = queue.pop().unwrap();

                if peek_v == last {
                    // Just add to new_item.
                    num_children += 1;
                    result[peek_entry.child_index].parents.push(new_index);
                } else {
                    debug_assert!(peek_v < last);
                    // Push the range from peek_entry.v to v.
                    result.push(ConflictGraphEntry {
                        parents: smallvec![new_index + 1],
                        span: (peek_v+1 .. last+1).into(),
                        num_children,
                        state: Default::default(),
                        flag,
                    });

                    new_index += 1;
                    num_children = 2;
                    result[peek_entry.child_index].parents.push(new_index);
                    last = peek_v;
                }

                if peek_entry.flag != flag { flag = DiffFlag::Shared; }
            }

            if queue.is_empty() {
                // If this is the end, stop here.
                debug_assert_eq!(flag, DiffFlag::Shared);
                println!("STOP 2");
                break;
            }

            // Emit the remainder of this txn.
            debug_assert_eq!(result.len(), new_index);
            result.push(ConflictGraphEntry {
                parents: smallvec![],
                span: (containing_txn.span.start..last+1).into(),
                num_children,
                state: Default::default(),
                flag,
            });

            queue.push(QueueEntry {
                version: containing_txn.parents.as_ref().into(),
                flag,
                child_index: new_index,
            });
        }

        assert!(!root_children.is_empty());
        if root_children.as_ref() != &[result.len() - 1] {
            // Make a new node just for the root children.
            let root_index = result.len();
            result.push(ConflictGraphEntry {
                parents: smallvec![],
                span: Default::default(),
                num_children: root_children.len(),
                state: Default::default(),
                flag: DiffFlag::Shared,
            });
            for r in root_children {
                result[r].parents.push(root_index);
            }
        }

        ConflictSubgraph(result)
    }
}

impl<S: Default + Debug> ConflictSubgraph<S> {
    fn dbg_check_conflicting(&self, graph: &Graph, a: &[LV], b: &[LV]) {
        let mut actual_only_a: SmallVec<[DTRange; 2]> = smallvec![];
        let mut actual_only_b: SmallVec<[DTRange; 2]> = smallvec![];
        let mut actual_shared: SmallVec<[DTRange; 2]> = smallvec![];

        for e in self.0.iter().skip(1) { // Ignore the first merge item.
            if !e.span.is_empty() {
                let list = match e.flag {
                    DiffFlag::OnlyA => &mut actual_only_a,
                    DiffFlag::OnlyB => &mut actual_only_b,
                    DiffFlag::Shared => &mut actual_shared,
                };
                list.push_reversed_rle(e.span);
            }
        }

        let mut expected_only_a: SmallVec<[DTRange; 2]> = smallvec![];
        let mut expected_only_b: SmallVec<[DTRange; 2]> = smallvec![];
        let mut expected_shared: SmallVec<[DTRange; 2]> = smallvec![];
        graph.find_conflicting(a, b, |span, flag| {
            println!("find_conflicting {:?} {:?}", span, flag);
            let list = match flag {
                DiffFlag::OnlyA => &mut expected_only_a,
                DiffFlag::OnlyB => &mut expected_only_b,
                DiffFlag::Shared => &mut expected_shared,
            };
            list.push_reversed_rle(span);
        });

        // dbg!(&self);
        assert_eq!(actual_only_a, expected_only_a);
        assert_eq!(actual_only_b, expected_only_b);
        assert_eq!(actual_shared, expected_shared);
    }

    pub(crate) fn dbg_check(&self) {
        // Things that should be true:
        // - ROOT is referenced exactly once
        // - The last item is the only one without children
        // - num_children is correct

        if self.0.is_empty() {
            // This is a bit arbitrary.
            // assert_eq!(self.last, usize::MAX);
            return;
        }

        assert_eq!(self.0[0].num_children, 0, "Item 0 (last) should have no children");

        for (idx, e) in self.0.iter().enumerate() {
            // println!("{idx}: {:?}", e);
            // println!("contained by {:#?}", self.ops.iter()
            //     .filter(|e| e.parents.contains(&idx))
            //     .collect::<Vec<_>>());

            // Check num_children is correct.
            let actual_num_children = self.0.iter()
                .filter(|e| e.parents.contains(&idx))
                .count();

            if idx > 0 {
                assert_ne!(actual_num_children, 0, "Graph must only have 1 item with no children");
            }

            assert_eq!(actual_num_children, e.num_children,
                       "num_children is incorrect at index {idx}. Actual {actual_num_children} != claimed {}", e.num_children);

            if e.parents.is_empty() {
                assert_eq!(idx, self.0.len() - 1, "The only entry pointing to ROOT should be the last entry");
            }

            // Each entry should either have non-zero parents or have operations.
            assert!(!e.span.is_empty() || e.parents.len() != 1 || idx == 0, "Operation is a noop");
            assert!(e.span.is_empty() || e.parents.len() <= 1, "Operation cannot both merge and have content");

            assert_ne!(idx == 0, e.num_children > 0, "The only item with no children should be item 0. idx {idx} has no children.");

            // The list is sorted in reverse time order. (Last stuff at the start). This property is
            // depended on by the diff code below.
            for p in e.parents.iter() {
                // if *p <= idx {
                //     dbg!(idx, e, self.ops.len(), &self.ops[*p]);
                // }
                assert!(*p > idx);

                if idx > 0 {
                    // idx 0 will say OnlyB, but its the merger of OnlyA and OnlyB. So thats special.
                    let e2 = &self.0[*p];
                    assert!(e2.flag == e.flag || e2.flag == DiffFlag::Shared);
                }
            }
        }

    }
}

impl CausalGraph {
    pub(crate) fn make_conflict_graph<S: Default>(&self) -> ConflictSubgraph<S> {
        self.graph.make_conflict_graph_between(&[], self.version.as_ref())
    }
}


#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use rle::HasLength;
    use crate::causalgraph::graph::{Graph, GraphEntrySimple};
    use crate::causalgraph::graph::tools::test::fancy_graph;
    use crate::{CausalGraph, Frontier, LV};
    use crate::causalgraph::graph::random_graphs::with_random_cgs;
    use crate::list::ListOpLog;

    fn check(graph: &Graph, a: &[LV], b: &[LV]) {
        // dbg!(a, b);
        let mut result = graph.make_conflict_graph_between(a, b);
        println!("a {:?}, b {:?} => result {:#?}", a, b, &result);
        result.dbg_check();
        result.dbg_check_conflicting(graph, a, b);

        let plan = result.make_plan();
        plan.simulate_plan(&graph, &[]);
    }

    #[test]
    fn test_from_fancy_graph() {
        let graph = fancy_graph();
        check(&graph, &[], &[]);
        check(&graph, &[0], &[]);
        check(&graph, &[0], &[3]);
        check(&graph, &[0], &[6]);
        check(&graph, &[2], &[6]);
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

        let mut result = graph.make_conflict_graph_between(&[2], &[3]);
        // let mut result = graph.find_conflicting_2(&[4], &[5]);
        // dbg!(&result);
        result.dbg_check();
        let plan = result.make_plan();
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
        let cg = &o.cg;

        // let mut conflict_subgraph = cg.graph.to_test_entry_list();
        let mut conflict_subgraph = cg.graph.make_conflict_graph_between(&[], cg.version.as_ref());

        conflict_subgraph.dbg_check();
        let plan = conflict_subgraph.make_plan();

        plan.dbg_check(true);

        // println!("Plan with {} steps, using {} indexes", plan.actions.len(), plan.indexes_used);
        plan.dbg_print();

        plan.simulate_plan(&cg.graph, &[]);

        plan.cost_estimate(|range| { o.estimate_cost(range) });
        plan.cost_estimate(|range| { range.len() });
    }

    #[test]
    fn fuzz_conflict_subgraph() {
        with_random_cgs(123, (1000, 3), |(_i, _k), cg, frontiers| {
            println!("{_i} {_k}");
            let subgraph = cg.graph.make_conflict_graph_between::<()>(&[], cg.version.as_ref());
            subgraph.dbg_check();
            // subgraph.dbg_check_conflicting(&cg.graph, &[], cg.version.as_ref());

            for fs in frontiers.windows(2) {
                if (_i, _k) == (5, 1) && fs[0].as_ref() == &[0] && fs[1].as_ref() == &[2] {
                    println!("f: {:?}", fs);
                    dbg!(&cg.graph);
                }
                let subgraph = cg.graph.make_conflict_graph_between::<()>(fs[0].as_ref(), fs[1].as_ref());
                // dbg!(&subgraph);
                subgraph.dbg_check();

                // if (_i, _k) == (5, 1) && fs[0].as_ref() == &[0] && fs[1].as_ref() == &[2] {
                //     dbg!(fs);
                //     dbg!(&cg.graph);
                //     dbg!(&subgraph);
                //
                // }
                // subgraph.dbg_check_conflicting(&cg.graph, fs[0].as_ref(), fs[1].as_ref());
            }
        });
    }

    #[test]
    fn fuzz_action_plans() {
        with_random_cgs(123, (1, 100), |_i, cg, _frontiers| {
            let mut subgraph = cg.graph.make_conflict_graph_between(&[], cg.version.as_ref());
            let plan = subgraph.make_plan();
            plan.simulate_plan(&cg.graph, &[]);

            // TODO:
            // for fs in frontiers.windows(2) {
            //     let start = fs[0].as_ref();
            //     let merge_in = fs[1].as_ref();
            //     let mut subgraph = cg.graph.make_conflict_graph_between(start, merge_in);
            //     let plan = subgraph.make_plan();
            //
            //     // let base = cg.graph.
            //     plan.simulate_plan(&cg.graph, start);
            // }
        });
    }
}

// git-makefile:
// spans: 1808985, forks: 668 maxes 1043 / 15950
//
// Or with identical items merged:
// Plan with 2710 steps, using 36 indexes
// spans: 101008, forks: 662 maxes 371
// spans: 1808985, forks: 662 maxes 371

// node_nodecc:
// spans: 1907491, forks: 45 maxes 1 / 53622


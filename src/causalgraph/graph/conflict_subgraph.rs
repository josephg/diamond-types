/// The conflict graph is a convenient data structure for parts of the code that need to do complex
/// operations with the causal graph.
///
/// It combines functionality from:
///
/// - find_conflicts
/// - SimpleGraph
/// - (eventually) subgraph
///
/// and it allows callers to add extra fields on each returned item.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt::Debug;

use smallvec::{SmallVec, smallvec};

use rle::AppendRle;

use crate::{CausalGraph, DTRange, Frontier, LV};
use crate::causalgraph::graph::Graph;
use crate::causalgraph::graph::tools::DiffFlag;

#[derive(Debug, Clone)]
pub(crate) struct ConflictGraphEntry<S: Default = ()> {
    pub parents: SmallVec<usize, 2>, // 2+ items. These are indexes to sibling items, not LVs.
    pub span: DTRange,
    // pub num_children: usize,
    pub state: S,
    pub flag: DiffFlag,
}

#[derive(Debug, Clone)]
pub(crate) struct ConflictSubgraph<S: Default = ()> {
    pub entries: Vec<ConflictGraphEntry<S>>,
    pub base_version: Frontier,

    // Indexes of A, B in the resulting entries.
    pub a_root: usize,
    pub b_root: usize,
}


// Sorted highest to lowest (so we compare the highest first).
#[derive(Debug, PartialEq, Eq, Clone)]
struct RevSortFrontier(SmallVec<LV, 2>);

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Child {
    Idx(usize),
    ARoot,
    BRoot,
}

#[derive(Debug, Clone)]
struct QueueEntry {
    version: RevSortFrontier,
    flag: DiffFlag,
    // These are indexes into the output for child items that need their parents updated when
    // they get inserted.
    child: Child,
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
        if a == b {
            // Nothing to do here.
            //
            // This is a weird output for the conflict graph. It might make a lot more sense to
            // insert a single dummy entry which a_root and b_root both point to. Then other code
            // wouldn't need to special case this.
            return ConflictSubgraph {
                entries: vec![],
                base_version: a.into(),
                a_root: usize::MAX,
                b_root: usize::MAX,
            };
        }

        // let mut result: Vec<ActionGraphEntry> = vec![];
        let mut entries: Vec<ConflictGraphEntry<S>> = vec![];
        // let mut parents: Vec<usize> = vec![];

        // This is a temporary stack to store the child indexes which point to the next item we're
        // going to emit - if any.
        let mut children: SmallVec<Child, 2> = smallvec![];
        let mut a_root = usize::MAX;
        let mut b_root = usize::MAX;

        // fn push_result<S: Default>(span: DTRange, flag: DiffFlag, children: &mut SmallVec<Child, 2>, result: &mut Vec<ConflictGraphEntry<S>>) -> usize {
        let mut push_result = |span: DTRange, flag: DiffFlag, children: &mut SmallVec<Child, 2>| -> usize {
            let new_index = entries.len();
            // println!("push_result {new_index} <- {:?}", children);

            // let mut num_children = 0;
            for &c in children.iter() {
                match c {
                    Child::Idx(idx) => {
                        entries[idx].parents.push(new_index);
                        // add_child(
                        // num_children += 1;
                    },
                    Child::ARoot => {
                        // println!("ARoot {new_index}");
                        debug_assert_eq!(a_root, usize::MAX);
                        a_root = new_index;
                    }
                    Child::BRoot => {
                        // println!("BRoot {new_index}");
                        debug_assert_eq!(b_root, usize::MAX);
                        b_root = new_index;
                    }
                }
            }

            // Not updating one_final_entry because we won't stop here anyway.
            entries.push(ConflictGraphEntry { // Push the merge entry.
                parents: smallvec![],
                span,
                // num_children,
                state: Default::default(),
                flag,
            });

            children.clear();
            new_index
        };

        // The "final" state needs to be in a single entry, and that entry needs to be at the start
        // of the resulting graph.
        //
        // We're merging b into a. There's essentially 2 cases here:
        //
        // 1. b is a direct descendant of a. The "last" (first) item will be b.
        // 2. a and b have concurrent operations. The last item will be a merge with multiple
        // parents.

        // The heap is sorted such that we pull the highest items first.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();

        queue.push(QueueEntry { version: a.into(), flag: DiffFlag::OnlyA, child: Child::ARoot });
        queue.push(QueueEntry { version: b.into(), flag: DiffFlag::OnlyB, child: Child::BRoot });

        // Loop until we've collapsed the graph down to a single element.
        let frontier: Frontier = 'outer: loop {
            let entry = queue.pop().unwrap();

            // println!("pop {:?} / {:?}", &entry, &queue);
            let mut flag = entry.flag;

            debug_assert!(children.is_empty());
            // println!("CP1 {:?}", entry.child);
            children.push(entry.child);

            // Look for more children of this entry.
            while let Some(peek_entry) = queue.peek() {
                if peek_entry.version == entry.version { // Compare the whole frontier.
                    // println!("peek1 {:?}", peek_entry);

                    debug_assert_ne!(peek_entry.child, entry.child);
                    if peek_entry.flag != flag { flag = DiffFlag::Shared; }
                    // println!("CP2 {:?}", peek_entry.child);
                    children.push(peek_entry.child);
                    queue.pop();
                } else { break; }
            }

            let Some((&v, merged_with)) = entry.version.0.split_last() else {
                // If we hit items with no version, we're at the end of the queue. Burn them out
                // into children.
                // TODO: Merge with block below.
                debug_assert_eq!(flag, DiffFlag::Shared);
                break Frontier::root();
            };

            if queue.is_empty() {
                // We've hit a common version for the whole graph. Stop here.
                // Note that this entry might merge multiple other things, but thats ok, because we
                // don't care about anything past this point.
                debug_assert_eq!(flag, DiffFlag::Shared);
                // println!("STOP 1");
                break entry.version.0.into();
            }

            if !merged_with.is_empty() {
                // Merge. We'll make a separate entry just for this merge.

                // Its possible there's another node which shares versions with this merge.
                // If they're actually identical, we want to make 1 merge node and have all the
                // entries parented off that. If there's weird complex overlapping nonsense, we'll
                // just leave it as a merge node for each.
                let mut process_here = true;
                if let Some(peek_entry) = queue.peek() {
                    if let Some((&peek_v, peek_rest)) = peek_entry.version.0.split_last() {
                        if peek_v == v && !peek_rest.is_empty() { process_here = false; }
                    }
                }

                // Shatter.
                // print!("P1: ");
                let new_index = push_result(Default::default(), flag, &mut children);
                for m in merged_with {
                    queue.push(QueueEntry { version: (*m).into(), flag, child: Child::Idx(new_index) });
                }

                if !process_here {
                    // Shatter this version too and continue.
                    queue.push(QueueEntry { version: v.into(), flag, child: Child::Idx(new_index) });
                    continue;
                }

                // Otherwise the new item is the child of what we do below.
                // println!("CP3 {}", new_index);
                children.push(Child::Idx(new_index));
                // num_children = 1;

                // Then process v (the highest version) using merge_index as a parent.
                // new_index += 1;
            }

            // Ok, now we're going to prepare all the items which exist within the txn containing v.
            let containing_txn = self.entries.find_packed(v);
            let mut last = v;

            // Consume all other changes within this txn.
            loop {
                if let Some(peek_entry) = queue.peek() {
                    // println!("peek {:?}", &peek_entry);
                    // Might be simpler to use containing_txn.contains(peek_time.last).

                    // A bit gross, but the best I can come up with for this logic.
                    let Some((&peek_v, remainder)) = peek_entry.version.0.split_last() else { break; };
                    if peek_v < containing_txn.span.start { break; } // Flush the rest of this txn.
                    // println!("peek2 {:?}", peek_entry);

                    if !remainder.is_empty() {
                        debug_assert!(peek_v < last); // Guaranteed thanks to the queue ordering.
                        // There's a merge in the pipe. Push the range from peek_v+1..=last.

                        // Push the range from peek_entry.v to v.
                        // print!("P2: ");
                        let new_index = push_result((peek_v + 1..last + 1).into(), flag, &mut children);

                        // We'll process the direct parent of this item after the merger we found in
                        // the queue. This is just in case the merger is duplicated - we need to process
                        // all that stuff before the rest of this txn.
                        queue.push(QueueEntry { version: peek_v.into(), flag, child: Child::Idx(new_index) });
                        continue 'outer;
                    } else {
                        // The next item is within this txn. Consume it.
                        let peek_entry = queue.pop().unwrap();

                        if peek_v != last {
                            debug_assert!(peek_v < last);
                            // Push the range from peek_entry.v to v.
                            // new_index += 1;
                            // print!("P3: ");
                            let new_index = push_result((peek_v + 1..last + 1).into(), flag, &mut children);
                            children.push(Child::Idx(new_index));
                            // println!("CP4 {:?} {new_index}", peek_entry.child);

                            last = peek_v;
                        }

                        // println!("CP5 {:?}", peek_entry.child);
                        children.push(peek_entry.child);
                        // if peek_entry.child != usize::MAX { children.push(peek_entry.child); }

                        if peek_entry.flag != flag { flag = DiffFlag::Shared; }
                    }
                } else {
                    // If this is the end, stop here.
                    debug_assert_eq!(flag, DiffFlag::Shared);
                    // println!("STOP 2");
                    break 'outer Frontier::new_1(last);
                }
            }

            // Emit the remainder of this txn.
            // debug_assert_eq!(result.len(), new_index);
            // print!("P4: ");
            let new_index = push_result((containing_txn.span.start..last+1).into(), flag, &mut children);
            queue.push(QueueEntry {
                version: containing_txn.parents.as_ref().into(),
                flag,
                child: Child::Idx(new_index),
            });
        };

        // dbg!(&children);
        // assert!(!root_children.is_empty());
        if children.len() > 1 {
            // Make a new node just for the root children.
            // print!("P5: ");
            push_result(Default::default(), DiffFlag::Shared, &mut children);
        }

        debug_assert_ne!(a_root, usize::MAX);
        debug_assert_ne!(b_root, usize::MAX);
        
        // let rng = &mut rand::thread_rng();
        // for r in result.iter_mut() {
        //     r.parents.shuffle(rng);
        // //     r.parents.reverse();
        // }

        ConflictSubgraph {
            entries, base_version: frontier, a_root, b_root,
        }
    }
}

impl<S: Default + Debug> ConflictSubgraph<S> {
    #[allow(unused)]
    pub(crate) fn dbg_check_conflicting(&self, graph: &Graph, a: &[LV], b: &[LV]) {
        let mut actual_only_a: SmallVec<DTRange, 2> = smallvec![];
        let mut actual_only_b: SmallVec<DTRange, 2> = smallvec![];
        let mut actual_shared: SmallVec<DTRange, 2> = smallvec![];

        let dominators = graph.find_dominators_2(a, b);

        for e in self.entries.iter() {
            if !e.span.is_empty() {
                // Every span must be in the dominator set.
                assert!(graph.frontier_contains_version(dominators.as_ref(), e.span.last()));
                // But not less than the common version.
                assert!(!graph.frontier_contains_version(self.base_version.as_ref(), e.span.start));

                let list = match e.flag {
                    DiffFlag::OnlyA => &mut actual_only_a,
                    DiffFlag::OnlyB => &mut actual_only_b,
                    DiffFlag::Shared => &mut actual_shared,
                };
                list.push_reversed_rle(e.span);
            }
        }

        let mut expected_only_a: SmallVec<DTRange, 2> = smallvec![];
        let mut expected_only_b: SmallVec<DTRange, 2> = smallvec![];
        let mut expected_shared: SmallVec<DTRange, 2> = smallvec![];
        let common = graph.find_conflicting(a, b, |span, flag| {
            // println!("find_conflicting {:?} {:?}", span, flag);
            let list = match flag {
                DiffFlag::OnlyA => &mut expected_only_a,
                DiffFlag::OnlyB => &mut expected_only_b,
                DiffFlag::Shared => &mut expected_shared,
            };
            list.push_reversed_rle(span);
        });

        assert_eq!(common, self.base_version);

        // dbg!(&self);
        assert_eq!(actual_only_a, expected_only_a);
        assert_eq!(actual_only_b, expected_only_b);
        assert_eq!(actual_shared, expected_shared);
    }

    // fn check_indexes_concurrent(&self, idxs: &[usize]) {
    //
    // }

    #[allow(unused)]
    pub(crate) fn dbg_print(&self) {
        for (i, e) in self.entries.iter().enumerate() {
            print!("{i}: {:?}", e);
            if i == self.a_root { print!(" (A ROOT)"); }
            if i == self.b_root { print!(" (B ROOT)"); }
            println!();
        }
        println!("(Base version: {:?} / a_root {} / b_root {})", self.base_version, self.a_root, self.b_root);
    }

    #[allow(unused)]
    fn dbg_check_parents_concurrent(&self, parents: &[usize]) {
        if parents.len() < 1 { return; }

        let mut queue: BinaryHeap<Reverse<(usize, bool)>> = BinaryHeap::new();
        for p in parents {
            queue.push(Reverse((*p, true)));
        }

        // We'll stop when there's no more parent entries.
        let mut parent_entries = parents.len();

        while let Some(Reverse((p, is_parent))) = queue.pop() {
            let e = &self.entries[p];
            if is_parent { parent_entries -= 1; }

            while let Some(Reverse((peek_p, peek_parent))) = queue.peek() {
                if *peek_p == p {
                    if is_parent || *peek_parent {
                        panic!("Parents are not concurrent! {:?}", parents);
                    }
                    // If they're both not parents, its fine.
                    queue.pop();
                } else { break; }
            }

            if parent_entries == 0 { break; }

            for pp in e.parents.iter() {
                queue.push(Reverse((*pp, false)));
            }
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_check(&self) {
        // Things that should be true:
        // - ROOT is referenced exactly once
        // - The last item is the only one without children
        // - num_children is correct

        if self.entries.is_empty() {
            // This is a bit arbitrary.
            // assert_eq!(self.last, usize::MAX);
            return;
        }

        // assert_eq!(self.entries[0].num_children, 0, "Item 0 (last) should have no children");

        assert_ne!(self.a_root, usize::MAX);
        assert_ne!(self.b_root, usize::MAX);


        let mut next_v = 0;
        for e in self.entries.iter().rev() {
            if !e.span.is_empty() {
                assert!(e.span.start >= next_v);
                next_v = e.span.end;
            }
        }

        for (idx, e) in self.entries.iter().enumerate() {
            // println!("{idx}: {:?}", e);
            // println!("contained by {:#?}", self.ops.iter()
            //     .filter(|e| e.parents.contains(&idx))
            //     .collect::<Vec<_>>());

            // Check num_children is correct.
            let actual_num_children = self.entries.iter()
                .filter(|e| e.parents.contains(&idx))
                .count();

            if idx != self.a_root && idx != self.b_root {
                assert_ne!(actual_num_children, 0, "Graph must not have any nodes with no children other than roots");
            }

            // assert_eq!(actual_num_children, e.num_children,
            //            "num_children is incorrect at index {idx}. Actual {actual_num_children} != claimed {}", e.num_children);

            if e.parents.is_empty() {
                // if idx != self.entries.len() - 1 {
                //     self.dbg_print();
                // }
                assert_eq!(idx, self.entries.len() - 1, "The only entry pointing to ROOT should be the last entry");
            }

            // Each entry should either have non-zero parents or have operations.
            assert!(!e.span.is_empty() || e.parents.len() != 1 || idx == 0, "Operation is a noop");
            assert!(e.span.is_empty() || e.parents.len() <= 1, "Operation cannot both merge and have content");

            // assert_ne!(idx == 0, e.num_children > 0, "The only item with no children should be item 0. idx {idx} has no children.");

            // The list is sorted in reverse time order. (Last stuff at the start). This property is
            // depended on by the diff code below.

            self.dbg_check_parents_concurrent(e.parents.as_ref());

            for &p in e.parents.iter() {
                // if *p <= idx {
                //     dbg!(idx, e, self.ops.len(), &self.ops[*p]);
                // }
                assert!(p > idx);
                assert!(p < self.entries.len());

                if idx > 0 {
                    // idx 0 will say OnlyB, but its the merger of OnlyA and OnlyB. So thats special.
                    let e2 = &self.entries[p];
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
    use crate::{Frontier, LV};
    use crate::causalgraph::graph::Graph;
    use crate::causalgraph::graph::random_graphs::with_random_cgs;
    use crate::causalgraph::graph::tools::test::fancy_graph;

    fn check(graph: &Graph, a: &[LV], b: &[LV]) {
        // dbg!(a, b);
        let result = graph.make_conflict_graph_between::<()>(a, b);
        // println!("a {:?}, b {:?} => result {:#?}", a, b, &result);
        result.dbg_check();
        result.dbg_check_conflicting(graph, a, b);
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
    fn fuzz_conflict_subgraph() {
        with_random_cgs(12, (100, 10), |(_i, _k), cg, frontiers| {
            // Iterate through the frontiers, and [root -> cg.version].
            for (_j, fs) in std::iter::once([Frontier::root(), cg.version.clone()].as_slice())
                .chain(frontiers.windows(2))
                .enumerate()
            {
                // println!("{_i} {_k} {_j}");

                // if true {
                // if (_i, _k, _j) == (0, 0, 2) {
                //     println!("\n\n");
                //     dbg!(&cg.graph);
                //     println!("f: {:?}", fs);
                // }

                let subgraph = cg.graph.make_conflict_graph_between::<()>(fs[0].as_ref(), fs[1].as_ref());
                // dbg!(&subgraph);
                // subgraph.dbg_print();

                subgraph.dbg_check();
                subgraph.dbg_check_conflicting(&cg.graph, fs[0].as_ref(), fs[1].as_ref());
            }
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

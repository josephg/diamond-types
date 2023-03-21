mod action_plan;
mod test_conversion;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use rle::SplitableSpan;
use crate::causalgraph::graph::{Graph, GraphEntrySimple};
use crate::{DTRange, Frontier, LV};
use crate::causalgraph::graph::tools::DiffFlag;
use crate::frontier::FrontierRef;
use crate::listmerge2::action_plan::EntryState;

type Index = usize;


#[derive(Debug, Clone)]
struct ActionGraphEntry {
    pub parents: SmallVec<[usize; 2]>, // 2+ items.
    pub span: DTRange,
    pub num_children: usize,
    pub state: EntryState,
    // flag: DiffFlag,
}

#[derive(Debug, Clone)]
pub(super) struct ConflictSubgraph {
    ops: Vec<ActionGraphEntry>,
    last: usize,
}


// Sorted highest to lowest (so we compare the highest first).
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

#[derive(Debug, Clone)]
struct QueueEntry {
    version: LV,
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


impl Graph {
    fn find_conflicting_2(&self, a: &[LV], b: &[LV]) -> ConflictSubgraph {
        // TODO: Short circuits.
        if a == b {
            // Nothing to do here.
            return ConflictSubgraph { ops: vec![], last: usize::MAX };
        }

        // let mut result: Vec<ActionGraphEntry> = vec![];
        let mut result: Vec<ActionGraphEntry> = vec![];

        // The heap is sorted such that we pull the highest items first.
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();
        for &version in a {
            queue.push(QueueEntry { version, flag: DiffFlag::OnlyA, child_index: usize::MAX });
        }
        for &version in b {
            queue.push(QueueEntry { version, flag: DiffFlag::OnlyB, child_index: usize::MAX });
        }

        let mut first_a: Option<usize> = None;
        let mut first_b: Option<usize> = None;

        #[derive(Clone, Copy)]
        enum RootEntry {
            Unknown, OneRoot(usize), MergedRoot(usize)
        }

        let mut root_entry = RootEntry::Unknown;

        let mut insert_parent = |result: &mut Vec<ActionGraphEntry>, child_index: usize, new_index: usize, flag: DiffFlag| {
            if child_index == usize::MAX {
                let first = if flag == DiffFlag::OnlyA { &mut first_a } else { &mut first_b };
                if first.is_none() { *first = Some(new_index) };
            } else {
                result[child_index].parents.push(new_index);
            }
        };

        // Loop until we've collapsed the graph down to a single element.
        while let Some(entry) = queue.pop() { // TODO: Replace with a while let Some() = pop() ?
            dbg!(&entry);
            let mut flag = entry.flag;

            let mut new_index = result.len();
            let mut num_children = 0;
            insert_parent(&mut result, entry.child_index, new_index, flag);

            // Ok, now we're going to prepare all the items which exist within the txn containing v.
            let containing_txn = self.entries.find_packed(entry.version);
            let mut last = entry.version;

            // Consume all other changes within this txn.
            while let Some(peek_entry) = queue.peek() {
                println!("peek {:?}", &peek_entry);
                // Might be simpler to use containing_txn.contains(peek_time.last).

                // A bit gross, but the best I can come up with for this logic.
                let peek_v = peek_entry.version;
                // let Some((&peek_v, peek_merge)) = peek_entry.frontier.0.0.split_last() else { break; };
                if peek_v < containing_txn.span.start { break; }

                // The next item is within this txn. Consume it.
                let peek_entry = queue.pop().unwrap();

                if peek_v == last {
                    // Just add to new_item.
                    num_children += 1;
                    insert_parent(&mut result, peek_entry.child_index, new_index, flag);
                } else {
                    debug_assert!(peek_v < last);

                    result.push(ActionGraphEntry {
                        parents: smallvec![new_index + 1],
                        span: (peek_v+1 .. last+1).into(),
                        num_children,
                        state: Default::default(),
                    });

                    new_index += 1;
                    num_children = 1;
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
                // the root as "parents". This way the standard multiple-children logic can run at
                // the root of the graph.
                //
                // We'll detect and set that up here.
                match root_entry {
                    RootEntry::Unknown => { root_entry = RootEntry::OneRoot(new_index); }
                    RootEntry::OneRoot(index) => {
                        // Make a new merged root pointing at index and new_index.
                        let merged_index = result.len();
                        result.push(ActionGraphEntry {
                            parents: smallvec![],
                            span: (containing_txn.span.start..last+1).into(),
                            num_children: 2,
                            state: Default::default(),
                        });
                        result[index].parents.push(merged_index);
                        result[new_index].parents.push(merged_index);
                        root_entry = RootEntry::MergedRoot(merged_index);
                    }
                    RootEntry::MergedRoot(merged_index) => {
                        result[merged_index].num_children += 1;
                        result[new_index].parents.push(merged_index);
                    }
                }
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

        dbg!(first_a, first_b);
        let last = match (first_a, first_b) {
            (None, None) => usize::MAX,
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (Some(a), Some(b)) => {
                let last = result.len();
                result.push(ActionGraphEntry {
                    parents: smallvec![a, b],
                    span: Default::default(),
                    num_children: 0,
                    state: Default::default(),
                });
                result[a].num_children += 1;
                result[b].num_children += 1;
                last
            }
        };

        ConflictSubgraph {
            ops: result,
            last,
        }
    }
}


#[cfg(test)]
mod test {
    use crate::causalgraph::graph::tools::test::fancy_graph;

    #[test]
    fn foo() {
        let graph = fancy_graph();
        // let result = graph.find_conflicting_2(&[1], &[2]);
        let result = graph.find_conflicting_2(&[0], &[3]);
        dbg!(&result);
        result.dbg_check();
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
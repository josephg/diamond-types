use crate::list::{ROOT_ORDER, Branch, Order};
use bitvec::prelude::*;
use smallvec::{SmallVec, smallvec};
use crate::list::txn::TxnSpan;
use crate::rle::RleVec;
use crate::list::branch::{retreat_branch_by, advance_branch, advance_branch_by_known};
use std::ops::Range;

// So essentially what I'm doing here is a depth first iteration of the time DAG. The trouble is
// that we only want to visit each item exactly once, and we want to minimize the aggregate cost of
// pointlessly traversing up and down the tree.
//
// This is closely related to Edmond's algorithm for finding the minimal spanning arborescence:
// https://en.wikipedia.org/wiki/Edmonds%27_algorithm
//
// The traversal must also obey the ordering rule for time DAGs. No item can be visited before all
// of its parents have been visited.
//
// The code was manually unrolled into an iterator so we could walk it without needing to collect
// this structure to a vec or something.
#[derive(Debug)]
pub(crate) struct OriginTxnIter<'a> {
    // I could hold a slice reference here instead, but it'd be missing the find() methods.
    history: &'a RleVec<TxnSpan>,

    branch: Branch,
    consumed: BitBox, // Could use markers on txns for this instead?
    root_children: SmallVec<[usize; 2]>, // Might make sense to cache this on the document
    stack: Vec<usize>, // smallvec? This will have an upper bound of the number of txns.

    num_consumed: usize, // For debugging.
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalkEntry {
    retreat: SmallVec<[Range<Order>; 4]>,
    advance_rev: SmallVec<[Range<Order>; 4]>,
    // txn: &'a TxnSpan,
    consume: Range<Order>,
}

impl<'a> OriginTxnIter<'a> {
    pub(crate) fn new(history: &'a RleVec<TxnSpan>) -> Self {
        let root_children = history.0.iter().enumerate().filter_map(|(i, txn)| {
            // if txn.parents.iter().eq(std::iter::once(&ROOT_ORDER)) {
            if txn.parents.len() == 1 && txn.parents[0] == ROOT_ORDER {
                Some(i)
            } else { None }
        }).collect::<SmallVec<[usize; 2]>>();

        Self {
            history,
            // TODO: Refactor to start with branch and stack empty.
            branch: smallvec![ROOT_ORDER],
            consumed: bitbox![0; history.len()],
            root_children,
            stack: vec![],
            num_consumed: 0
        }
    }

    // Dirty - returns usize::MAX when there is no next child.
    #[inline(always)]
    fn get_next_child(&self, child_idxs: &[usize]) -> Option<usize> {
        for &i in child_idxs { // Sorted??
            // println!("  - {}", i);
            if self.consumed[i] { continue; }

            let next = &self.history[i];
            // We're looking for a child where all the parents have been satisfied
            if next.parents.len() == 1 || next.parents.iter().all(|&p| {
                // TODO: Speed this up by caching the index of each parent in each txn
                self.consumed[self.history.find_index(p).unwrap()]
            }) {
                return Some(i);
            }
        }
        None
    }
}

impl<'a> Iterator for OriginTxnIter<'a> {
    type Item = WalkEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // Find the next item to consume. We'll start with all the children of the top of the
        // stack, and greedily walk up looking for anything which has all its dependencies
        // satisfied.
        let next_idx = loop {
            // A previous implementation handled both of these cases the same way, but it needed a
            // dummy value at the start of self.stack, which caused an allocation when the doc only
            // has one txn span. This approach is a bit more complex, but leaves the stack empty in
            // this case.
            if let Some(&idx) = self.stack.last() {
                // println!("stack top {}!", idx);
                if let Some(next_idx) = self.get_next_child(&self.history[idx].child_indexes) {
                    break next_idx;
                }

                // TODO: We could retreat branch here. That would have the benefit of not needing as
                // many lookups through txns, but the downside is we'd end up retreating all the way
                // back to the start of the document at the end of the process. Benchmark to see
                // which way is better.

                // println!("pop {}!", idx);
                self.stack.pop();
            } else {
                if let Some(next_idx) = self.get_next_child(&self.root_children) {
                    break next_idx;
                } else {
                    // The stack was exhausted and we didn't find anything. We're done here.
                    debug_assert!(self.consumed.all());
                    debug_assert_eq!(self.num_consumed, self.history.len());
                    return None;
                }
            }
        };

        assert!(next_idx < self.history.len());
        assert!(!self.consumed[next_idx]);

        let next_txn = &self.history[next_idx];

        let (only_branch, only_txn) = self.history.diff(&self.branch, &next_txn.parents);
        // dbg!((&branch, &next_txn.parents, &only_branch, &only_txn));
        // Note that even if we're moving to one of our direct children we might see items only
        // in only_branch if the child has a parent in the middle of our txn.
        for range in &only_branch {
            // println!("Retreat branch by {:?}", span);
            retreat_branch_by(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }
        for range in only_txn.iter().rev() {
            // println!("Advance branch by {:?}", span);
            advance_branch(&mut self.branch, &self.history, range.clone());
            // dbg!(&branch);
        }

        // println!("consume {} (order {:?})", next_idx, next_txn.as_span());
        advance_branch_by_known(&mut self.branch, &next_txn.parents, next_txn.as_order_range());
        // dbg!(&branch);
        self.consumed.set(next_idx, true);
        self.num_consumed += 1;
        self.stack.push(next_idx);

        return Some(WalkEntry {
            retreat: only_branch,
            advance_rev: only_txn,
            consume: next_txn.as_order_range()
        });
    }
}

impl RleVec<TxnSpan> {
    /// This function is for efficiently finding the order we should traverse the time DAG in order to
    /// walk all the changes so we can efficiently save everything to disk. This is needed because if
    /// we simply traverse the txns in the order they're in right now, we can have pathological
    /// behaviour in the presence of multiple interleaved branches. (Eg if you're streaming from two
    /// peers concurrently editing different branches).
    pub(crate) fn txn_spanning_tree_iter(&self) -> OriginTxnIter {
        OriginTxnIter::new(self)
    }
}


#[cfg(test)]
mod test {
    use crate::list::ROOT_ORDER;
    use crate::rle::RleVec;
    use crate::list::txn::TxnSpan;
    use smallvec::smallvec;
    use crate::list::time::txn_trace::{OriginTxnIter, WalkEntry};

    #[test]
    fn iter_span_for_empty_doc() {
        let history = RleVec::new();
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();
        assert!(walk.is_empty());
    }

    #[test]
    fn iter_span_from_root() {
        let history = RleVec(vec![
            TxnSpan {
                order: 0, len: 10, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            },
            TxnSpan {
                order: 10, len: 20, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![]
            }
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();

        assert_eq!(walk, [
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![],
                consume: 0..10,
            },
            WalkEntry {
                retreat: smallvec![0..10],
                advance_rev: smallvec![],
                consume: 10..30,
            },
        ]);
    }

    #[test]
    fn fork_and_join() {
        let history = RleVec(vec![
            TxnSpan {
                order: 0, len: 10, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 10, len: 20, shadow: 10,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 30, len: 20, shadow: 0,
                parents: smallvec![9, 29],
                parent_indexes: smallvec![0, 1], child_indexes: smallvec![]
            },
        ]);
        let walk = history.txn_spanning_tree_iter().collect::<Vec<_>>();

        assert_eq!(walk, [
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![],
                consume: 0..10,
            },
            WalkEntry {
                retreat: smallvec![0..10],
                advance_rev: smallvec![],
                consume: 10..30,
            },
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![0..10],
                consume: 30..50,
            },
        ]);

        // dbg!(walk);
    }

    #[test]
    fn two_chains() {
        let history = RleVec(vec![
            TxnSpan {
                order: 0, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 1, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![3]
            },
            TxnSpan {
                order: 2, len: 1, shadow: 2,
                parents: smallvec![0],
                parent_indexes: smallvec![0], child_indexes: smallvec![4]
            },
            TxnSpan {
                order: 3, len: 1, shadow: 3,
                parents: smallvec![1],
                parent_indexes: smallvec![1], child_indexes: smallvec![4]
            },
            TxnSpan {
                order: 4, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![2, 3],
                parent_indexes: smallvec![2, 3], child_indexes: smallvec![]
            },
        ]);

        // history.traverse_txn_spanning_tree();
        let iter = OriginTxnIter::new(&history);
        // for item in iter {
        //     dbg!(item);
        // }

        assert!(iter.eq(std::array::IntoIter::new([
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![],
                consume: 0..1,
            },
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![],
                consume: 2..3,
            },

            WalkEntry {
                retreat: smallvec![2..3, 0..1],
                advance_rev: smallvec![],
                consume: 1..2,
            },
            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![],
                consume: 3..4,
            },

            WalkEntry {
                retreat: smallvec![],
                advance_rev: smallvec![2..3, 0..1],
                consume: 4..5,
            },
        ])));
    }
}
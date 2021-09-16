use crate::list::{ROOT_ORDER, Branch};
use bitvec::prelude::*;
use smallvec::{SmallVec, smallvec};
use crate::list::txn::TxnSpan;
use crate::rle::RleVec;
use crate::list::branch::{retreat_branch_by, advance_branch, advance_branch_by_known};

impl RleVec<TxnSpan> {
    /// This function is for efficiently finding the order we should traverse the time DAG in order to
    /// walk all the changes so we can efficiently save everything to disk. This is needed because if
    /// we simply traverse the txns in the order they're in right now, we can have pathological
    /// behaviour in the presence of multiple interleaved branches. (Eg if you're streaming from two
    /// peers concurrently editing different branches).
    fn traverse_txn_spanning_tree(&self) {
        // So essentially what I'm doing here is a depth first iteration of the time DAG. The
        // trouble is that we only want to visit each item exactly once, and we want to minimize the
        // aggregate cost of pointlessly traversing up and down the tree.
        //
        // This is closely related to Edmond's algorithm for finding the minimal spanning
        // arborescence:
        // https://en.wikipedia.org/wiki/Edmonds%27_algorithm
        //
        // The traversal must also obey the ordering rule for time DAGs. No item can be visited
        // before all of its parents have been visited.

        // Once each item has been visited, it will be tagged as consumed.
        let mut consumed = bitbox![0; self.len()];

        let root_children = self.0.iter().enumerate().filter_map(|(i, txn)| {
            // if txn.parents.iter().eq(std::iter::once(&ROOT_ORDER)) {
            if txn.parents.len() == 1 && txn.parents[0] == ROOT_ORDER {
                Some(i)
            } else { None }
        }).collect::<SmallVec<[usize; 2]>>();
        // dbg!(&root_children);

        // let mut stack = Vec::new();
        let mut stack = vec![usize::MAX];
        let mut num_consumed = 0;
        let mut branch: Branch = smallvec![ROOT_ORDER];

        while num_consumed < self.len() {
            // Find the next item to consume. We'll start with all the children of the top of the
            // stack, and greedily walk up looking for anything which has all its dependencies
            // satisfied.
            let mut next_idx = 0;
            'outer: while let Some(&idx) = stack.last() {
                // println!("stack top {}!", idx);
                let child_idxs = if idx == usize::MAX { &root_children } else {
                    &self.0[idx].child_indexes
                };

                for i in child_idxs { // Sorted??
                    // println!("  - {}", i);
                    if consumed[*i] { continue; }

                    let next = &self.0[*i];
                    // We're looking for a child where all the parents have been satisfied
                    if next.parents.len() == 1 || next.parents.iter().all(|p| {
                        // TODO: Speed this up by caching the index of each parent in each txn
                        consumed[self.find_index(*p).unwrap()]
                    }) {
                        next_idx = *i;
                        break 'outer;
                    }
                }

                // TODO: We could retreat branch here. That would have the benefit of not needing as
                // many lookups through txns, but the downside is we'd end up retreating all the way
                // back to the start of the document at the end of the process. Benchmark to see
                // which way is better.

                // println!("pop {}!", idx);
                stack.pop();
            }

            assert!(next_idx < self.len());
            assert!(!consumed[next_idx]);

            let next_txn = &self.0[next_idx];

            let (only_branch, only_txn) = self.diff(&branch, &next_txn.parents);
            // dbg!((&branch, &next_txn.parents, &only_branch, &only_txn));
            // Note that even if we're moving to one of our direct children we might see items only
            // in only_branch if the child has a parent in the middle of our txn.
            for span in &only_branch {
                println!("Retreat branch by {:?}", span);
                retreat_branch_by(&mut branch, self, span.order, span.len);
                // dbg!(&branch);
            }
            for span in only_txn.iter().rev() {
                println!("Advance branch by {:?}", span);
                advance_branch(&mut branch, self, *span);
                // dbg!(&branch);
            }

            println!("consume {} (order {:?})", next_idx, next_txn.as_span());
            advance_branch_by_known(&mut branch, &next_txn.parents, next_txn.as_span());
            // dbg!(&branch);
            consumed.set(next_idx, true);
            num_consumed += 1;

            stack.push(next_idx);
        }

        assert_eq!(num_consumed, self.len());
        assert!(consumed.all());

        // dbg!(&branch);
    }
}


#[cfg(test)]
mod test {
    use crate::list::ROOT_ORDER;
    use crate::rle::RleVec;
    use crate::list::txn::TxnSpan;
    use smallvec::smallvec;

    #[test]
    fn iter_span_from_root() {
        RleVec(vec![
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
        ]).traverse_txn_spanning_tree();
    }

    #[test]
    fn fork_and_join() {
        RleVec(vec![
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
        ]).traverse_txn_spanning_tree();
    }

    #[test]
    fn two_chains() {
        RleVec(vec![
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
                parent_indexes: smallvec![1], child_indexes: smallvec![5]
            },
            TxnSpan {
                order: 4, len: 1, shadow: 4,
                parents: smallvec![2],
                parent_indexes: smallvec![2], child_indexes: smallvec![6]
            },
            TxnSpan {
                order: 5, len: 1, shadow: 5,
                parents: smallvec![3],
                parent_indexes: smallvec![3], child_indexes: smallvec![6]
            },
            TxnSpan {
                order: 6, len: 1, shadow: ROOT_ORDER,
                parents: smallvec![4, 5],
                parent_indexes: smallvec![4, 5], child_indexes: smallvec![]
            },
        ]).traverse_txn_spanning_tree();
    }
}
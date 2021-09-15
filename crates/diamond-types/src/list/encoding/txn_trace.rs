use crate::list::ROOT_ORDER;
use bitvec::prelude::*;
use smallvec::SmallVec;
use crate::list::txn::TxnSpan;
use crate::rle::RleVec;

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
        dbg!(&root_children);

        // let mut stack = Vec::new();
        let mut stack = vec![usize::MAX];
        let mut num_consumed = 0;

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
                        consumed[self.search(*p).unwrap()]
                    }) {
                        next_idx = *i;
                        break 'outer;
                    }
                }

                // println!("pop {}!", idx);
                stack.pop();
            }

            assert!(next_idx < self.len());
            assert!(!consumed[next_idx]);

            let last = &self.0[next_idx];
            println!("consume {} (order {})", next_idx, last.order);
            consumed.set(next_idx, true);
            num_consumed += 1;

            stack.push(next_idx);
        }

        assert_eq!(num_consumed, self.len());
        assert!(consumed.all());
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
                order: 10,
                len: 20,
                shadow: 0,
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
                order: 10, len: 20, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 10, len: 20, shadow: 0,
                parents: smallvec![9, 29],
                parent_indexes: smallvec![0, 1], child_indexes: smallvec![]
            },
        ]).traverse_txn_spanning_tree();
    }

    #[test]
    fn two_chains() {
        RleVec(vec![
            TxnSpan {
                order: 0, len: 1, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![2]
            },
            TxnSpan {
                order: 1, len: 1, shadow: 0,
                parents: smallvec![ROOT_ORDER],
                parent_indexes: smallvec![], child_indexes: smallvec![3]
            },
            TxnSpan {
                order: 2, len: 1, shadow: 0,
                parents: smallvec![0],
                parent_indexes: smallvec![0], child_indexes: smallvec![4]
            },
            TxnSpan {
                order: 3, len: 1, shadow: 0,
                parents: smallvec![1],
                parent_indexes: smallvec![1], child_indexes: smallvec![5]
            },
            TxnSpan {
                order: 4, len: 1, shadow: 0,
                parents: smallvec![2],
                parent_indexes: smallvec![2], child_indexes: smallvec![6]
            },
            TxnSpan {
                order: 5, len: 1, shadow: 0,
                parents: smallvec![3],
                parent_indexes: smallvec![3], child_indexes: smallvec![6]
            },
            TxnSpan {
                order: 6, len: 1, shadow: 0,
                parents: smallvec![4, 5],
                parent_indexes: smallvec![4, 5], child_indexes: smallvec![]
            },
        ]).traverse_txn_spanning_tree();
    }
}
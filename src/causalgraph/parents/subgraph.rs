use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use crate::causalgraph::parents::{Parents, ParentsEntryInternal};
use crate::{DTRange, Frontier, LV};
use crate::rle::RleVec;

impl Parents {
    pub fn subgraph(&self, filter: &[DTRange], parents: &[LV]) -> Parents {
        #[derive(PartialOrd, Ord, Eq, PartialEq, Clone, Debug)]
        struct QueueEntry {
            target_parent: LV,
            children: SmallVec<[usize; 2]>,
        }

        // Filter must be sorted.
        let mut filter_iter = filter.iter().copied().rev();
        let mut queue: BinaryHeap<QueueEntry> = BinaryHeap::new();
        let mut result_rev = Vec::<ParentsEntryInternal>::new();
        for p in parents {
            queue.push(QueueEntry {
                target_parent: *p,
                children: smallvec![]
            });
        }

        if let Some(mut filter) = filter_iter.next() {
            'outer: while let Some(mut entry) = queue.pop() {
                // There's essentially 2 cases here:
                // 1. The entry is either inside a filtered item, or an earlier item in this txn
                //    is allowed by the filter.
                // 2. The filter doesn't allow the txn the entry is inside.

                let txn = self.entries.find_packed(entry.target_parent);

                'txn_loop: loop {
                    while filter.start > entry.target_parent {
                        if let Some(f) = filter_iter.next() { filter = f; }
                        else { break 'txn_loop; }
                    }

                    if filter.end <= txn.span.start {
                        break;
                    }

                    debug_assert!(txn.span.start < filter.end);
                    debug_assert!(entry.target_parent >= filter.start);
                    debug_assert!(entry.target_parent >= txn.span.start);

                    // Case 1. We'll add a new parents entry this loop iteration.

                    let p = entry.target_parent.min(filter.end - 1);
                    let idx_here = result_rev.len();

                    for idx in entry.children {
                        // result_rev[idx].parents.insert(p);
                        result_rev[idx].parents.0.push(p);
                    }

                    let base = filter.start.max(txn.span.start);
                    // For simplicity, pull out anything that is within this txn *and* this filter.
                    while let Some(peeked_entry) = queue.peek() {
                        if peeked_entry.target_parent < base { break; }

                        let peeked_target = peeked_entry.target_parent.min(filter.end - 1);
                        for idx in &peeked_entry.children {
                            // if !child_indexes.contains(&idx) { child_indexes.push(*idx); }
                            // result_rev[*idx].parents.insert(peeked_target);
                            result_rev[*idx].parents.0.push(peeked_target);
                        }

                        queue.pop();
                    }

                    result_rev.push(ParentsEntryInternal {
                        span: (base..p + 1).into(),
                        shadow: txn.shadow, // This is pessimistic.
                        parents: Frontier::default(), // Parents current unknown!
                    });

                    if filter.start > txn.span.start {
                        // The item we've just added has an (implicit) parent of base-1. We'll
                        // update entry and loop - which might either find more filter items
                        // within this txn, or it might bump us to the case below where the txn's
                        // items are added.
                        entry = QueueEntry {
                            target_parent: filter.start - 1,
                            children: smallvec![idx_here],
                        };
                    } else {
                        // filter.start <= txn.span.start. We're done with this txn.
                        if !txn.parents.is_empty() {
                            for p in txn.parents.iter() {
                                queue.push(QueueEntry {
                                    target_parent: *p,
                                    children: smallvec![idx_here],
                                })
                            }
                        }
                        continue 'outer;
                    }
                }

                // Case 2. The remainder of this txn is filtered out.
                //
                // We'll create new queue entries for all of this txn's parents.
                let mut child_idxs = entry.children;

                while let Some(peeked_entry) = queue.peek() {
                    if peeked_entry.target_parent < txn.span.start { break; } // Next item is out of this txn.

                    for i in peeked_entry.children.iter() {
                        if !child_idxs.contains(&i) { child_idxs.push(*i); }
                    }
                    queue.pop();
                }

                if txn.parents.0.len() == 1 {
                    // A silly little optimization to avoid an unnecessary clone() below.
                    queue.push(QueueEntry { target_parent: txn.parents.0[0], children: child_idxs })
                } else {
                    for p in txn.parents.iter() {
                        queue.push(QueueEntry {
                            target_parent: *p,
                            children: child_idxs.clone()
                        })
                    }
                }
            }
        }

        result_rev.reverse();

        for e in result_rev.iter_mut() {
            if e.parents.len() >= 2 {
                e.parents.0.reverse();
                // I wish I didn't need to do this. At least I don't think it'll show up on the
                // performance profile.
                e.parents = self.find_dominators(&e.parents.0);
            }
        }

        Parents {
            entries: RleVec(result_rev),
        }
    }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::causalgraph::parents::{Parents, ParentsEntryInternal};
    use crate::Frontier;
    use crate::rle::RleVec;

    fn fancy_parents() -> Parents {
        let p = Parents {
            entries: RleVec(vec![
                ParentsEntryInternal { // 0-2
                    span: (0..3).into(), shadow: 0,
                    parents: Frontier::from_sorted(&[]),
                },
                ParentsEntryInternal { // 3-5
                    span: (3..6).into(), shadow: 3,
                    parents: Frontier::from_sorted(&[]),
                },
                ParentsEntryInternal { // 6-8
                    span: (6..9).into(), shadow: 6,
                    parents: Frontier::from_sorted(&[1, 4]),
                },
                ParentsEntryInternal { // 9-10
                    span: (9..11).into(), shadow: 6,
                    parents: Frontier::from_sorted(&[2, 8]),
                },
            ]),
        };

        p.dbg_check(true);
        p
    }

    #[test]
    #[ignore]
    fn foo() {
        let parents = fancy_parents();

        // let subgraph = parents.subgraph(&[(0..11).into()], &[5, 10]);
        // assert_eq!(subgraph, parents);

        let subgraph2 = parents.subgraph(&[(1..11).into()], &[5, 10]);
        dbg!(&subgraph2);
        // subgraph2.dbg_check(true);
        // let subgraph3 = parents.subgraph(&[(5..6).into()], &[5, 10]);
        // let subgraph3 = parents.subgraph(&[(0..1).into(), (10..11).into()], &[5, 10]);
        // let subgraph3 = parents.subgraph(&[(0..11).into()], &[10]);
        // dbg!(&subgraph3);
        // subgraph3.dbg_check(true);
    }
}
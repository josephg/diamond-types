// This is a replacement for the merge1 algorithm to improve performance.


use smallvec::{SmallVec, smallvec};
use rle::{AppendRle, HasLength, Trim};
use crate::frontier::{frontier_is_sorted, local_version_eq};
use crate::list::{ListBranch, ListOpLog};
use crate::{DTRange, Time};
use crate::causalgraph::parents::tools::DiffFlag;

impl ListBranch {
    pub fn merge_new(&mut self, oplog: &ListOpLog, merge_frontier: &[Time]) {
        debug_assert!(frontier_is_sorted(merge_frontier));
        debug_assert!(frontier_is_sorted(&self.version));

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        //
        // Both of these lists are in reverse time order(!).
        let mut new_ops: SmallVec<[DTRange; 4]> = smallvec![];
        let mut conflict_ops: SmallVec<[DTRange; 4]> = smallvec![];

        let mut shared_size = 0;
        let mut shared_ranges = 0;

        let mut common_ancestor = oplog.cg.parents.find_conflicting(&self.version, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            if flag == DiffFlag::Shared {
                shared_size += span.len();
                shared_ranges += 1;
            }

            // dbg!(&span, flag);
            let target = match flag {
                DiffFlag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);

            #[cfg(feature = "dot_export")]
            if MAKE_GRAPHS {
                let color = match flag {
                    DiffFlag::OnlyA => Blue,
                    DiffFlag::OnlyB => Green,
                    DiffFlag::Shared => Grey,
                };
                dbg_all_ops.push((span, color));
            }
        });

        // dbg!(&opset.history);
        // dbg!((&new_ops, &conflict_ops, &common_ancestor));

        debug_assert!(frontier_is_sorted(&common_ancestor));

        // We don't want to have to make and maintain a tracker, and we don't need to in most
        // situations. We don't need to when all operations in diff.only_b can apply cleanly
        // in-order.
        let mut did_ff = false;
        loop {
            if let Some(span) = new_ops.last() {
                let txn = oplog.cg.parents.entries.find_packed(span.start);
                let can_ff = txn.with_parents(span.start, |parents| {
                    local_version_eq(self.version.as_slice(), parents)
                });

                if can_ff {
                    let mut span = new_ops.pop().unwrap();
                    let remainder = span.trim(txn.span.end - span.start);
                    // println!("FF {:?}", &span);
                    self.apply_range_from(oplog, span);
                    self.version = smallvec![span.last()];

                    if let Some(r) = remainder {
                        new_ops.push(r);
                    }
                    did_ff = true;
                } else {
                    break;
                }
            } else {
                // We're done!
                return;
            }
        }

        if did_ff {
            // Since we ate some of the ops fast-forwarding, reset conflict_ops and common_ancestor
            // so we don't scan unnecessarily.
            //
            // We don't need to reset new_ops because that was updated above.

            // This sometimes adds the FF'ed ops to the conflict_ops set so we add them to the
            // merge set. This is a pretty bad way to do this - if we're gonna add them to
            // conflict_ops then FF is pointless.
            conflict_ops.clear();
            shared_size = 0;
            common_ancestor = oplog.cg.parents.find_conflicting(&self.version, merge_frontier, |span, flag| {
                if flag == DiffFlag::Shared {
                    shared_size += span.len();
                    shared_ranges += 1;
                }

                if flag != DiffFlag::OnlyB {
                    conflict_ops.push_reversed_rle(span);
                }
            });
        }




        // TODO: Also FF at the end!
    }
}
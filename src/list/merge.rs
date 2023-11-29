use rle::HasLength;
use crate::frontier::FrontierRef;
use crate::list::{ListBranch, ListOpLog};
use crate::list::operation::{ListOpKind, TextOperation};
use crate::listmerge::merge::{reverse_str, TransformedOpsIter2};
use crate::listmerge::merge::TransformedResult::{BaseMoved, DeleteAlreadyHappened};
use crate::{DTRange, LV};

impl ListOpLog {
    pub(crate) fn get_xf_operations_full(&self, from: FrontierRef, merging: FrontierRef) -> TransformedOpsIter2 {
        // TransformedOpsIter::new(&self.cg.graph, &self.cg.agent_assignment,
        //                         &self.operation_ctx, &self.operations,
        //                         from, merging)
        TransformedOpsIter2::new(&self.cg.graph, &self.cg.agent_assignment,
                                &self.operation_ctx, &self.operations,
                                from, merging)
    }

    /// Iterate through all the *transformed* operations from some point in time. Internally, the
    /// OpLog stores all changes as they were when they were created. This makes a lot of sense from
    /// CRDT academic point of view (and makes signatures and all that easy). But its is rarely
    /// useful for a text editor.
    ///
    /// `get_xf_operations` returns an iterator over the *transformed changes*. That is, the set of
    /// changes that could be applied linearly to a document to bring it up to date.
    pub fn iter_xf_operations_from(&self, from: FrontierRef, merging: FrontierRef) -> impl Iterator<Item=(DTRange, Option<TextOperation>)> + '_ {
        self.get_xf_operations_full(from, merging)
            .map(|(lv, mut origin_op, xf)| {
                let len = origin_op.len();
                let op: Option<TextOperation> = match xf {
                    BaseMoved(base) => {
                        origin_op.loc.span = (base..base+len).into();
                        let content = origin_op.get_content(&self.operation_ctx);
                        Some((origin_op, content).into())
                    }
                    DeleteAlreadyHappened => None,
                };
                ((lv..lv +len).into(), op)
            })
    }

    /// Get all transformed operations from the start of time.
    ///
    /// This is a shorthand for `oplog.get_xf_operations(&[], oplog.local_version)`, but
    /// I hope that future optimizations make this method way faster.
    ///
    /// See [OpLog::iter_xf_operations_from](OpLog::iter_xf_operations_from) for more information.
    pub fn iter_xf_operations(&self) -> impl Iterator<Item=(DTRange, Option<TextOperation>)> + '_ {
        self.iter_xf_operations_from(&[], self.cg.version.as_ref())
    }

    #[cfg(feature = "merge_conflict_checks")]
    pub fn has_conflicts_when_merging(&self) -> bool {
        let mut iter = TransformedOpsIter2::new(&self.cg.graph, &self.cg.agent_assignment,
                                               &self.operation_ctx, &self.operations,
                                               &[], self.cg.version.as_ref());
        for _ in &mut iter {}
        iter.concurrent_inserts_collided()
    }
}


impl ListBranch {
    /// Add everything in merge_frontier into the set..
    pub fn merge(&mut self, oplog: &ListOpLog, merge_frontier: &[LV]) {
        let mut iter = oplog.get_xf_operations_full(self.version.as_ref(), merge_frontier);
        // println!("merge '{}' at {:?} + {:?}", self.content.to_string(), self.version, merge_frontier);

        for (_lv, origin_op, xf) in &mut iter {
            // dbg!(_lv, &origin_op, &xf);
            match (origin_op.kind, xf) {
                (ListOpKind::Ins, BaseMoved(pos)) => {
                    // println!("Insert '{}' at {} (len {})", op.content, ins_pos, op.len());
                    debug_assert!(origin_op.content_pos.is_some()); // Ok if this is false - we'll just fill with junk.
                    let content = origin_op.get_content(&oplog.operation_ctx).unwrap();
                    assert!(pos <= self.content.len_chars());
                    if origin_op.loc.fwd {
                        self.content.insert(pos, content);
                    } else {
                        // We need to insert the content in reverse order.
                        let c = reverse_str(content);
                        self.content.insert(pos, &c);
                    }
                }

                (_, DeleteAlreadyHappened) => {}, // Discard.

                (ListOpKind::Del, BaseMoved(pos)) => {
                    let del_end = pos + origin_op.len();
                    debug_assert!(self.content.len_chars() >= del_end);
                    // println!("Delete {}..{} (len {}) '{}'", del_start, del_end, mut_len, to.content.slice_chars(del_start..del_end).collect::<String>());
                    self.content.remove(pos..del_end);
                }
            }
        }


        // dbg!(iter.count_range_tracker_size());

        // let expect_v = oplog.cg.graph.find_dominators_2(self.version.as_ref(), merge_frontier);
        self.version = iter.into_frontier();
        // println!("-> '{}' v {:?}", self.content.to_string(), self.version);
        // assert_eq!(self.version, expect_v);
    }

}
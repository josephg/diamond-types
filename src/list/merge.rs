use rle::HasLength;

use crate::{DTRange, LV};
use crate::frontier::FrontierRef;
use crate::list::{ListBranch, ListOpLog};
use crate::list::op_metrics::ListOpMetrics;
use crate::list::operation::{ListOpKind, TextOperation};
use crate::listmerge::merge::{reverse_str, TransformedOpsIterRaw, TransformedResultRaw, TransformedSimpleOp, TransformedSimpleOpsIter};
use crate::listmerge::merge::TransformedResult::{BaseMoved, DeleteAlreadyHappened};
use crate::listmerge::plan::M1PlanAction;
use crate::rle::KVPair;

impl ListOpLog {
    pub fn dbg_bench_make_plan(&self) {
        self.cg.graph.make_m1_plan(Some(&self.operations), &[], self.cg.version.as_ref(), false);
    }

    pub(crate) fn get_xf_operations_full(&self, from: FrontierRef, merging: FrontierRef) -> TransformedOpsIterRaw<'_> {
        TransformedOpsIterRaw::new(&self.cg.graph, &self.cg.agent_assignment,
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
        let iter: TransformedSimpleOpsIter = self.get_xf_operations_full(from, merging).into();

        iter.map(|result| {
            match result {
                TransformedSimpleOp::Apply(KVPair(start, op)) => {
                    let content = op.get_content(&self.operation_ctx);
                    let len = op.len();
                    let text_op: TextOperation = (op, content).into();
                    ((start..start + len).into(), Some(text_op))
                }
                TransformedSimpleOp::DeleteAlreadyHappened(range) => (range, None)
            }
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
        let mut iter = TransformedOpsIterRaw::new(&self.cg.graph, &self.cg.agent_assignment,
                                               &self.operation_ctx, &self.operations,
                                               &[], self.cg.version.as_ref());
        // let mut iter = TransformedOpsIter::new(&self.cg.graph, &self.cg.agent_assignment,
        //                                        &self.operation_ctx, &self.operations,
        //                                        &[], self.cg.version.as_ref());
        for _ in &mut iter {}
        iter.concurrent_inserts_collided()
    }

    pub fn dbg_iter_xf_operations_no_ff(&self) -> impl Iterator<Item=(DTRange, Option<TextOperation>)> + '_ {
        let (plan, _common) = self.cg.graph.make_m1_plan(Some(&self.operations), &[], self.cg.version.as_ref(), false);
        let iter: TransformedSimpleOpsIter = TransformedOpsIterRaw::from_plan(&self.cg.agent_assignment,
                                                                              &self.operation_ctx, &self.operations,
                                                                              plan)
            .into();

        iter.map(|result| {
            match result {
                TransformedSimpleOp::Apply(KVPair(start, op)) => {
                    let content = op.get_content(&self.operation_ctx);
                    let len = op.len();
                    let text_op: TextOperation = (op, content).into();
                    ((start..start + len).into(), Some(text_op))
                }
                TransformedSimpleOp::DeleteAlreadyHappened(range) => (range, None)
            }
        })

        // // allow_ff: false!
        // let (plan, common) = self.cg.graph.make_m1_plan(Some(&self.operations), &[], self.cg.version.as_ref(), false);
        // let iter = TransformedOpsIter::from_plan(&self.cg.graph, &self.cg.agent_assignment,
        //                                              &self.operation_ctx, &self.operations,
        //                                              plan, common);
        //
        // // Return the data in the same format as iter_xf_operations_from to make benchmarks fair.
        // iter.map(|(lv, mut origin_op, xf)| {
        //     let len = origin_op.len();
        //     let op: Option<TextOperation> = match xf {
        //         BaseMoved(base) => {
        //             origin_op.loc.span = (base..base+len).into();
        //             let content = origin_op.get_content(&self.operation_ctx);
        //             Some((origin_op, content).into())
        //         }
        //         DeleteAlreadyHappened => None,
        //     };
        //     ((lv..lv +len).into(), op)
        // })
    }

    pub fn get_ff_stats(&self) -> (usize, usize, usize) {
        let (plan, _common) = self.cg.graph.make_m1_plan(Some(&self.operations), &[], self.cg.version.as_ref(), true);

        let mut normal_advances = 0;
        let mut clears = 0;
        let mut ff = 0;

        for a in &plan.0 {
            match a {
                M1PlanAction::Apply(span) => { normal_advances += span.len(); }
                M1PlanAction::Clear => { clears += 1; }
                M1PlanAction::FF(span) => { ff += span.len(); }
                _ => {}
            }
        }

        (clears, normal_advances, ff)
    }
}


impl ListBranch {
    #[inline(always)]
    fn apply_op_at(&mut self, oplog: &ListOpLog, op: ListOpMetrics) {
        // let xf_pos = op.loc.span.start;
        match op.kind {
            ListOpKind::Ins => {
                let content = oplog.operation_ctx.get_str(ListOpKind::Ins, op.content_pos.unwrap());
                // assert!(pos <= self.content.len_chars());
                if op.loc.fwd {
                    self.content.insert(op.loc.span.start, content);
                } else {
                    // We need to insert the content in reverse order.
                    let c = reverse_str(content);
                    self.content.insert(op.loc.span.start, &c);
                }
            }
            ListOpKind::Del => {
                self.content.remove(op.loc.span.into());
            }
        }
    }

    pub fn merge(&mut self, oplog: &ListOpLog, merge_frontier: &[LV]) {
        // let mut iter = oplog.get_xf_operations_full_raw(self.version.as_ref(), merge_frontier).merge_spans();
        let iter = oplog.get_xf_operations_full(self.version.as_ref(), merge_frontier);
        // println!("merge '{}' at {:?} + {:?}", self.content.to_string(), self.version, merge_frontier);

        for xf in iter {
            // dbg!(&xf);
            // dbg!(_lv, &origin_op, &xf);
            match xf {
                TransformedResultRaw::Apply { xf_pos, op: KVPair(_, mut op) } => {
                    // dbg!(&op);
                    op.transpose_to(xf_pos);
                    self.apply_op_at(oplog, op);
                }

                TransformedResultRaw::FF(range) => {
                    // Activate *SUPER FAST MODE*.
                    for KVPair(_, op) in oplog.operations.iter_range_ctx(range, &oplog.operation_ctx) {
                        // dbg!(&op);
                        self.apply_op_at(oplog, op);
                    }
                }

                TransformedResultRaw::DeleteAlreadyHappened(_) => {} // Discard.
            }
        }
        
        self.version = oplog.cg.graph.find_dominators_2(self.version.as_ref(), merge_frontier);
    }
}
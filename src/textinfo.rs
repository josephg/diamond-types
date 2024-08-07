use jumprope::JumpRopeBuf;
use rle::HasLength;
use crate::causalgraph::graph::Graph;
use crate::dtrange::DTRange;
use crate::frontier::Frontier;
use crate::list::ListOpLog;
use crate::list::op_iter::{OpMetricsWithContent, OpMetricsIter};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::{ListOpKind, TextOperation};
use crate::listmerge::merge::reverse_str;
use crate::LV;
use crate::rle::KVPair;
use crate::rle::rle_vec::RleVec;

#[derive(Debug, Clone, Default)]
pub(crate) struct TextInfo {
    pub(crate) ctx: ListOperationCtx,
    pub(crate) ops: RleVec<KVPair<ListOpMetrics>>,
    pub(crate) frontier: Frontier,
}

impl TextInfo {
    pub fn iter_metrics_range(&self, range: DTRange) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, range)
    }
    pub fn iter_metrics(&self) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, (0..self.ops.end()).into())
    }

    pub fn iter_fast(&self) -> OpMetricsWithContent {
        self.iter_metrics().into()
    }

    pub fn iter(&self) -> impl Iterator<Item = TextOperation> + '_ {
        self.iter_fast().map(|pair| (pair.0.1, pair.1).into())
    }

    fn push_op_internal(&mut self, op: TextOperation, v_range: DTRange) {
        debug_assert_eq!(v_range.len(), op.len());

        let content_pos = op.content.as_ref().map(|content| {
            self.ctx.push_str(op.kind, content)
        });

        self.ops.push(KVPair(v_range.start, ListOpMetrics {
            loc: op.loc,
            kind: op.kind,
            content_pos
        }));
    }

    pub fn remote_push_op(&mut self, op: TextOperation, v_range: DTRange, parents: &[LV], graph: &Graph) {
        self.push_op_internal(op, v_range);
        // // TODO: Its probably simpler to just call advance_sparse() here.
        // let local_parents = graph.project_onto_subgraph_raw(
        //     subgraph_rev_iter(&self.ops),
        //     parents
        // );
        // self.frontier.advance_by_known_run(local_parents.as_ref(), v_range);
        self.frontier.advance_sparse_known_run(graph, parents, v_range);
    }

    pub fn remote_push_op_unknown_parents(&mut self, op: TextOperation, v_range: DTRange, graph: &Graph) {
        self.push_op_internal(op, v_range);
        self.frontier.advance_sparse(graph, v_range);
    }

    pub fn local_push_op(&mut self, op: TextOperation, v_range: DTRange) {
        self.push_op_internal(op, v_range);
        self.frontier.replace_with_1(v_range.last());
    }

    #[inline(always)]
    pub(crate) fn apply_op_to(&self, op: ListOpMetrics, dest: &mut JumpRopeBuf) {
        // let xf_pos = op.loc.span.start;
        match op.kind {
            ListOpKind::Ins => {
                let content = self.ctx.get_str(ListOpKind::Ins, op.content_pos.unwrap());
                // assert!(pos <= self.content.len_chars());
                if op.loc.fwd {
                    dest.insert(op.loc.span.start, content);
                } else {
                    // We need to insert the content in reverse order.
                    let c = reverse_str(content);
                    dest.insert(op.loc.span.start, &c);
                }
            }
            ListOpKind::Del => {
                dest.remove(op.loc.span.into());
            }
        }
    }

}

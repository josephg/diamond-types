use smallvec::{SmallVec, smallvec};

use rle::{AppendRle, HasLength, Trim, TrimCtx};

use crate::{DTRange, Frontier, LV};
use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::causalgraph::graph::Graph;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::frontier::local_frontier_eq;
use crate::list::buffered_iter::BufferedIter;
use crate::list::encoding::txn_trace::SpanningTreeWalker;
use crate::list::op_iter::OpMetricsIter;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::listmerge::M2Tracker;
use crate::listmerge::merge::TransformedResult;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};

#[derive(Debug)]
pub(crate) struct TransformedOpsIterOld<'a> {
    // oplog: &'a ListOpLog,
    // cg: &'a CausalGraph,
    subgraph: &'a Graph,
    aa: &'a AgentAssignment,
    op_ctx: &'a ListOperationCtx,
    ops: &'a RleVec<KVPair<ListOpMetrics>>,

    op_iter: Option<BufferedIter<OpMetricsIter<'a>>>,
    ff_mode: bool,
    // ff_idx: usize,
    did_ff: bool, // TODO: Do I really need this?

    merge_frontier: Frontier,

    common_ancestor: Frontier,
    conflict_ops: SmallVec<DTRange, 4>,
    new_ops: SmallVec<DTRange, 4>,

    next_frontier: Frontier,

    // TODO: This tracker allocates - which we don't need to do if we're FF-ing.
    phase2: Option<(M2Tracker, SpanningTreeWalker<'a>)>,
}


impl<'a> TransformedOpsIterOld<'a> {
    #[allow(unused)]
    pub(crate) fn count_range_tracker_size(&self) -> usize {
        self.phase2.as_ref()
            .map(|(tracker, _)| { tracker.old_range_tree.count_entries() })
            .unwrap_or_default()
    }

    pub(crate) fn new(subgraph: &'a Graph, aa: &'a AgentAssignment, op_ctx: &'a ListOperationCtx, ops: &'a RleVec<KVPair<ListOpMetrics>>, from_frontier: &[LV], merge_frontier: &[LV]) -> Self {
        // The strategy here looks like this:
        // We have some set of new changes to merge with a unified set of parents.
        // 1. Find the parent set of the spans to merge
        // 2. Generate the conflict set, and make a tracker for it (by iterating all the conflicting
        //    changes).
        // 3. Use OptTxnIter to iterate through the (new) merge set, merging along the way.

        // let mut diff = opset.history.diff(&self.frontier, merge_frontier);

        // First lets see what we've got. I'll divide the conflicting range into two groups:
        // - The new operations we need to merge
        // - The conflict set. Ie, stuff we need to build a tracker around.
        //
        // Both of these lists are in reverse time order(!).
        let mut new_ops: SmallVec<DTRange, 4> = smallvec![];
        let mut conflict_ops: SmallVec<DTRange, 4> = smallvec![];

        let common_ancestor = subgraph.find_conflicting(from_frontier, merge_frontier, |span, flag| {
            // Note we'll be visiting these operations in reverse order.

            // dbg!(&span, flag);
            let target = match flag {
                DiffFlag::OnlyB => &mut new_ops,
                _ => &mut conflict_ops
            };
            target.push_reversed_rle(span);
        });

        common_ancestor.debug_check_sorted();

        // dbg!(&opset.history);
        // dbg!((&new_ops, &conflict_ops, &common_ancestor));


        Self {
            subgraph,
            aa,
            op_ctx,
            ops,
            op_iter: None,
            ff_mode: true,
            did_ff: false,
            merge_frontier: Frontier::from(merge_frontier),
            common_ancestor,
            conflict_ops,
            new_ops,
            next_frontier: Frontier::from(from_frontier),
            phase2: None,
        }
    }

    pub(crate) fn into_frontier(self) -> Frontier {
        self.next_frontier
    }

    /// Returns if concurrent inserts ever collided at the same location while traversing.
    #[cfg(feature = "merge_conflict_checks")]
    pub(crate) fn concurrent_inserts_collided(&self) -> bool {
        self.phase2.as_ref().map_or(false, |(tracker, _)| {
            tracker.concurrent_inserts_collide
        })
    }
}

impl<'a> Iterator for TransformedOpsIterOld<'a> {
    /// Iterator over transformed operations. The KVPair.0 holds the original time of the operation.
    type Item = (LV, ListOpMetrics, TransformedResult);

    fn next(&mut self) -> Option<Self::Item> {
        // We're done when we've merged everything in self.new_ops.
        if self.op_iter.is_none() && self.new_ops.is_empty() { return None; }

        if self.ff_mode {
            // Keep trying to fast forward. If we have an op_iter while ff_mode is set, we can just
            // eat operations out of it without transforming, as fast as we can.
            if let Some(iter) = self.op_iter.as_mut() {
                // Keep iterating through this iter.
                if let Some(result) = iter.next() {
                    // Could ditch the iterator if its empty now...
                    // return result;
                    return Some(TransformedResult::not_moved(result));
                } else {
                    self.op_iter = None;
                    // This is needed because we could be sitting on an empty op_iter.
                    if self.new_ops.is_empty() { return None; }
                }
            }

            debug_assert!(self.op_iter.is_none());
            debug_assert!(!self.new_ops.is_empty());

            let span = self.new_ops.last().unwrap();
            let txn = self.subgraph.entries.find_packed(span.start);
            let can_ff = txn.with_parents(span.start, |parents: &[LV]| {
                local_frontier_eq(&self.next_frontier, parents)
            });

            if can_ff {
                let mut span = self.new_ops.pop().unwrap();

                let remainder = span.trim(txn.span.end - span.start);

                debug_assert!(!span.is_empty());

                self.next_frontier = Frontier::new_1(span.last());

                if let Some(r) = remainder {
                    self.new_ops.push(r);
                }
                self.did_ff = true;

                let mut iter = OpMetricsIter::new(self.ops, self.op_ctx, span);

                // Pull the first item off the iterator and keep it for later.
                // A fresh iterator should always return something!
                let result = iter.next().unwrap();
                // assert!(result.is_some());

                self.op_iter = Some(iter.into());
                // println!("FF {:?}", result);
                return Some(TransformedResult::not_moved(result));
            } else {
                self.ff_mode = false;
                if self.did_ff {
                    // Since we ate some of the ops fast-forwarding, reset conflict_ops and common_ancestor
                    // so we don't scan unnecessarily.
                    //
                    // We don't need to reset new_ops because that was updated above.

                    // This sometimes adds the FF'ed ops to the conflict_ops set so we add them to the
                    // merge set. This is a pretty bad way to do this - if we're gonna add them to
                    // conflict_ops then FF is pointless.
                    self.conflict_ops.clear();
                    self.common_ancestor = self.subgraph.find_conflicting(self.next_frontier.as_ref(), self.merge_frontier.as_ref(), |span, flag| {
                        if flag != DiffFlag::OnlyB {
                            self.conflict_ops.push_reversed_rle(span);
                        }
                    });
                }
            }
        }

        // Ok, time for serious mode.

        // For conflicting operations, we'll make a tracker starting at the common_ancestor and
        // containing the conflicting_ops set. (Which is everything that is either common, or only
        // in this branch).

        // So first we can just call .walk() to setup the tracker "hot".
        let (tracker, walker) = match self.phase2.as_mut() {
            None => {
                // First time through this code we'll end up here. Walk the conflicting
                // operations to populate the tracker and walker structures.
                let mut tracker = M2Tracker::new();
                // dbg!(&self.conflict_ops);
                let frontier = tracker.walk(
                    self.subgraph, self.aa,
                    self.op_ctx,
                    self.ops,
                    std::mem::take(&mut self.common_ancestor),
                    &self.conflict_ops,
                    None);
                // dbg!(&tracker);

                let walker = SpanningTreeWalker::new(self.subgraph, &self.new_ops, frontier);
                self.phase2 = Some((tracker, walker));
                // This is a kinda gross way to do this. TODO: Rewrite without .unwrap() somehow?
                self.phase2.as_mut().unwrap()
            },
            Some(phase2) => phase2,
        };

        let (mut pair, op_iter) = loop {
            if let Some(op_iter) = self.op_iter.as_mut() {
                if let Some(pair) = op_iter.next() {
                    break (pair, op_iter);
                }
            }

            // Otherwise advance to the next chunk from walker.

            // If this returns None, we're done.
            let walk = walker.next()?;

            // dbg!(&walk);
            for range in walk.retreat {
                tracker.retreat_by_range(range);
            }

            for range in walk.advance_rev.into_iter().rev() {
                tracker.advance_by_range(range);
            }

            // dbg!(&walk.consume, &tracker);
            assert!(!walk.consume.is_empty());

            // Only really advancing the frontier so we can consume into it. The resulting frontier
            // is interesting in lots of places.
            //
            // The walker can be unwrapped into its inner frontier, but that won't include
            // everything. (TODO: Look into fixing that?)
            self.next_frontier.advance(self.subgraph, walk.consume);
            self.op_iter = Some(OpMetricsIter::new(self.ops, self.op_ctx, walk.consume).into());
        };

        // Ok, try to consume as much as we can from pair.
        let span = self.aa.local_span_to_agent_span(pair.span());
        let len = span.len().min(pair.len());

        let (consumed_here, xf_result) = tracker.apply(self.aa, self.op_ctx, &pair, len, span.agent);

        let remainder = pair.trim_ctx(consumed_here, self.op_ctx);

        // (Time, OperationInternal, TransformedResult)
        let result = (pair.0, pair.1, xf_result);

        if let Some(r) = remainder {
            op_iter.push_back(r);
        }

        Some(result)
        // TODO: Also FF at the end!
    }
}
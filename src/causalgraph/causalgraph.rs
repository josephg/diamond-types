use smallvec::SmallVec;

use rle::{HasLength, MergableSpan, SplitableSpan};
use rle::zip::rle_zip;

use crate::{AgentId, CausalGraph, LV};
use crate::causalgraph::*;
use crate::causalgraph::agent_assignment::remote_ids::{RemoteFrontier, RemoteFrontierOwned};
use crate::causalgraph::agent_span::AgentSpan;
use crate::causalgraph::entry::CGEntry;
use crate::causalgraph::graph::GraphEntrySimple;
use crate::rle::{RleSpanHelpers, RleVec};

impl CausalGraph {
    pub fn new() -> Self {
        Self::default()
    }

    // There's a lot of methods in agent_assignment that we could wrap here. This is my one
    // admission to practicality.
    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.agent_assignment.get_or_create_agent_id(name)
    }

    pub fn num_agents(&self) -> AgentId {
        self.agent_assignment.client_data.len() as AgentId
    }

    pub(crate) fn len_assignment(&self) -> usize {
        self.agent_assignment.len()
    }

    pub(crate) fn len_history(&self) -> usize {
        self.graph.len()
    }

    /// Get the number of operations. This method is only valid when the history and assignment
    /// lengths are the same.
    ///
    /// TODO: Consider calling this something other than len(). We're essentially returning the next
    /// local version.
    pub fn len(&self) -> usize {
        let len = self.len_assignment();
        debug_assert_eq!(len, self.len_history());
        len
    }

    pub fn is_empty(&self) -> bool {
        self.agent_assignment.is_empty()
    }

    // #[allow(unused)]
    // pub(crate) fn map_parents(&self, crdt_parents: &[AgentVersion]) -> Frontier {
    //     // TODO: Make a try_ version of this.
    //     Frontier::from_unsorted_iter(crdt_parents.iter()
    //         .map(|p| self.try_agent_version_to_lv(*p).unwrap())
    //     )
    // }

    pub(crate) fn check_flat(&self) {
        assert_eq!(self.len_assignment(), self.len_history());
    }

    // TODO: These functions look incredibly similar! We need both of them because of the borrow
    // checker. I could write a function which takes parents: Option<&[LV]> but that'll make the
    // benchmarks slower.
    pub fn assign_local_op_with_parents(&mut self, parents: &[LV], agent: AgentId, num: usize) -> DTRange {
        if cfg!(debug_assertions) { self.check_flat(); }

        let start = self.len();
        let span = (start .. start + num).into();

        self.agent_assignment.assign_lv_to_client_next_seq(agent, span);
        self.graph.push(parents, span);
        self.version.advance_by_known_run(parents, span);
        span
    }

    pub(crate) fn assign_span(&mut self, agent: AgentId, parents: &[LV], span: DTRange) {
        debug_assert_eq!(span.start, self.len_assignment());
        self.assign_local_op_with_parents(parents, agent, span.len());
    }

    pub fn assign_local_op(&mut self, agent: AgentId, num: usize) -> DTRange {
        // This is gross. Its a barely changed copy+paste job of assign_local_op_with_parents.
        if cfg!(debug_assertions) { self.check_flat(); }

        let start = self.len();
        let span = (start .. start + num).into();

        self.agent_assignment.assign_lv_to_client_next_seq(agent, span);
        self.graph.push(self.version.as_ref(), span);
        self.version.replace_with_1(span.last());
        span
    }

    /// An alternate variant of merge_and_assign which is slightly faster, but will panic if the
    /// specified span is already included in the causal graph.
    pub fn merge_and_assign_nonoverlapping(&mut self, parents: &[LV], span: AgentSpan) -> DTRange {
        let time_start = self.len();

        // Agent ID must have already been assigned.
        let client_data = &mut self.agent_assignment.client_data[span.agent as usize];

        // Make sure the time isn't already assigned. Can I elide this check in release mode?
        // Note I only need to check the start of the seq_range.
        let (x, _offset) = client_data.lv_for_seq.find_sparse(span.seq_range.start);
        if let Err(range) = x {
            assert!(range.end >= span.seq_range.end, "Time range already assigned");
        } else {
            panic!("Time range already assigned");
        }

        let time_span = (time_start .. time_start + span.len()).into();

        // Almost always appending to the end but its possible for the same agent ID to be used on
        // two concurrent branches, then transmitted in a different order.
        client_data.lv_for_seq.insert(KVPair(span.seq_range.start, time_span));
        self.agent_assignment.client_with_lv.push(KVPair(time_start, span));
        self.graph.push(parents, time_span);
        self.version.advance_by_known_run(parents, time_span);
        time_span
    }

    /// This method merges the specified entry into the causal graph. The incoming data might
    /// already be known by the causal graph.
    ///
    /// This takes a CGEntry rather than a CRDTSpan because that makes the overlap calculations much
    /// easier (its constant time rather than needing to loop, because subsequent ops in the region)
    /// all depend on the first).
    ///
    /// Method returns the new span of local versions. Note this span might be smaller than `span`
    /// if some or all of the operations are already known by the causal graph.
    pub fn merge_and_assign(&mut self, parents: &[LV], span: AgentSpan) -> DTRange {
        let time_start = self.len();

        // The agent ID must already be assigned.
        let client_data = &mut self.agent_assignment.client_data[span.agent as usize];

        // We're looking to see how much we can assign, which is the (backwards) size of the empty
        // span from the last item.

        // This is quite subtle. There's 3 cases here:
        // 1. The new span is entirely known in the causal graph. Discard it.
        // 2. The new span is entirely unknown in the causal graph. This is the most likely case.
        //    Append all of it.
        // 3. There's some overlap. The overlap must be at the start of the entry, because all of
        //    each item's parents must be known.

        match client_data.lv_for_seq.find_index(span.seq_range.last()) {
            Ok(_idx) => {
                // If we know the last ID, the entire entry is known. Case 1 - discard and return.
                (time_start..time_start).into()
            }
            Err(idx) => {
                // idx is the index where the item could be inserted to maintain order.
                if idx >= 1 { // if idx == 0, there's no overlap anyway.
                    let prev_entry = &mut client_data.lv_for_seq.0[idx - 1];
                    let previous_end = prev_entry.end();

                    if previous_end >= span.seq_range.start {
                        // In this case we need to trim the incoming edit and insert it. But we
                        // already have the previous edit. We need to extend it.
                        let actual_len = span.seq_range.end - previous_end;
                        let time_span: DTRange = (time_start..time_start + actual_len).into();
                        let new_entry = KVPair(previous_end, time_span);

                        self.agent_assignment.client_with_lv.push(KVPair(time_start, AgentSpan {
                            agent: span.agent,
                            seq_range: (prev_entry.end()..span.seq_range.end).into()
                        }));

                        if previous_end > span.seq_range.start {
                            // Case 3 - there's some overlap.
                            let parents = &[prev_entry.1.last()];
                            self.version.advance_by_known_run(parents, time_span);
                            self.graph.push(parents, time_span);
                        } else {
                            // I don't like the duplication here but ... ehhh.
                            self.version.advance_by_known_run(parents, time_span);
                            self.graph.push(parents, time_span);
                        }

                        if prev_entry.can_append(&new_entry) {
                            prev_entry.append(new_entry);
                        } else {
                            client_data.lv_for_seq.0.insert(idx, new_entry);
                        }

                        return time_span;
                    }
                }

                // We know it can't combine with the previous element.
                let time_span = (time_start..time_start + span.len()).into();
                client_data.lv_for_seq.0.insert(idx, KVPair(span.seq_range.start, time_span));
                self.agent_assignment.client_with_lv.push(KVPair(time_start, span));
                self.graph.push(parents, time_span);
                self.version.advance_by_known_run(parents, time_span);
                time_span
            }
        }
    }

    /// Iterate through history entries
    pub fn iter_parents(&self) -> impl Iterator<Item=GraphEntrySimple> + '_ {
        self.graph.iter()
    }

    pub fn simple_entry_at(&self, v: DTRange) -> CGEntry {
        let entry = self.graph.entries.find_packed(v.start);
        let parents = entry.clone_parents_at_version(v.start);

        let mut av = self.agent_assignment.local_span_to_agent_span(v);

        // The entry needs to be the size of min(av, entry).
        let usable_entry_len = entry.span.end - v.start;
        if usable_entry_len < av.len() {
            av.truncate(usable_entry_len);
        }

        CGEntry {
            start: v.start,
            parents,
            span: av,
        }
    }

    pub fn iter_range(&self, range: DTRange) -> impl Iterator<Item=CGEntry> + '_ {
        let parents = self.graph.iter_range(range);
        let aa = self.agent_assignment.client_with_lv.iter_range(range)
            .map(|KVPair(_, data)| data);

        rle_zip(parents, aa).map(|(parents, span): (GraphEntrySimple, AgentSpan)| {
            debug_assert_eq!(parents.len(), span.len());

            CGEntry {
                start: parents.span.start,
                parents: parents.parents,
                span
            }
        })
    }

    pub fn make_simple_graph(&self) -> RleVec<GraphEntrySimple> {
        self.graph.make_simple_graph(self.version.as_ref())
    }

    pub fn remote_frontier(&self) -> RemoteFrontier {
        self.agent_assignment.local_to_remote_frontier(self.version.as_ref())
    }

    pub fn remote_frontier_owned(&self) -> RemoteFrontierOwned {
        self.agent_assignment.local_to_remote_frontier_owned(self.version.as_ref())
    }

    #[allow(unused)]
    pub fn iter(&self) -> impl Iterator<Item=CGEntry> + '_ {
        self.iter_range((0..self.len()).into())
    }

    pub fn diff_since(&self, frontier: &[LV]) -> SmallVec<DTRange, 4> {
        let mut result = self.diff_since_rev(frontier);
        result.reverse();
        result
    }

    pub fn diff_since_rev(&self, frontier: &[LV]) -> SmallVec<DTRange, 4> {
        let (only_a, only_b) = self.graph.diff_rev(frontier, self.version.as_ref());
        debug_assert!(only_a.is_empty());
        only_b
    }
}

#[cfg(test)]
mod tests {
    use crate::CausalGraph;

    #[test]
    fn merge_and_assign_updates_version() {
        // Regression.

        let mut cg = CausalGraph::new();
        let agent = cg.get_or_create_agent_id("seph");
        cg.merge_and_assign(&[], (agent, 0..10).into());
        cg.dbg_check(true);

        cg.merge_and_assign(&[4], (agent, 5..10).into());
        cg.dbg_check(true);

        cg.merge_and_assign(&[4], (agent, 5..15).into());
        cg.dbg_check(true);
    }
}
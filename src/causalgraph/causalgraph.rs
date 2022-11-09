use std::cmp::Ordering;
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};
use rle::zip::rle_zip;
use crate::{AgentId, CausalGraph, LV};
use crate::causalgraph::*;
use crate::causalgraph::entry::CGEntry;
use crate::causalgraph::parents::ParentsEntrySimple;
use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};
use crate::frontier::sort_frontier;
use crate::rle::RleSpanHelpers;

impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        // Equivalent to this. I'm not sure which I prefer...
        // self.item_times
        //     .last()
        //     .map(|last| last.end())
        //     .unwrap_or(0)

        if let Some(last) = self.item_times.last() {
            last.end()
        } else { 0 }
    }

    // pub fn get_last_seq(&self) -> Option<usize> {
    //     self.item_times
    //         .last()
    //         .map(|last_entry| last_entry.last())
    // }

    pub fn is_empty(&self) -> bool {
        self.item_times.is_empty()
    }

    #[inline]
    pub(crate) fn try_seq_to_lv(&self, seq: usize) -> Option<LV> {
        let (entry, offset) = self.item_times.find_with_offset(seq)?;
        Some(entry.1.start + offset)
    }

    pub(crate) fn seq_to_lv(&self, seq: usize) -> LV {
        self.try_seq_to_lv(seq).unwrap()
    }

    /// Note the returned timespan might be shorter than seq_range.
    pub fn try_seq_to_lv_span(&self, seq_range: DTRange) -> Option<DTRange> {
        let (KVPair(_, entry), offset) = self.item_times.find_with_offset(seq_range.start)?;

        let start = entry.start + offset;
        let end = usize::min(entry.end, start + seq_range.len());
        Some(DTRange { start, end })
    }

    pub fn seq_to_time_span(&self, seq_range: DTRange) -> DTRange {
        self.try_seq_to_lv_span(seq_range).unwrap()
    }
}

impl CausalGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        self.client_data.iter()
            .position(|client_data| client_data.name == name)
            .map(|id| id as AgentId)
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        // TODO: -> Result or something so this can be handled.
        if name == "ROOT" { panic!("Agent ID 'ROOT' is reserved"); }

        if let Some(id) = self.get_agent_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: SmartString::from(name),
                item_times: RleVec::new()
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    pub fn get_agent_name(&self, agent: AgentId) -> &str {
        self.client_data[agent as usize].name.as_str()
    }

    pub(crate) fn len_assignment(&self) -> usize {
        self.client_with_localtime.end()
    }

    pub(crate) fn len_history(&self) -> usize {
        self.parents.entries.end()
    }

    /// Get the number of operations. This method is only valid when the history and assignment
    /// lengths are the same.
    pub fn len(&self) -> usize {
        let len = self.len_assignment();
        debug_assert_eq!(len, self.len_history());
        len
    }

    pub fn is_empty(&self) -> bool {
        self.client_with_localtime.is_empty()
    }

    pub(crate) fn lv_to_agent_version(&self, version: LV) -> AgentVersion {
        debug_assert_ne!(version, usize::MAX);
        self.client_with_localtime.get(version)
    }

    pub(crate) fn lv_span_to_agent_span(&self, version: DTRange) -> AgentSpan {
        debug_assert_ne!(version.start, usize::MAX);

        let (loc, offset) = self.client_with_localtime.find_packed_with_offset(version.start);
        let start = loc.1.seq_range.start + offset;
        let end = usize::min(loc.1.seq_range.end, start + version.len());
        AgentSpan {
            agent: loc.1.agent,
            seq_range: DTRange { start, end }
        }
    }

    pub(crate) fn try_agent_version_to_lv(&self, (agent, seq): AgentVersion) -> Option<LV> {
        debug_assert_ne!(agent, AgentId::MAX);

        self.client_data.get(agent as usize).and_then(|c| {
            c.try_seq_to_lv(seq)
        })
    }

    #[allow(unused)]
    pub(crate) fn map_parents(&self, crdt_parents: &[AgentVersion]) -> Frontier {
        // TODO: Make a try_ version of this.
        Frontier::from_unsorted_iter(crdt_parents.iter()
            .map(|p| self.try_agent_version_to_lv(*p).unwrap())
        )
    }

    pub(crate) fn check_flat(&self) {
        assert_eq!(self.len_assignment(), self.len_history());
    }

    /// span is the local timespan we're assigning to the named agent.
    fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: DTRange) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.client_with_localtime.push(KVPair(span.start, AgentSpan {
            agent,
            seq_range: DTRange { start: next_seq, end: next_seq + span.len() },
        }));
    }

    // TODO: These functions look incredibly similar! We need both of them because of the borrow
    // checker. I could write a function which takes parents: Option<&[LV]> but that'll make the
    // benchmarks slower.
    pub fn assign_local_op_with_parents(&mut self, parents: &[LV], agent: AgentId, num: usize) -> DTRange {
        if cfg!(debug_assertions) { self.check_flat(); }

        let start = self.len();
        let span = (start .. start + num).into();

        self.assign_next_time_to_client_known(agent, span);
        self.parents.push(parents, span);
        self.version.advance_by_known_run(parents, span);
        span
    }

    pub(crate) fn assign_span(&mut self, agent: AgentId, parents: &[LV], span: DTRange) {
        debug_assert_eq!(span.start, self.len_assignment());
        self.assign_local_op_with_parents(parents, agent, span.len());
    }

    pub fn assign_local_op(&mut self, agent: AgentId, num: usize) -> DTRange {
        if cfg!(debug_assertions) { self.check_flat(); }

        let start = self.len();
        let span = (start .. start + num).into();

        self.assign_next_time_to_client_known(agent, span);
        self.parents.push(self.version.as_ref(), span);
        self.version.replace_with_1(span.last());
        span
    }

    /// An alternate variant of merge_and_assign which is slightly faster, but will panic if the
    /// specified span is already included in the causal graph.
    pub fn merge_and_assign_nonoverlapping(&mut self, parents: &[LV], span: AgentSpan) -> DTRange {
        let time_start = self.len();

        // Agent ID must have already been assigned.
        let client_data = &mut self.client_data[span.agent as usize];

        // Make sure the time isn't already assigned. Can I elide this check in release mode?
        // Note I only need to check the start of the seq_range.
        let (x, _offset) = client_data.item_times.find_sparse(span.seq_range.start);
        if let Err(range) = x {
            assert!(range.end >= span.seq_range.end, "Time range already assigned");
        } else {
            panic!("Time range already assigned");
        }

        let time_span = (time_start .. time_start + span.len()).into();

        // Almost always appending to the end but its possible for the same agent ID to be used on
        // two concurrent branches, then transmitted in a different order.
        client_data.item_times.insert(KVPair(span.seq_range.start, time_span));
        self.client_with_localtime.push(KVPair(time_start, span));
        self.parents.push(parents, time_span);
        time_span
    }

    /// This method merges the specified entry into the causal graph. The incoming data might
    /// already be known by the causal graph.
    ///
    /// This takes a CGEntry rather than a CRDTSpan because that makes the overlap calculations much
    /// easier (its constant time rather than needing to loop, because subsequent ops in the region)
    /// all depend on the first).
    ///
    /// Method returns the
    pub fn merge_and_assign(&mut self, mut parents: &[LV], mut span: AgentSpan) -> DTRange {
        let time_start = self.len();

        // The agent ID must already be assigned.
        let client_data = &mut self.client_data[span.agent as usize];

        // We're looking to see how much we can assign, which is the (backwards) size of the empty
        // span from the last item.

        // This is quite subtle. There's 3 cases here:
        // 1. The new span is entirely known in the causal graph. Discard it.
        // 2. The new span is entirely unknown in the causal graph. This is the most likely case.
        //    Append all of it.
        // 3. There's some overlap. The overlap must be at the start of the entry, because all of
        //    each item's parents must be known.

        match client_data.item_times.find_index(span.seq_range.last()) {
            Ok(idx) => {
                // If we know the last ID, the entire entry is known. Case 1 - discard and return.
                (time_start..time_start).into()
            }
            Err(idx) => {
                // idx is the index where the item could be inserted to maintain order.
                if idx >= 1 { // if idx == 0, there's no overlap anyway.
                    let prev_entry = &mut client_data.item_times.0[idx - 1];
                    let previous_end = prev_entry.end();

                    if previous_end >= span.seq_range.start {
                        // In this case we need to trim the incoming edit and insert it. But we
                        // already have the previous edit. We need to extend it.
                        let actual_len = span.seq_range.end - previous_end;
                        let time_span: DTRange = (time_start..time_start + actual_len).into();
                        let new_entry = KVPair(previous_end, time_span);

                        self.client_with_localtime.push(KVPair(time_start, AgentSpan {
                            agent: span.agent,
                            seq_range: (prev_entry.end()..span.seq_range.end).into()
                        }));

                        if previous_end > span.seq_range.start {
                            // Case 3 - there's some overlap.
                            self.parents.push(&[prev_entry.1.last()], time_span);
                        } else {
                            self.parents.push(parents, time_span);
                        }

                        if prev_entry.can_append(&new_entry) {
                            prev_entry.append(new_entry);
                        } else {
                            client_data.item_times.0.insert(idx, new_entry);
                        }

                        return time_span;
                    }
                }

                // We know it can't combine with the previous element.
                let time_span = (time_start..time_start + span.len()).into();
                client_data.item_times.0.insert(idx, KVPair(span.seq_range.start, time_span));
                self.client_with_localtime.push(KVPair(time_start, span));
                self.parents.push(parents, time_span);
                time_span
            }
        }
    }

    /// This is used to break ties.
    pub(crate) fn tie_break_crdt_versions(&self, v1: AgentVersion, v2: AgentVersion) -> Ordering {
        if v1 == v2 { Ordering::Equal }
        else {
            let c1 = &self.client_data[v1.0 as usize];
            let c2 = &self.client_data[v2.0 as usize];

            c1.name.cmp(&c2.name)
                .then(v1.1.cmp(&v2.1))
        }
    }

    pub(crate) fn tie_break_versions(&self, v1: LV, v2: LV) -> Ordering {
        if v1 == v2 { Ordering::Equal }
        else {
            self.tie_break_crdt_versions(
                self.lv_to_agent_version(v1),
                self.lv_to_agent_version(v2)
            )
        }
    }

    /// Iterate through history entries
    pub fn iter_parents(&self) -> impl Iterator<Item=ParentsEntrySimple> + '_ {
        self.parents.entries.iter().map(|e| e.into())
    }

    pub fn iter_range(&self, range: DTRange) -> impl Iterator<Item=CGEntry> + '_ {
        let parents = self.parents.iter_range(range);
        let aa = self.client_with_localtime.iter_range_packed(range)
            .map(|KVPair(_, data)| data);

        rle_zip(parents, aa).map(|(parents, span): (ParentsEntrySimple, AgentSpan)| {
            debug_assert_eq!(parents.len(), span.len());

            CGEntry {
                start: parents.span.start,
                parents: parents.parents,
                span
            }
        })
    }

    #[allow(unused)]
    pub fn iter(&self) -> impl Iterator<Item=CGEntry> + '_ {
        self.iter_range((0..self.len()).into())
    }
}
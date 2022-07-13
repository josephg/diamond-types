use std::cmp::Ordering;
use rle::{HasLength, Searchable};
use crate::{AgentId, CausalGraph, ROOT_AGENT, ROOT_TIME, Time};
use crate::causalgraph::*;
use crate::causalgraph::parents::ParentsEntrySimple;
use crate::frontier::clean_version;
use crate::remotespan::{CRDT_DOC_ROOT, CRDTGuid};
use crate::rle::RleSpanHelpers;


impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        if let Some(last) = self.item_times.last() {
            last.end()
        } else { 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.item_times.is_empty()
    }

    #[inline]
    pub(crate) fn try_seq_to_time(&self, seq: usize) -> Option<Time> {
        let (entry, offset) = self.item_times.find_with_offset(seq)?;
        Some(entry.1.start + offset)
    }

    pub(crate) fn seq_to_time(&self, seq: usize) -> Time {
        self.try_seq_to_time(seq).unwrap()
    }

    // /// Note the returned timespan might be shorter than seq_range.
    // pub fn try_seq_to_time_span(&self, seq_range: TimeSpan) -> Option<TimeSpan> {
    //     let (KVPair(_, entry), offset) = self.item_orders.find_with_offset(seq_range.start)?;
    //
    //     let start = entry.start + offset;
    //     let end = usize::min(entry.end, start + seq_range.len());
    //     Some(TimeSpan { start, end })
    // }
}

impl CausalGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(ROOT_AGENT) }
        else {
            self.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
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

    pub(crate) fn len_assignment(&self) -> usize {
        self.client_with_localtime.end()
        // if let Some(last) = self.client_with_localtime.last() {
        //     last.end()
        // } else { 0 }
    }

    pub(crate) fn len_history(&self) -> usize {
        self.history.entries.end()
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

    pub fn version_to_crdt_id(&self, time: usize) -> CRDTGuid {
        if time == ROOT_TIME { CRDT_DOC_ROOT }
        else {
            let (loc, offset) = self.client_with_localtime.find_packed_with_offset(time);
            loc.1.at_offset(offset as usize)
        }
    }

    pub fn try_crdt_id_to_version(&self, id: CRDTGuid) -> Option<Time> {
        if id.agent == ROOT_AGENT {
            debug_assert_eq!(id.seq, 0);
            Some(ROOT_TIME)
        } else {
            self.client_data.get(id.agent as usize).and_then(|c| {
                c.try_seq_to_time(id.seq)
            })
        }
    }

    pub(crate) fn map_parents(&self, crdt_parents: &[CRDTGuid]) -> LocalVersion {
        // TODO: Make a try_ version of this.
        let mut parents = crdt_parents.iter()
            .map(|p| self.try_crdt_id_to_version(*p).unwrap()).collect();
        clean_version(&mut parents);
        parents
    }

    pub(crate) fn check_flat(&self) {
        assert_eq!(self.len_assignment(), self.len_history());
    }

    pub(crate) fn assign_times_to_agent(&mut self, span: CRDTSpan) -> DTRange {
        let time_start = self.len_assignment();
        let client_data = &mut self.client_data[span.agent as usize];

        // Make sure the time isn't already assigned. I probably only need this in debug mode?
        let (x, offset) = client_data.item_times.find_sparse(span.seq_range.start);
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

        time_span
    }

    /// span is the local timespan we're assigning to the named agent.
    pub(crate) fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: DTRange) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.client_with_localtime.push(KVPair(span.start, CRDTSpan {
            agent,
            seq_range: DTRange { start: next_seq, end: next_seq + span.len() },
        }));
    }

    pub(crate) fn assign_op(&mut self, parents: &[Time], agent: AgentId, num: usize) -> DTRange {
        if cfg!(debug_assertions) { self.check_flat(); }

        let start = self.len();
        let span = (start .. start + num).into();

        self.assign_next_time_to_client_known(agent, span);
        self.history.insert(parents, span);

        span
    }

    /// This is used to break ties.
    pub(crate) fn tie_break_crdt_versions(&self, v1: CRDTGuid, v2: CRDTGuid) -> Ordering {
        if v1 == v2 { return Ordering::Equal; }
        else {
            let c1 = &self.client_data[v1.agent as usize];
            let c2 = &self.client_data[v2.agent as usize];

            c1.name.cmp(&c2.name)
                .then(v1.seq.cmp(&v2.seq))
        }
    }

    pub(crate) fn tie_break_versions(&self, v1: Time, v2: Time) -> Ordering {
        if v1 == v2 { Ordering::Equal }
        else {
            self.tie_break_crdt_versions(
                self.version_to_crdt_id(v1),
                self.version_to_crdt_id(v2)
            )
        }
    }

    /// Iterate through history entries
    pub fn iter_history(&self) -> impl Iterator<Item=ParentsEntrySimple> + '_ {
        self.history.entries.iter().map(|e| e.into())
    }
}
use std::cmp::Ordering;
use smartstring::alias::String as SmartString;
use rle::HasLength;
use crate::causalgraph::agent_span::{AgentSpan, AgentVersion};
use crate::{AgentId, DTRange, LV};
use crate::rle::{KVPair, RleSpanHelpers, RleVec};

pub mod remote_ids;

#[derive(Clone, Debug)]
pub(crate) struct ClientData {
    /// Used to map from client's name / hash to its numerical ID.
    pub(crate) name: SmartString,

    /// This is a packed RLE in-order list of all operations from this client.
    ///
    /// Each entry in this list is grounded at the client's sequence number and maps to the span of
    /// local time entries.
    ///
    /// A single agent ID might be used to modify multiple concurrent branches. Because of this, and
    /// the propensity of diamond types to reorder operations for performance, the
    /// time spans here will *almost* always (but not always) be monotonically increasing. Eg, they
    /// might be ordered as (0, 2, 1). This will only happen when changes are concurrent. The order
    /// of time spans must always obey the partial order of changes. But it will not necessarily
    /// agree with the order amongst time spans.
    pub(crate) item_times: RleVec<KVPair<DTRange>>,
}

#[derive(Debug, Clone, Default)]
pub struct AgentAssignment {

    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map Local time -> External CRDT locations.
    ///
    /// List is packed.
    pub(crate) client_with_localtime: RleVec<KVPair<AgentSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Order numbers.
    pub(crate) client_data: Vec<ClientData>,

}


impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        self.item_times.end()
    }

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

impl AgentAssignment {
    pub fn new() -> Self { Self::default() }

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

    pub fn len(&self) -> usize {
        self.client_with_localtime.end()
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

    /// span is the local timespan we're assigning to the named agent.
    pub(crate) fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: DTRange) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.client_with_localtime.push(KVPair(span.start, AgentSpan {
            agent,
            seq_range: DTRange { start: next_seq, end: next_seq + span.len() },
        }));
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
}

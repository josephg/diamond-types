use std::ops::Range;
use crate::list::ListOpLog;
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::dtrange::DTRange;
use crate::{CausalGraph, LocalFrontier, LV};
use crate::frontier::sort_frontier;
use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};

/// This file contains utilities to convert remote IDs to local time and back.
///
/// Remote IDs are IDs you can pass to a remote peer.

/// External equivalent of CRDTId
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(from = "RemoteIdTuple", into = "RemoteIdTuple"))]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: usize,
}

/// This is used to flatten `[agent, seq]` into a tuple for serde serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct RemoteIdTuple(SmartString, usize);

impl From<RemoteIdTuple> for RemoteId {
    fn from(f: RemoteIdTuple) -> Self {
        Self { agent: f.0, seq: f.1 }
    }
}
impl From<RemoteId> for RemoteIdTuple {
    fn from(id: RemoteId) -> Self {
        RemoteIdTuple(id.agent, id.seq)
    }
}

impl<S> From<(S, usize)> for RemoteId where S: Into<SmartString> {
    fn from(r: (S, usize)) -> Self {
        Self {
            agent: r.0.into(),
            seq: r.1
        }
    }
}

/// External equivalent of CRDTSpan.
/// TODO: Do the same treatment here for seq_range.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RemoteIdSpan {
    pub agent: SmartString,
    pub seq_range: DTRange,
}

// So we need methods for:
//
// Remote id -> time
// time -> remote id

// frontier -> [remote id]
// [remote id] -> frontier

// (not done yet)
// timespan -> remote id span
// remote id span -> timespan

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ConversionError {
    UnknownAgent,
    SeqInFuture,
}

impl CausalGraph {
    pub fn try_remote_to_local_time(&self, id: &RemoteId) -> Result<LV, ConversionError> {
        let agent = self.get_agent_id(id.agent.as_str())
            .ok_or(ConversionError::UnknownAgent)?;

        self.client_data[agent as usize]
            .try_seq_to_lv(id.seq)
            .ok_or(ConversionError::SeqInFuture)
    }

    /// This panics if the ID isn't known to the document.
    pub fn remote_to_local_time(&self, id: &RemoteId) -> LV {
        let agent = self.get_agent_id(id.agent.as_str()).unwrap();
        self.client_data[agent as usize].seq_to_lv(id.seq)
    }

    fn crdt_id_to_remote(&self, loc: AgentVersion) -> RemoteId {
        RemoteId {
            agent: self.get_agent_name(loc.agent).into(),
            seq: loc.seq
        }
    }

    fn crdt_span_to_remote(&self, loc: AgentSpan) -> RemoteIdSpan {
        RemoteIdSpan {
            agent: self.get_agent_name(loc.agent).into(),
            seq_range: loc.seq_range
        }
    }

    pub fn local_to_remote_time(&self, time: LV) -> RemoteId {
        let crdt_id = self.lv_to_agent_version(time);
        self.crdt_id_to_remote(crdt_id)
    }

    /// **NOTE:** This method will return a timespan with length min(time, agent_time). The
    /// resulting length will NOT be guaranteed to be the same as the input.
    pub fn local_to_remote_time_span(&self, v: DTRange) -> RemoteIdSpan {
        let crdt_span = self.lv_span_to_agent_span(v);
        self.crdt_span_to_remote(crdt_span)
    }

    pub fn try_remote_to_local_version<'a, I: Iterator<Item=&'a RemoteId> + 'a>(&self, ids_iter: I) -> Result<LocalFrontier, ConversionError> {
        let mut version: LocalFrontier = ids_iter
            .map(|remote_id| self.try_remote_to_local_time(remote_id))
            .collect::<Result<LocalFrontier, ConversionError>>()?;

        sort_frontier(&mut version);
        Ok(version)
    }

    pub fn remote_to_local_version<'a, I: Iterator<Item=&'a RemoteId> + 'a>(&self, ids_iter: I) -> LocalFrontier {
        let mut version: LocalFrontier = ids_iter
            .map(|remote_id| self.remote_to_local_time(remote_id))
            .collect();

        sort_frontier(&mut version);
        version
    }

    pub fn local_to_remote_version(&self, local_version: &[LV]) -> SmallVec<[RemoteId; 4]> {
        // Could return an impl Iterator here instead.
        local_version
            .iter()
            .map(|time| self.local_to_remote_time(*time))
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::causalgraph::remote_ids::RemoteId;
    use crate::CausalGraph;

    #[test]
    fn id_smoke_test() {
        let mut cg = CausalGraph::new();
        cg.get_or_create_agent_id("seph");
        cg.get_or_create_agent_id("mike");
        cg.assign_local_op(&[], 0, 2);
        cg.assign_local_op(&[], 1, 4);

        assert_eq!(0, cg.remote_to_local_time(&RemoteId {
            agent: "seph".into(),
            seq: 0
        }));
        assert_eq!(1, cg.remote_to_local_time(&RemoteId {
            agent: "seph".into(),
            seq: 1
        }));

        assert_eq!(2, cg.remote_to_local_time(&RemoteId {
            agent: "mike".into(),
            seq: 0
        }));

        for time in 0..cg.len() {
            let id = cg.local_to_remote_time(time);
            let expect_time = cg.remote_to_local_time(&id);
            assert_eq!(time, expect_time);
        }

        // assert_eq!(oplog.get_vector_clock().as_slice(), &[
        //     RemoteId {
        //         agent: "seph".into(),
        //         seq: 2,
        //     },
        //     RemoteId {
        //         agent: "mike".into(),
        //         seq: 4,
        //     },
        // ]);
    }

    #[test]
    fn remote_versions_can_be_empty() {
        let cg = CausalGraph::new();
        assert!(cg.remote_to_local_version(std::iter::empty()).is_empty());
    }
}
use crate::list::ListOpLog;
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::dtrange::DTRange;
use crate::{CausalGraph, CRDTSpan, LocalVersion, ROOT_AGENT, ROOT_TIME, Time};
use crate::frontier::clean_version;
use crate::causalgraph::remotespan::CRDTGuid;

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
    pub fn try_remote_to_local_time(&self, id: &RemoteId) -> Result<Time, ConversionError> {
        let agent = self.get_agent_id(id.agent.as_str())
            .ok_or(ConversionError::UnknownAgent)?;

        if agent == ROOT_AGENT { Ok(ROOT_TIME) }
        else {
            self.client_data[agent as usize]
                .try_seq_to_time(id.seq)
                .ok_or(ConversionError::SeqInFuture)
        }
    }

    /// This panics if the ID isn't known to the document.
    pub fn remote_to_local_time(&self, id: &RemoteId) -> Time {
        let agent = self.get_agent_id(id.agent.as_str()).unwrap();

        if agent == ROOT_AGENT { ROOT_TIME }
        else {
            self.client_data[agent as usize].seq_to_time(id.seq)
        }
    }

    fn crdt_id_to_remote(&self, loc: CRDTGuid) -> RemoteId {
        RemoteId {
            agent: self.get_agent_name(loc.agent).into(),
            seq: loc.seq
        }
    }

    fn crdt_span_to_remote(&self, loc: CRDTSpan) -> RemoteIdSpan {
        RemoteIdSpan {
            agent: self.get_agent_name(loc.agent).into(),
            seq_range: loc.seq_range
        }
    }

    pub fn local_to_remote_time(&self, time: Time) -> RemoteId {
        let crdt_id = self.version_to_crdt_id(time);
        self.crdt_id_to_remote(crdt_id)
    }

    /// **NOTE:** This method will return a timespan with length min(time, agent_time). The
    /// resulting length will NOT be guaranteed to be the same as the input.
    pub fn local_to_remote_time_span(&self, v: DTRange) -> RemoteIdSpan {
        let crdt_span = self.version_span_to_crdt_span(v);
        self.crdt_span_to_remote(crdt_span)
    }

    pub fn try_remote_to_local_version<'a, I: Iterator<Item=&'a RemoteId> + 'a>(&self, ids_iter: I) -> Result<LocalVersion, ConversionError> {
        let mut version: LocalVersion = ids_iter
            .map(|remote_id| self.try_remote_to_local_time(remote_id))
            .collect::<Result<LocalVersion, ConversionError>>()?;

        clean_version(&mut version);
        Ok(version)
    }

    pub fn remote_to_local_version<'a, I: Iterator<Item=&'a RemoteId> + 'a>(&self, ids_iter: I) -> LocalVersion {
        let mut version: LocalVersion = ids_iter
            .map(|remote_id| self.remote_to_local_time(remote_id))
            .collect();

        clean_version(&mut version);
        version
    }

    pub fn local_to_remote_version(&self, local_version: &[Time]) -> SmallVec<[RemoteId; 4]> {
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
    use crate::{CausalGraph, ROOT_TIME};

    #[test]
    fn id_smoke_test() {
        let mut cg = CausalGraph::new();
        cg.get_or_create_agent_id("seph");
        cg.get_or_create_agent_id("mike");
        cg.assign_local_op(&[], 0, 2);
        cg.assign_local_op(&[], 1, 4);

        assert_eq!(ROOT_TIME, cg.remote_to_local_time(&RemoteId {
            agent: "ROOT".into(),
            seq: 0
        }));

        assert_eq!(cg.local_to_remote_time(ROOT_TIME), RemoteId {
            agent: "ROOT".into(),
            seq: 0
        });

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
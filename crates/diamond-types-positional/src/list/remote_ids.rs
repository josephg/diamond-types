use crate::list::{Frontier, OpLog, Time};
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::localtime::TimeSpan;
use crate::{ROOT_AGENT, ROOT_TIME};
use crate::list::frontier::{check_frontier, frontier_is_sorted};
use crate::remotespan::CRDTId;

/// This file contains utilities to convert remote IDs to local time and back.
///
/// Remote IDs are IDs you can pass to a remote peer.

/// External equivalent of CRDTId
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: usize,
}


/// External equivalent of CRDTSpan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct RemoteIdSpan {
    pub agent: SmartString,
    pub seq_range: TimeSpan,
}

impl OpLog {
    /// This panics if the ID isn't known to the document. TODO: Make a try- variant of this.
    pub fn remote_id_to_time(&self, id: &RemoteId) -> Time {
        let agent = self.get_agent_id(id.agent.as_str()).unwrap();

        if agent == ROOT_AGENT { ROOT_TIME }
        else {
            self.client_data[agent as usize].seq_to_time(id.seq)
        }
    }

    pub fn remote_ids_to_frontier<I: Iterator<Item=RemoteId>>(&self, ids_iter: I) -> Frontier {
        let mut frontier: Frontier = ids_iter
            .map(|remote_id| self.remote_id_to_time(&remote_id))
            .collect();
        if !frontier_is_sorted(frontier.as_slice()) {
            // TODO: Check how this effects wasm bundle size.
            frontier.sort_unstable();
        }
        frontier
    }

    fn crdt_id_to_remote(&self, loc: CRDTId) -> RemoteId {
        RemoteId {
            agent: self.get_agent_name(loc.agent).into(),
            seq: loc.seq
        }
    }

    pub fn time_to_remote_id(&self, time: Time) -> RemoteId {
        let crdt_id = self.get_crdt_location(time);
        self.crdt_id_to_remote(crdt_id)
    }

    pub fn frontier_to_remote_ids(&self, frontier: &[Time]) -> SmallVec<[RemoteId; 4]> {
        // Could return an impl Iterator here instead.
        frontier
            .iter()
            .map(|time| self.time_to_remote_id(*time))
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::list::OpLog;
    use crate::list::remote_ids::RemoteId;
    use crate::{ROOT_AGENT, ROOT_TIME};

    #[test]
    fn id_smoke_test() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        oplog.get_or_create_agent_id("mike");
        oplog.push_insert(0, &[ROOT_TIME], 0, "hi".into());
        oplog.push_insert(1, &[ROOT_TIME], 0, "yooo".into());

        assert_eq!(ROOT_TIME, oplog.remote_id_to_time(&RemoteId {
            agent: "ROOT".into(),
            seq: 0
        }));

        assert_eq!(oplog.time_to_remote_id(ROOT_TIME), RemoteId {
            agent: "ROOT".into(),
            seq: 0
        });

        assert_eq!(0, oplog.remote_id_to_time(&RemoteId {
            agent: "seph".into(),
            seq: 0
        }));
        assert_eq!(1, oplog.remote_id_to_time(&RemoteId {
            agent: "seph".into(),
            seq: 1
        }));

        assert_eq!(2, oplog.remote_id_to_time(&RemoteId {
            agent: "mike".into(),
            seq: 0
        }));

        for time in 0..5 {
            let id = oplog.time_to_remote_id(time);
            let expect_time = oplog.remote_id_to_time(&id);
            assert_eq!(time, expect_time);
        }
    }
}
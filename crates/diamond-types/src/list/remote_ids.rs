use crate::list::{Frontier, OpLog, Time};
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::localtime::TimeSpan;
use crate::{ROOT_AGENT, ROOT_TIME};
use crate::list::frontier::frontier_is_sorted;
use crate::list::remote_ids::ConversionError::SeqInFuture;
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
#[cfg_attr(feature = "serde", derive(Serialize), serde(crate="serde_crate"))]
pub enum ConversionError {
    UnknownAgent,
    SeqInFuture,
}

impl OpLog {
    pub fn try_remote_id_to_time(&self, id: &RemoteId) -> Result<Time, ConversionError> {
        let agent = self.get_agent_id(id.agent.as_str())
            .ok_or(ConversionError::UnknownAgent)?;

        if agent == ROOT_AGENT { Ok(ROOT_TIME) }
        else {
            self.client_data[agent as usize]
                .try_seq_to_time(id.seq)
                .ok_or(SeqInFuture)
        }
    }

    /// This panics if the ID isn't known to the document.
    pub fn remote_id_to_time(&self, id: &RemoteId) -> Time {
        let agent = self.get_agent_id(id.agent.as_str()).unwrap();

        if agent == ROOT_AGENT { ROOT_TIME }
        else {
            self.client_data[agent as usize].seq_to_time(id.seq)
        }
    }

    fn crdt_id_to_remote(&self, loc: CRDTId) -> RemoteId {
        RemoteId {
            agent: self.get_agent_name(loc.agent).into(),
            seq: loc.seq
        }
    }

    pub fn time_to_remote_id(&self, time: Time) -> RemoteId {
        let crdt_id = self.time_to_crdt_id(time);
        self.crdt_id_to_remote(crdt_id)
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

    pub fn frontier_to_remote_ids(&self, frontier: &[Time]) -> SmallVec<[RemoteId; 4]> {
        // Could return an impl Iterator here instead.
        frontier
            .iter()
            .map(|time| self.time_to_remote_id(*time))
            .collect()
    }

    /// Sooo, when 2 peers love each other very much...
    ///
    /// They connect together. And they need to find the shared point in time from which they should
    /// send changes.
    ///
    /// Over the network this problem fundamentally pits round-trip time against bandwidth overhead.
    /// The algorithmic approach which would result in the fewest round-trips would just be for both
    /// peers to send their entire histories immediately. But this would waste bandwidth. And the
    /// approach using the least bandwidth would have peers essentially do a distributed binary
    /// search to find a common point in time. But this would take log(n) round-trips, and over long
    /// network distances this is really slow.
    ///
    /// In practice this is usually mostly unnecessary - usually one peer's version is a direct
    /// ancestor of the other peer's version. (Eg, I'm modifying a document and you're just
    /// observing it.)
    ///
    /// Ny design here is a hybrid approach. I'm going to construct a fixed-sized chunk of known
    /// versions we can send to our remote peer. (And the remote peer can do the same with us). The
    /// chunk will contain exponentially less information the further back in time we scan; so the
    /// more time which has passed since we have a common ancestor, the more wasted bytes of changes
    /// we'll send to the remote peer. But this approach will always only need 1RTT to sync.
    ///
    /// Its not perfect, but it'll do donkey. It'll do.
    #[allow(unused)]
    fn get_stochastic_version(&self, target_count: usize) -> Vec<CRDTId> {
        let target_count = target_count.max(self.frontier.len());
        let mut result = Vec::with_capacity(target_count + 10);

        let time_len = self.len();

        // If we have no changes, just return the empty set. Descending from ROOT is implied anyway.
        if time_len == 0 { return result; }

        let mut push_time = |t: Time| {
            result.push(self.time_to_crdt_id(t));
        };

        // No matter what, we'll send the current frontier:
        for t in &self.frontier {
            push_time(*t);
        }

        // So we want about target_count items. I'm assuming there's an exponentially decaying
        // probability of syncing as we go further back in time. This is a big assumption - and
        // probably not true in practice. But it'll do. (TODO: Quadratic might be better?)
        //
        // Given factor, the approx number of operations we'll return is log_f(|ops|).
        // Solving for f gives f = |ops|^(1/target).
        if target_count > self.frontier.len() {
            // Note I'm using n_ops here rather than time, since this easily scales time by the
            // approximate size of the transmitted operations. TODO: This might be a faulty
            // assumption given we're probably sending inserted content? Hm!
            let remaining_count = target_count - self.frontier.len();
            let n_ops = self.operations.0.len();
            let factor = f32::powf(n_ops as f32, 1f32 / (remaining_count) as f32);

            let mut t_inv = 1f32;
            while t_inv < time_len as f32 {
                dbg!(t_inv);
                push_time(time_len - (t_inv as usize));
                t_inv *= factor;
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use crate::list::OpLog;
    use crate::list::remote_ids::RemoteId;
    use crate::ROOT_TIME;

    #[test]
    fn id_smoke_test() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        oplog.get_or_create_agent_id("mike");
        oplog.push_insert_at(0, &[ROOT_TIME], 0, "hi".into());
        oplog.push_insert_at(1, &[ROOT_TIME], 0, "yooo".into());

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

        for time in 0..oplog.len() {
            let id = oplog.time_to_remote_id(time);
            let expect_time = oplog.remote_id_to_time(&id);
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
    fn test_versions_since() {
        let mut oplog = OpLog::new();
        // Should be an empty set
        assert_eq!(oplog.get_stochastic_version(10), &[]);

        oplog.get_or_create_agent_id("seph");
        oplog.push_insert(0, 0, "a");
        oplog.push_insert(0, 0, "a");
        oplog.push_insert(0, 0, "a");
        oplog.push_insert(0, 0, "a");
        dbg!(oplog.get_stochastic_version(10));
    }
}
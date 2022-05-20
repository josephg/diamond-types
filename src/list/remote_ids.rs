use crate::list::OpLog;
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use super::serde::RemoteIdTuple;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use smallvec::SmallVec;
use crate::dtrange::DTRange;
use crate::{LocalVersion, ROOT_AGENT, ROOT_TIME, Time};
use crate::frontier::clean_version;
use crate::remotespan::CRDTGuid;

/// This file contains utilities to convert remote IDs to local time and back.
///
/// Remote IDs are IDs you can pass to a remote peer.

/// External equivalent of CRDTId
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate", from = "RemoteIdTuple", into = "RemoteIdTuple"))]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: usize,
}

/// External equivalent of CRDTSpan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
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
#[cfg_attr(feature = "serde", derive(Serialize), serde(crate="serde_crate"))]
pub enum ConversionError {
    UnknownAgent,
    SeqInFuture,
}

impl OpLog {
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

    pub fn local_to_remote_time(&self, time: Time) -> RemoteId {
        let crdt_id = self.time_to_crdt_id(time);
        self.crdt_id_to_remote(crdt_id)
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
    fn get_stochastic_version(&self, target_count: usize) -> Vec<CRDTGuid> {
        // TODO: WIP.
        let target_count = target_count.max(self.version.len());
        let mut result = Vec::with_capacity(target_count + 10);

        let time_len = self.len();

        // If we have no changes, just return the empty set. Descending from ROOT is implied anyway.
        if time_len == 0 { return result; }

        let mut push_time = |t: Time| {
            result.push(self.time_to_crdt_id(t));
        };

        // No matter what, we'll send the current frontier:
        for t in &self.version {
            push_time(*t);
        }

        // So we want about target_count items. I'm assuming there's an exponentially decaying
        // probability of syncing as we go further back in time. This is a big assumption - and
        // probably not true in practice. But it'll do. (TODO: Quadratic might be better?)
        //
        // Given factor, the approx number of operations we'll return is log_f(|ops|).
        // Solving for f gives f = |ops|^(1/target).
        if target_count > self.version.len() {
            // Note I'm using n_ops here rather than time, since this easily scales time by the
            // approximate size of the transmitted operations. TODO: This might be a faulty
            // assumption given we're probably sending inserted content? Hm!
            let remaining_count = target_count - self.version.len();
            let n_ops = self.operations.0.len();
            let mut factor = f32::powf(n_ops as f32, 1f32 / (remaining_count) as f32);
            factor = factor.max(1.1);

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
        oplog.add_insert_at(0, &[], 0, "hi".into());
        oplog.add_insert_at(1, &[], 0, "yooo".into());

        assert_eq!(ROOT_TIME, oplog.remote_to_local_time(&RemoteId {
            agent: "ROOT".into(),
            seq: 0
        }));

        assert_eq!(oplog.local_to_remote_time(ROOT_TIME), RemoteId {
            agent: "ROOT".into(),
            seq: 0
        });

        assert_eq!(0, oplog.remote_to_local_time(&RemoteId {
            agent: "seph".into(),
            seq: 0
        }));
        assert_eq!(1, oplog.remote_to_local_time(&RemoteId {
            agent: "seph".into(),
            seq: 1
        }));

        assert_eq!(2, oplog.remote_to_local_time(&RemoteId {
            agent: "mike".into(),
            seq: 0
        }));

        for time in 0..oplog.len() {
            let id = oplog.local_to_remote_time(time);
            let expect_time = oplog.remote_to_local_time(&id);
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
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        dbg!(oplog.get_stochastic_version(10));
    }

    #[test]
    fn remote_versions_can_be_empty() {
        let oplog = OpLog::new();
        assert!(oplog.remote_to_local_version(std::iter::empty()).is_empty());
    }
}
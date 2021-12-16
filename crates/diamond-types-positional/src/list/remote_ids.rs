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

#[derive(Debug, Eq, PartialEq, Clone)]
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

    // /// Get the vector clock for this oplog.
    // ///
    // /// NOTE: This is different from the frontier:
    // /// - The vector clock contains an entry for every agent which has *ever* edited this document.
    // /// - The vector clock specifies the *next* version for each useragent, not the last
    // ///
    // /// In general the vector clock is much bigger than the frontier set. It will grow unbounded
    // /// in large documents.
    // ///
    // /// This is currently used for replication because frontiers are not always comparable. But
    // /// ideally I'd like to retire this and use something closer to Automerge's probabilistic
    // /// solution for replication instead.
    // pub fn get_vector_clock(&self) -> SmallVec<[RemoteId; 4]> {
    //     self.client_data.iter().map(|c| {
    //         RemoteId {
    //             agent: c.name.clone(),
    //             seq: c.get_next_seq()
    //         }
    //     }).collect()
    // }
    //
    // /// This method returns the list of spans of orders which will bring a client up to date
    // /// from the specified vector clock version.
    // #[allow(unused)]
    // pub(crate) fn time_spans_since_vector_clock<B>(&self, vector_clock: &[RemoteId]) -> B
    //     where B: Default + AppendRle<TimeSpan>
    // {
    //     #[derive(Clone, Copy, Debug, Eq)]
    //     struct OpSpan {
    //         agent_id: usize,
    //         next_time: Time,
    //         idx: usize,
    //     }
    //
    //     impl PartialEq for OpSpan {
    //         fn eq(&self, other: &Self) -> bool {
    //             self.next_time == other.next_time
    //         }
    //     }
    //
    //     impl PartialOrd for OpSpan {
    //         fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    //             self.next_time.partial_cmp(&other.next_time)
    //         }
    //     }
    //
    //     impl Ord for OpSpan {
    //         fn cmp(&self, other: &Self) -> Ordering {
    //             self.next_time.cmp(&other.next_time)
    //         }
    //     }
    //
    //     let mut heap = BinaryHeap::new();
    //     // We need to go through all clients in the local document because we also need to include
    //     // all entries for any client which *isn't* named in the vector clock.
    //     for (agent_id, client) in self.client_data.iter().enumerate() {
    //         let from_seq = vector_clock.iter()
    //             .find(|rid| rid.agent == client.name)
    //             .map_or(0, |rid| rid.seq);
    //
    //         let idx = client.item_orders.find_index(from_seq).unwrap_or_else(|idx| idx);
    //         if idx < client.item_orders.0.len() {
    //             let entry = &client.item_orders.0[idx];
    //
    //             heap.push(Reverse(OpSpan {
    //                 agent_id,
    //                 next_time: entry.1.start + from_seq.saturating_sub(entry.0),
    //                 idx,
    //             }));
    //         }
    //     }
    //
    //     let mut result = B::default();
    //
    //     while let Some(Reverse(e)) = heap.pop() {
    //         let e = e; // Urgh intellij.
    //         // Append a span of times from here and requeue.
    //         let c = &self.client_data[e.agent_id];
    //         let KVPair(_, span) = c.item_orders.0[e.idx];
    //
    //         let start = span.start.max(e.next_time);
    //         result.push_rle(TimeSpan {
    //             // Kinda gross but at least its branchless.
    //             start,
    //             // end: start + (span.end - e.next_time),
    //             end: span.end,
    //         });
    //
    //         // And potentially requeue this agent.
    //         if e.idx + 1 < c.item_orders.0.len() {
    //             heap.push(Reverse(OpSpan {
    //                 agent_id: e.agent_id,
    //                 next_time: c.item_orders.0[e.idx + 1].1.start,
    //                 idx: e.idx + 1,
    //             }));
    //         }
    //     }
    //
    //     result
    // }
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

    // #[test]
    // fn test_versions_since() {
    //     let mut oplog = OpLog::new();
    //     oplog.get_or_create_agent_id("seph"); // 0
    //     oplog.push_insert(0, 0, "hi");
    //     oplog.get_or_create_agent_id("mike"); // 0
    //     oplog.push_insert(1, 2, "yo");
    //     oplog.push_insert(0, 4, "a");
    //
    //     // When passed an empty vector clock, we fetch all versions from the start.
    //     let vs = oplog.time_spans_since_vector_clock::<Vec<_>>(&[]);
    //     assert_eq!(vs, vec![TimeSpan { start: 0, end: 5 }]);
    //
    //     let vs = oplog.time_spans_since_vector_clock::<Vec<_>>(&[RemoteId {
    //         agent: "seph".into(),
    //         seq: 2
    //     }]);
    //     assert_eq!(vs, vec![TimeSpan { start: 2, end: 5 }]);
    //
    //     let vs = oplog.time_spans_since_vector_clock::<Vec<_>>(&[RemoteId {
    //         agent: "seph".into(),
    //         seq: 100
    //     }, RemoteId {
    //         agent: "mike".into(),
    //         seq: 100
    //     }]);
    //     assert_eq!(vs, vec![]);
    // }
}
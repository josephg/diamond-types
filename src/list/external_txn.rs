use smartstring::alias::{String as SmartString};
use smallvec::SmallVec;
use crate::list::{ListCRDT, Order};
use crate::order::OrderSpan;
use std::collections::BinaryHeap;
use std::cmp::{Ordering, Reverse};
use crate::rle::{Rle, KVPair};
// use crate::LocalOp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteOp {
    Ins {
        origin_left: RemoteId,
        origin_right: RemoteId,
        // ins_content: SmartString, // ?? Or just length?
        len: u32,
    },

    Del {
        id: RemoteId,
        len: u32,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTxn {
    pub id: RemoteId,
    pub parents: SmallVec<[RemoteId; 2]>, // usually 1 entry
    pub ops: SmallVec<[RemoteOp; 2]>, // usually 1-2 entries.

    // pub ins_content: SmartString;
}

// #[derive(Clone, Debug, Eq, PartialEq)]
// pub struct BraidTxn {
//     pub id: RemoteId,
//     pub parents: SmallVec<[RemoteId; 2]>, // usually 1 entry
//     pub ops: SmallVec<[LocalOp; 2]> // usually 1-2 entries.
// }

// thread_local! {
// const REMOTE_ROOT: RemoteId = RemoteId {
//     agent: "ROOT".into(),
//     seq: u32::MAX
// };
// }

/// A vector clock names the *next* expected sequence number for each client in the document.
/// Any entry missing from a vector clock is implicitly 0 - which is to say, the next expected
/// sequence number is 0.
type VectorClock = Vec<RemoteId>;

impl ListCRDT {
    pub fn get_vector_clock(&self) -> VectorClock {
        self.client_data.iter()
            .filter(|c| !c.item_orders.is_empty())
            .map(|c| {
                RemoteId {
                    agent: c.name.clone(),
                    seq: c.item_orders.last().unwrap().end()
                }
            })
            .collect()
    }

    // -> SmallVec<[OrderSpan; 4]>
    /// This method returns the list of spans of orders which will bring a client up to date
    /// from the specified vector clock version.
    pub fn get_versions_since(&self, vv: &VectorClock) -> Rle<OrderSpan> {
        #[derive(Clone, Copy, Debug, Eq)]
        struct OpSpan {
            agent_id: usize,
            next_order: Order,
            idx: usize,
        }

        impl PartialEq for OpSpan {
            fn eq(&self, other: &Self) -> bool {
                self.next_order == other.next_order
            }
        }

        impl PartialOrd for OpSpan {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                self.next_order.partial_cmp(&other.next_order)
            }
        }

        impl Ord for OpSpan {
            fn cmp(&self, other: &Self) -> Ordering {
                self.next_order.cmp(&other.next_order)
            }
        }

        let mut heap = BinaryHeap::new();
        // We need to go through all clients in the local document because we need to include
        // all entries for any client which *isn't* named in the vector clock.
        for (agent_id, client) in self.client_data.iter().enumerate() {
            let from_seq = vv.iter()
                .find(|rid| rid.agent == client.name)
                .map_or(0, |rid| rid.seq);

            let idx = client.item_orders.search(from_seq).unwrap_or_else(|idx| idx);
            if idx < client.item_orders.0.len() {
                let entry = &client.item_orders.0[idx];

                heap.push(Reverse(OpSpan {
                    agent_id,
                    next_order: entry.1.order + from_seq.saturating_sub(entry.0),
                    idx,
                }));
            }
        }

        let mut result = Rle::new();

        while let Some(Reverse(e)) = heap.pop() {
            // Append a span of orders from here and requeue.
            let c = &self.client_data[e.agent_id];
            let KVPair(_, span) = c.item_orders.0[e.idx];
            result.append(OrderSpan {
                // Kinda gross but at least its branchless.
                order: span.order.max(e.next_order),
                len: span.len - (e.next_order - span.order),
            });

            // And potentially requeue this agent.
            if e.idx + 1 < c.item_orders.0.len() {
                heap.push(Reverse(OpSpan {
                    agent_id: e.agent_id,
                    next_order: c.item_orders.0[e.idx + 1].1.order,
                    idx: e.idx + 1,
                }));
            }
        }

        result
    }
}


#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;
    use crate::list::external_txn::{RemoteId, VectorClock};
    use crate::order::OrderSpan;

    #[test]
    fn version_vector() {
        let mut doc = ListCRDT::new();
        assert_eq!(doc.get_vector_clock(), vec![]);
        doc.get_or_create_agent_id("seph"); // 0
        assert_eq!(doc.get_vector_clock(), vec![]);
        doc.local_insert(0, 0, "hi".into());
        assert_eq!(doc.get_vector_clock(), vec![
            RemoteId {
                agent: "seph".into(),
                seq: 2
            }
        ]);
    }

    #[test]
    fn test_versions_since() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.local_insert(0, 0, "hi".into());
        doc.get_or_create_agent_id("mike"); // 0
        doc.local_insert(1, 2, "yo".into());
        doc.local_insert(0, 4, "a".into());

        // When passed an empty vector clock, we fetch all versions from the start.
        let vs = doc.get_versions_since(&VectorClock::new());
        assert_eq!(vs.0, vec![OrderSpan { order: 0, len: 5 }]);

        let vs = doc.get_versions_since(&vec![RemoteId {
            agent: "seph".into(),
            seq: 2
        }]);
        assert_eq!(vs.0, vec![OrderSpan { order: 2, len: 3 }]);

        let vs = doc.get_versions_since(&vec![RemoteId {
            agent: "seph".into(),
            seq: 100
        }, RemoteId {
            agent: "mike".into(),
            seq: 100
        }]);
        assert_eq!(vs.0, vec![]);
    }
}
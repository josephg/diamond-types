use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::iter::FromIterator;

#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;

use diamond_core::{AgentId, CRDT_DOC_ROOT, CRDTId};
use rle::splitable_span::SplitableSpan;

use crate::crdtspan::CRDTSpan;
use crate::content_tree::Toggleable;
use crate::list::{Branch, ListCRDT, Order, ROOT_ORDER};
use crate::list::external_txn::RemoteCRDTOp::{Del, Ins};
use crate::order::OrderSpan;
use crate::rle::{AppendRLE, KVPair, RleSpanHelpers};

// use crate::LocalOp;

/// External equivalent of CRDTLocation
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: u32,
}

/// External equivalent of CRDTSpan
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct RemoteIdSpan {
    // This flattens the structure, but for some reason then it uses a JS map instead of an object
    // :/
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub id: RemoteId,
    pub len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub enum RemoteCRDTOp {
    Ins {
        origin_left: RemoteId,
        origin_right: RemoteId,
        // ins_content: SmartString, // ?? Or just length?
        len: u32,

        // If the content has been deleted in a subsequent change, we might not know what it says.
        // I'm not too happy with this, but I'm not sure what a better solution would look like.
        //
        // Note: We could bind this into len (and make len +/- based on whether we know the content)
        // but in-memory compaction here isn't that important.
        content_known: bool,
    },

    Del {
        /// The id of the item *being deleted*.
        id: RemoteId,
        len: u32,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct RemoteTxn {
    pub id: RemoteId,
    pub parents: SmallVec<[RemoteId; 2]>, // usually 1 entry
    pub ops: SmallVec<[RemoteCRDTOp; 2]>, // usually 1-2 entries.

    pub ins_content: SmartString,
}

/// An iterator over the RemoteTxn objects. This allows RemoteTxns to be created lazily.
#[derive(Debug)]
struct RemoteTxnsIter<'a> {
    doc: &'a ListCRDT,
    span: OrderSpan,
}

impl RemoteCRDTOp {
    fn len(&self) -> u32 {
        match self {
            Ins { len, .. } => { *len }
            Del { len, .. } => { *len }
        }
    }

    // This *almost* matches the API for SplitableSpan. The problem is RemoteOp objects don't know
    // their own agent, and that's needed to infer internal origin_left values.
    //
    // The agent and base_seq provided are the RemoteID of other.
    fn can_append(&self, other: &Self, agent: &SmartString, other_seq: u32) -> bool {
        match (self, other) {
            (Ins {
                origin_right: or1,
                content_known: ck1,
                ..
            }, Ins {
                origin_left: ol2,
                origin_right: or2,
                content_known: ck2,
                ..
            }) => {
                or1 == or2
                    && ck1 == ck2
                    && ol2.agent == *agent
                    && ol2.seq == other_seq - 1
            }
            (Del {id: id1, len: len1}, Del {id: id2, ..}) => {
                // This is correct according to the fuzzer, but I can't figure out how to
                // artificially hit this case.
                id1.agent == id2.agent
                    && id1.seq + len1 == id2.seq
            }
            (_, _) => { false }
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (Ins { len: len1, .. }, Ins { len: len2, .. }) => { *len1 += len2; }
            (Del { len: len1, .. }, Del { len: len2, .. }) => { *len1 += len2; }
            _ => unreachable!(),
        }
    }
}

impl<'a> Iterator for RemoteTxnsIter<'a> {
    type Item = RemoteTxn;

    fn next(&mut self) -> Option<Self::Item> {
        if self.span.len == 0 { None }
        else {
            let (txn, len) = self.doc.next_remote_txn_from_order(self.span);
            debug_assert!(len > 0);
            debug_assert!(len <= self.span.len);
            self.span.consume_start(len);
            Some(txn)
        }
    }
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
pub type VectorClock = Vec<RemoteId>;

impl ListCRDT {
    pub fn remote_id_to_order(&self, id: &RemoteId) -> Order {
        let agent = self.get_agent_id(id.agent.as_str()).unwrap();
        if agent == AgentId::MAX { ROOT_ORDER }
        else { self.client_data[agent as usize].seq_to_order(id.seq) }
    }

    pub fn remote_ids_to_branch(&self, ids: &[RemoteId]) -> Branch {
        ids.iter().map(|remote_id| {
            self.remote_id_to_order(remote_id)
        }).collect()
    }

    pub(crate) fn crdt_id_to_remote(&self, loc: CRDTId) -> RemoteId {
        RemoteId {
            agent: if loc.agent == CRDT_DOC_ROOT.agent {
                "ROOT".into()
            } else {
                self.client_data[loc.agent as usize].name.clone()
            },
            seq: loc.seq
        }
    }

    pub(crate) fn crdt_span_to_remote(&self, span: CRDTSpan) -> RemoteIdSpan {
        RemoteIdSpan {
            id: self.crdt_id_to_remote(span.loc),
            len: span.len
        }
    }

    pub(crate) fn order_to_remote_id(&self, order: Order) -> RemoteId {
        let crdt_loc = self.get_crdt_location(order);
        self.crdt_id_to_remote(crdt_loc)
    }

    pub(crate) fn order_to_remote_id_span(&self, order: Order, max_size: u32) -> (RemoteId, u32) {
        let crdt_span = self.get_crdt_span(order, max_size);
        (self.crdt_id_to_remote(crdt_span.loc), crdt_span.len)
    }

    /// Get the current vector clock. This includes the version for each agent which has ever
    /// interacted with the document, so it will grow over time as the document grows.
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

    /// The frontier only contains the versions which aren't transitively included. Its usually
    /// much smaller than the vector clock, but harder to use because the frontier for one peer
    /// might be unintelligible by another peer.
    pub fn get_frontier<B: FromIterator<RemoteId>>(&self) -> B {
        self.frontier.iter().map(|order| self.order_to_remote_id(*order)).collect()
    }

    /// This method returns the list of spans of orders which will bring a client up to date
    /// from the specified vector clock version.
    pub(super) fn get_order_spans_since<B>(&self, vv: &[RemoteId]) -> B
    where B: Default + AppendRLE<OrderSpan>
    {
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

        let mut result = B::default();

        while let Some(Reverse(e)) = heap.pop() {
            // Append a span of orders from here and requeue.
            let c = &self.client_data[e.agent_id];
            let KVPair(_, span) = c.item_orders.0[e.idx];
            result.push_rle(OrderSpan {
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

    /// Gets the order spans information for the whole document. Always equivalent to
    /// get_versions_since(vec![]).
    pub(super) fn get_all_order_spans(&self) -> Option<OrderSpan> {
        if self.client_with_order.is_empty() {
            None
        } else {
            Some(OrderSpan {
                order: 0,
                // This is correct but it feels somewhat brittle.
                len: self.get_next_order()
            })
        }
    }

    /// This function is used to build an iterator for converting internal txns to remote
    /// transactions.
    fn next_remote_txn_from_order(&self, span: OrderSpan) -> (RemoteTxn, u32) {
        // Each entry we return has its length limited by 5 different things (!)
        // 1. the requested span length (span.len)
        // 2. The length of this txn entry (the number of items we know about in a run)
        // 3. The number of contiguous items by *this userid*
        // 4. The length of the delete or insert operation
        // 5. (For deletes) the contiguous section of items deleted which have the same agent id

        let (txn, offset) = self.txns.find(span.order).unwrap();

        let parents: SmallVec<[RemoteId; 2]> = if let Some(order) = txn.parent_at_offset(offset as _) {
            smallvec![self.order_to_remote_id(order)]
        } else {
            txn.parents.iter().map(|order| self.order_to_remote_id(*order))
                .collect()
        };

        let mut ins_content = SmartString::new();

        // Limit by #1 and #2
        let txn_len = u32::min(span.len, txn.len - offset);
        assert!(txn_len > 0);

        // Limit by #3
        let (id, txn_len) = self.order_to_remote_id_span(span.order, txn_len);

        let mut ops: SmallVec<[RemoteCRDTOp; 2]> = SmallVec::new();
        let mut txn_offset = 0; // Offset into the txn.

        while txn_offset < txn_len {
            // Look up the change at order and append a span with maximum size len_remaining.
            // dbg!(order, len_remaining);

            let order = span.order + txn_offset;
            let len_remaining = txn_len - txn_offset;
            // TODO: Use a smarter replacement for deletes.find() here, since we're traversing
            // linearly.
            let (next, len) = if let Some((d, offset)) = self.deletes.find(order) {
                // dbg!((d, offset));
                // Its a delete.

                // Limit by #4
                let len_limit_2 = u32::min(d.1.len - offset, len_remaining);
                // Limit by #5
                let (id, len) = self.order_to_remote_id_span(d.1.order + offset, len_limit_2);
                // dbg!((&id, len));
                (RemoteCRDTOp::Del { id, len }, len)
            } else {
                // It must be an insert. Fish information out of the range tree.
                let cursor = self.get_cursor_before(order);
                let entry = cursor.get_raw_entry();
                // Limit by #4
                let len = u32::min((entry.len() - cursor.offset) as u32, len_remaining);

                // I'm not fishing out the deleted content at the moment, for any reason.
                // This might be simpler if I just make up content for deleted items O_o
                let content_known = if entry.is_activated() {
                    if let Some(ref text) = self.text_content {
                        let pos = unsafe { cursor.count_content_pos() };
                        let content = text.chars_at(pos).take(len as usize);
                        ins_content.extend(content);
                        true
                    } else { false }
                } else { false };

                // We don't need to fetch the inserted CRDT span ID and limit the length based on
                // that. I thought we did, but it works without that test.

                (RemoteCRDTOp::Ins {
                    origin_left: self.order_to_remote_id(entry.origin_left_at_offset(cursor.offset as u32)),
                    origin_right: self.order_to_remote_id(entry.origin_right),
                    len,
                    content_known,
                }, len)
            };

            // Unfortunately we can't use append_rle because of the funky can_append signature
            if let Some(op) = ops.last_mut() {
                if op.can_append(&next, &id.agent, id.seq + txn_offset) {
                    op.append(next);
                } else { ops.push(next); }
            } else { ops.push(next); }

            txn_offset += len;
        }

        debug_assert_eq!(txn_offset, txn_len);

        (RemoteTxn {
            id,
            parents,
            ops,
            ins_content,
        }, txn_len)
    }

    fn iter_remote_txns<'a, I>(&'a self, spans: I) -> impl Iterator<Item=RemoteTxn> + 'a
    where I: Iterator<Item=&'a OrderSpan> + 'a
    {
        spans.flat_map(move |s| RemoteTxnsIter { doc: self, span: *s })
    }

    pub fn replicate_into(&self, dest: &mut Self) {
        let clock = dest.get_vector_clock();
        // TODO: Do something other than Vec<_> here.
        let order_ranges = self.get_order_spans_since::<Vec<_>>(&clock);
        for txn in self.iter_remote_txns(order_ranges.iter()) {
            dest.apply_remote_txn(&txn);
        }
    }

    /// This is a simplified API for exporting txns to remote peers.
    pub fn get_all_txns_since<B: FromIterator<RemoteTxn>>(&self, clock: &[RemoteId]) -> B {
        let spans = self.get_order_spans_since::<Vec<_>>(clock);
        self.iter_remote_txns(spans.iter()).collect()
    }

    pub fn get_all_txns<B: FromIterator<RemoteTxn>>(&self) -> B {
        // Using a smallvec instead of a vec here means a couple small methods get monomorphized in
        // the compiler output. One less allocation, but 500 more bytes.
        let spans = self.get_all_order_spans();
        // let spans = self.get_all_order_spans::<Vec<_>>();
        self.iter_remote_txns(spans.iter()).collect()
    }
}


#[cfg(test)]
mod tests {
    use crate::list::external_txn::{RemoteId, VectorClock};
    use crate::list::ListCRDT;
    use crate::order::OrderSpan;

    #[test]
    fn version_vector() {
        let mut doc = ListCRDT::new();
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
        let vs = doc.get_order_spans_since::<Vec<_>>(&VectorClock::new());
        assert_eq!(vs, vec![OrderSpan { order: 0, len: 5 }]);

        let vs = doc.get_order_spans_since::<Vec<_>>(&vec![RemoteId {
            agent: "seph".into(),
            seq: 2
        }]);
        assert_eq!(vs, vec![OrderSpan { order: 2, len: 3 }]);

        let vs = doc.get_order_spans_since::<Vec<_>>(&vec![RemoteId {
            agent: "seph".into(),
            seq: 100
        }, RemoteId {
            agent: "mike".into(),
            seq: 100
        }]);
        assert_eq!(vs, vec![]);
    }

    #[test]
    fn all_spans() {
        let mut doc = ListCRDT::new();

        let check = |doc: &ListCRDT| {
            let a = doc.get_all_order_spans();
            let b = doc.get_order_spans_since::<Vec<_>>(&vec![]);
            // Hilariously awful. Doesn't matter for testing though.
            assert_eq!(a.into_iter().collect::<Vec<_>>(), b);
        };
        check(&doc);

        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 0

        doc.local_insert(0, 0, "hi".into());
        check(&doc);
        doc.local_delete(0, 1, 1);
        check(&doc);

        doc.local_insert(1, 0, "yooo".into());
        check(&doc);
    }

    #[test]
    fn external_txns() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.local_insert(0, 0, "hi".into());
        doc.local_delete(0, 0, 2);

        // dbg!(&doc);
        dbg!(doc.next_remote_txn_from_order(OrderSpan { order: 0, len: 40 }));
        // assert_eq!(doc.next_remote_txn_from_order(OrderSpan { order: 0, len: 40 }), )
    }

    #[test]
    fn external_txn_inserts_merged() {
        // Regression
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0

        // This first insert is spatially split by the second insert, but they should still be
        // merged in the output.
        doc.local_insert(0, 0, "aaaa".into());
        doc.local_insert(0, 1, "b".into());

        // dbg!(doc.get_all_txns_since(None));
        let txns: Vec<_> = doc.get_all_txns();
        dbg!(&txns);
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].ops.len(), 2);
    }
}
use crate::list::ListCRDT;
use crate::list::external_txn::RemoteId;

/// This file creates a view of a document which simply streams a linearized sequence of operations.
///
/// This is useful for simple clients - eg web browsers which don't need to store a full copy of
/// history, and which can be hydrated by a simple document snapshot.
///
/// The advantage of this approach is client simplicity (no need for a CRDT on the remote peer) and
/// lower network overhead (the peer doesn't need all the CRDT chum). The downside is the peer isn't
/// a full node, doesn't have history and can't connect to other peers.


// struct

impl ListCRDT {


    fn spans_since(&self, _id: &RemoteId) {
        // This will almost always be a
    }
}

use smartstring::alias::String as SmartString;
use crate::{CRDTSpan, DTRange, Parents, KVPair, LocalVersion, RleVec};

pub(crate) mod storage;
mod causalgraph;
mod check;
pub mod parents;

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

#[derive(Clone, Debug, Default)]
pub struct CausalGraph {
    /// This is a bunch of ranges of (item order -> CRDT location span).
    /// The entries always have positive len.
    ///
    /// This is used to map Local time -> External CRDT locations.
    ///
    /// List is packed.
    pub(crate) client_with_localtime: RleVec<KVPair<CRDTSpan>>,

    /// For each client, we store some data (above). This is indexed by AgentId.
    ///
    /// This is used to map external CRDT locations -> Order numbers.
    pub(crate) client_data: Vec<ClientData>,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// At its core, this data set compactly stores the list of parents for every operation.
    pub(crate) history: Parents,

    // /// This is the version you get if you load the entire causal graph
    // pub(crate) version: LocalVersion,
}

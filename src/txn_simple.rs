use crate::common::CRDTLocation;
use smallvec::SmallVec;
use inlinable_string::InlinableString;
// use std::num::NonZeroU32;
//
// type Order = u64; // Actually limited to u48.
//
// struct FullInsert {
//     predecessor: Order,
//     content: InlinableString,
// }
//
// enum TxnContent {
//     InsertRun {
//         predecessor: Order,
//
//         // We'll have to calculate the length of this from scanning the content.
//         content: InlinableString,
//     },
//     DeleteRun {
//         start: Order,
//         length: NonZeroU32,
//     },
//     Full {
//         inserts: SmallVec<FullInsert>,
//         deletes: DeleteRun
//     }
// }
//
// #[derive(Clone, Debug)]
// struct TxnInternalRun {
//     id: CRDTLocation, // Base.
//     parents: SmallVec<[usize; 2]>, // Parents of the first item in the run
//
//     // raw: Txn,
//     order: u32, // For now. Probably u48 would be about right - if thats worth doing.
//
//     /// usize::max if this txn dominates everything with a lower order that we know about
//     dominates: usize,
//     /// usize::max if this txn is an ancestor
//     submits: usize,
//
//     // So there's 3 types of transactions:
//     // Single character inserts, single character deletes, and "complex" transactions that do more.
//
//     inserts: InlinableString,
//     // insert_at:
// }

enum Op {
    Insert {
        content: InlinableString,
        at: usize,
    },
    Delete {
        at: usize,
        span: usize,
    }
}

struct Txn {
    id: CRDTLocation,
    parents: SmallVec<[usize; 2]>,
    op_start: usize,

    dominates: usize,
    submits: usize,

    ops: SmallVec<[Op; 1]>,
}

// This supports scanning by txn order, by indexing. Or scanning by insert with a binary search.
type TxnsByOrder = Vec<Txn>;
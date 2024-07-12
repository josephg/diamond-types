/// This is some starter code written initially to demo the difference between CRDT patches and
/// positional patches. Its currently not fully tested, though its probably correct if the code
/// we're leaning on is correct.

use smallvec::{SmallVec, smallvec};
use crate::list::{ListCRDT, LV, PositionalComponent};
use smartstring::alias::{String as SmartString};
use rle::{AppendRle, HasLength, MergableSpan, SplitableSpanHelpers};
use crate::list::external_txn::{RemoteId, RemoteIdSpan};
use crate::list::time::docpatchiter::PositionalOpWalk;
use crate::list::txn::TxnSpan;
use crate::rangeextra::OrderRange;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RemoteParentRun {
    pub id: RemoteIdSpan,
    pub parents: SmallVec<RemoteId, 2>, // usually 1 entry
}

impl RemoteParentRun {
    pub fn from_txn(txn: &TxnSpan, offset: LV, max_len: LV, doc: &ListCRDT) -> Self {
        let max_len = max_len.min(txn.len - offset);

        debug_assert!(offset < txn.len);
        Self {
            id: doc.order_to_remote_id_span(txn.time + offset, max_len),
            // Stolen from external_txn. TODO: Hoist this into a method.
            parents: if let Some(order) = txn.parent_at_offset(offset as _) {
                smallvec![doc.order_to_remote_id(order)]
            } else {
                txn.parents
                    .iter()
                    .map(|order| doc.order_to_remote_id(*order))
                    .collect()
            }
        }
    }
}

impl HasLength for RemoteParentRun {
    fn len(&self) -> usize {
        self.id.len as usize
    }
}
impl SplitableSpanHelpers for RemoteParentRun {
    fn truncate_h(&mut self, _at: usize) -> Self {
        panic!("unused");
    }
}
impl MergableSpan for RemoteParentRun {
    fn can_append(&self, other: &Self) -> bool {
        // Gross.
        other.id.id.agent == self.id.id.agent
            && other.id.id.seq == self.id.id.seq + self.id.len
            && other.parents.len() == 1
            && other.parents[0].agent == self.id.id.agent
            && other.parents[0].seq == self.id.id.seq + self.id.len - 1
    }

    fn append(&mut self, other: Self) {
        self.id.len += other.id.len;
    }

    fn prepend(&mut self, other: Self) {
        self.id.id.seq = other.id.id.seq;
        self.id.len += other.id.len;
        self.parents = other.parents;
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RemotePositionalPatches { // TODO: Rename me.
    pub id_and_parents: SmallVec<RemoteParentRun, 1>,
    pub components: SmallVec<PositionalComponent, 1>,
    pub content: SmartString,
}

impl RemotePositionalPatches {
    pub fn from_internal(int: PositionalOpWalk, doc: &ListCRDT) -> Self {
        let mut result = Self {
            id_and_parents: Default::default(),
            components: int.components,
            content: int.content
        };

        for mut range in int.origin_order {
            while !range.is_empty() {
                let (txn, offset) = doc.txns.find_packed(range.start);
                let run = RemoteParentRun::from_txn(txn, offset, range.order_len(), doc);

                range.start += run.id.len;
                result.id_and_parents.push_rle(run);
            }
        }

        result
    }
}

impl ListCRDT {
    pub fn as_external_patch(&self) -> RemotePositionalPatches {
        let int_patch = self.iter_original_patches().into();
        RemotePositionalPatches::from_internal(int_patch, self)
    }
}
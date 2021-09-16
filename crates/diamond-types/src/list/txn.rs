use smallvec::SmallVec;

use rle::SplitableSpan;

use crate::list::{Order, ROOT_ORDER};
use crate::rle::RleKeyed;
use crate::order::OrderSpan;

/// This type stores metadata for a run of transactions created by the users.
///
/// Both individual inserts and deletes will use up txn numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnSpan {
    pub order: Order,
    pub len: u32, // Length of the span

    /// All txns in this span are direct descendants of all operations from order down to shadow.
    /// This is derived from other fields and used as an optimization for some calculations.
    pub shadow: Order,

    /// The parents vector of the first txn in this span. Must contain at least 1 entry (and will
    /// almost always contain exactly 1 entry - the only exception being in the case of concurrent
    /// changes).
    pub parents: SmallVec<[Order; 2]>,

    /// This is a list of the index of other txns which have a parent within this transaction.
    /// TODO: Consider constraining this to not include the next child. Complexity vs memory.
    pub parent_indexes: SmallVec<[usize; 2]>,
    pub child_indexes: SmallVec<[usize; 2]>,
}

impl TxnSpan {
    pub fn parent_at_offset(&self, at: usize) -> Option<Order> {
        if at > 0 {
            Some(self.order + at as u32 - 1)
        } else { None } // look at .parents field.
    }

    pub fn parent_at_order(&self, order: Order) -> Option<Order> {
        if order > self.order {
            Some(order - 1)
        } else { None } // look at .parents field.
    }

    pub fn contains(&self, order: Order) -> bool {
        order >= self.order && order < self.order + self.len
    }

    pub fn last_order(&self) -> Order {
        self.order + self.len - 1
    }

    pub fn shadow_contains(&self, order: Order) -> bool {
        debug_assert!(order <= self.last_order());
        self.shadow == ROOT_ORDER || order >= self.shadow
    }

    pub fn as_span(&self) -> OrderSpan {
        OrderSpan { order: self.order, len: self.len }
    }

    // pub fn parents_at_offset(&self, at: usize) -> SmallVec<[Order; 2]> {
    //     if at > 0 {
    //         smallvec![self.order + at as u32 - 1]
    //     } else {
    //         // I don't like this clone here, but it'll be pretty rare anyway.
    //         self.parents.clone()
    //     }
    // }
}

impl SplitableSpan for TxnSpan {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, _at: usize) -> Self {
        panic!("TxnSpan cannot be truncated");
        // debug_assert!(at >= 1);
        // let at = at as u32;
        // let other = Self {
        //     order: self.order + at,
        //     len: self.len - at,
        //     shadow: self.shadow,
        //     parents: smallvec![self.order + at - 1],
        // };
        // self.len = at as u32;
        // other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.parents.len() == 1
            && other.parents[0] == self.last_order()
            && other.shadow == self.shadow
    }

    fn append(&mut self, other: Self) {
        debug_assert!(other.parent_indexes.is_empty());
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(self.parent_indexes.is_empty());
        self.order = other.order;
        self.len += other.len;
        self.parents = other.parents;
        debug_assert_eq!(self.shadow, other.shadow);
    }
}

impl RleKeyed for TxnSpan {
    fn get_rle_key(&self) -> u32 {
        self.order
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use rle::SplitableSpan;
    use crate::list::txn::TxnSpan;

    // #[test]
    // fn txn_entry_valid() {
    //     test_splitable_methods_valid(TxnSpan {
    //         order: 1000,
    //         len: 5,
    //         shadow: 999,
    //         parents: smallvec![999]
    //     });
    // }

    #[test]
    fn test_txn_appends() {
        let mut txn_a = TxnSpan {
            order: 1000, len: 10, shadow: 500,
            parents: smallvec![999],
            parent_indexes: smallvec![], child_indexes: smallvec![],
        };
        let txn_b = TxnSpan {
            order: 1010, len: 5, shadow: 500,
            parents: smallvec![1009],
            parent_indexes: smallvec![], child_indexes: smallvec![],
        };

        assert!(txn_a.can_append(&txn_b));

        txn_a.append(txn_b);
        assert_eq!(txn_a, TxnSpan {
            order: 1000,
            len: 15,
            shadow: 500,
            parents: smallvec![999],
            parent_indexes: smallvec![],
            child_indexes: smallvec![],
        })
    }
}
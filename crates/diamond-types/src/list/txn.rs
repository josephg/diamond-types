use crate::list::Order;
use smallvec::{SmallVec, smallvec};
use rle::splitable_span::SplitableSpan;
use crate::rle::RleKeyed;

/// This type stores metadata for a run of transactions created by the users.
///
/// Both individual inserts and deletes will use up txn numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnSpan {
    pub order: Order,
    pub len: u32, // Length of the span

    /// All txns in this span are direct descendants of all operations from order down to shadow.
    pub shadow: Order,

    /// The parents vector of the first txn in this span. Must contain at least 1 entry (and will
    /// almost always contain exactly 1 entry - the only exception being in the case of concurrent
    /// changes).
    pub parents: SmallVec<[Order; 2]>
}

impl TxnSpan {
    pub fn parent_at_offset(&self, at: usize) -> Option<Order> {
        if at > 0 {
            Some(self.order + at as u32 - 1)
        } else { None } // look at .parents field.
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

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at >= 1);
        let at = at as u32;
        let other = Self {
            order: self.order + at,
            len: self.len - at,
            shadow: self.shadow,
            parents: smallvec![self.order + at - 1],
        };
        self.len = at as u32;
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.parents.len() == 1
            && other.parents[0] == self.order + self.len - 1
            && other.shadow == self.shadow
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
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
    use crate::list::txn::TxnSpan;
    use rle::splitable_span::{SplitableSpan, test_splitable_methods_valid};
    use smallvec::smallvec;

    #[test]
    fn txn_entry_valid() {
        test_splitable_methods_valid(TxnSpan {
            order: 1000,
            len: 5,
            shadow: 999,
            parents: smallvec![999]
        });
    }

    #[test]
    fn test_txn_appends() {
        let mut txn_a = TxnSpan {
            order: 1000,
            len: 10,
            shadow: 500,
            parents: smallvec![999]
        };
        let txn_b = TxnSpan {
            order: 1010,
            len: 5,
            shadow: 500,
            parents: smallvec![1009]
        };

        assert!(txn_a.can_append(&txn_b));

        txn_a.append(txn_b);
        assert_eq!(txn_a, TxnSpan {
            order: 1000,
            len: 15,
            shadow: 500,
            parents: smallvec![999]
        })
    }
}
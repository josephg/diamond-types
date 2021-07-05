use crate::splitable_span::SplitableSpan;
use std::ptr::NonNull;
use crate::range_tree::{NodeLeaf, EntryTraits, TreeIndex, AbsolutelyPositioned};
use std::fmt::Debug;
use crate::common::IndexGet;
use crate::universal::{ROOT_ORDER, Order};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry<E: EntryTraits, I: TreeIndex<E>> {
    pub order: u32,
    pub len: u32,
    pub ptr: NonNull<NodeLeaf<E, I>>,
}


impl<E: EntryTraits, I: TreeIndex<E>> SplitableSpan for MarkerEntry<E, I> {
    // type Item = NonNull<NodeLeaf>;

    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at as u32;
        self.len = at as u32;
        MarkerEntry {
            order: self.order + at as Order,
            len: remainder_len,
            ptr: self.ptr
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

// impl<E: EntryTraits, I: TreeIndex<E>> IndexGet<usize> for MarkerEntry<E, I> {
//     type Output = NonNull<NodeLeaf<E, I>>;
//
//     fn index_get(&self, _index: usize) -> Self::Output {
//         self.ptr
//     }
// }



impl<E: EntryTraits, I: TreeIndex<E>> Default for MarkerEntry<E, I> {
    fn default() -> Self {
        MarkerEntry {order: ROOT_ORDER, ptr: NonNull::dangling(), len: 0}
    }
}


impl<E: EntryTraits, I: TreeIndex<E>> MarkerEntry<E, I> {
    pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<E, I>> {
        self.ptr
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> EntryTraits for MarkerEntry<E, I> {
    type Item = NonNull<NodeLeaf<E, I>>;

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = Self {
            order: self.order,
            len: at as _,
            ptr: self.ptr
        };
        self.order += at as u32;
        self.len -= at as u32;
        left
    }

    fn contains(&self, _loc: Self::Item) -> Option<usize> {
        panic!("Should never be used")
        // if self.ptr == loc { Some(0) } else { None }
    }

    fn is_valid(&self) -> bool {
        // TODO: Replace this with a real nullptr.
        self.ptr != NonNull::dangling()
    }

    fn at_offset(&self, _offset: usize) -> Self::Item {
        self.ptr
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> AbsolutelyPositioned for MarkerEntry<E, I> {
    fn pos(&self) -> u32 {
        self.order
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use crate::universal::Order;

    #[test]
    fn test_sizes() {
        #[derive(Copy, Clone, Eq, PartialEq, Debug)]
        pub enum MarkerOp {
            Ins(NonNull<usize>),
            Del(Order),
        }

        #[derive(Copy, Clone, Eq, PartialEq, Debug)]
        pub struct MarkerEntry1 {
            // The order / seq is implicit from the location in the list.
            pub len: u32,
            pub op: MarkerOp
        }

        dbg!(std::mem::size_of::<MarkerEntry1>());


        #[derive(Copy, Clone, Eq, PartialEq, Debug)]
        pub enum MarkerEntry2 {
            Ins(u32, NonNull<usize>),
            Del(u32, Order, bool),
        }
        dbg!(std::mem::size_of::<MarkerEntry2>());
    }
}
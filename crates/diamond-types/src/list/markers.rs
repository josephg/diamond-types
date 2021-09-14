use rle::splitable_span::SplitableSpan;
use std::ptr::NonNull;
use crate::content_tree::*;
use std::fmt::Debug;
use crate::entry::{EntryTraits, Searchable};
// use crate::common::IndexGet;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MarkerEntry<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    // This is cleaner as a separate enum and struct, but doing it that way
    // bumps it from 16 to 24 bytes per entry because of alignment.
    pub len: u32,
    pub ptr: Option<NonNull<NodeLeaf<E, I, IE, LE>>>,
}

impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> SplitableSpan for MarkerEntry<E, I, IE, LE> {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at as u32;
        self.len = at as u32;
        MarkerEntry {
            len: remainder_len,
            ptr: self.ptr
        }
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let left = Self {
            len: at as _,
            ptr: self.ptr
        };
        self.len -= at as u32;
        left
    }

    fn can_append(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) { self.len += other.len; }
}

// impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> IndexGet<usize> for MarkerEntry<E, I, IE, LE> {
//     type Output = NonNull<NodeLeaf<E, I, IE, LE>>;
//
//     fn index_get(&self, _index: usize) -> Self::Output {
//         self.ptr
//     }
// }



impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Default for MarkerEntry<E, I, IE, LE> {
    fn default() -> Self {
        MarkerEntry {ptr: None, len: 0}
    }
}


impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> MarkerEntry<E, I, IE, LE> {
    pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<E, I, IE, LE>> {
        self.ptr.unwrap()
    }
}

impl<E: EntryTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Searchable for MarkerEntry<E, I, IE, LE> {
    type Item = Option<NonNull<NodeLeaf<E, I, IE, LE>>>;

    fn contains(&self, _loc: Self::Item) -> Option<usize> {
        panic!("Should never be used")
        // if self.ptr == loc { Some(0) } else { None }
    }


    fn at_offset(&self, _offset: usize) -> Self::Item {
        self.ptr
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use crate::list::Order;

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

        pub type MarkerEntry3 = (u64, Option<NonNull<usize>>);
        dbg!(std::mem::size_of::<MarkerEntry3>());
    }
}
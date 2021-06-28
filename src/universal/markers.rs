use crate::splitable_span::SplitableSpan;
use std::ptr::NonNull;
use crate::range_tree::{NodeLeaf, EntryTraits, TreeIndex};
use std::fmt::Debug;
use crate::common::IndexGet;
use crate::universal::Order;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum MarkerEntry<E: EntryTraits, I: TreeIndex<E>> {
    // This is cleaner as a separate enum and struct, but doing it that way
    // bumps it from 16 to 24 bytes per entry because of alignment.
    Ins { len: u32, ptr: NonNull<NodeLeaf<E, I>> },
    Del { len: u32, order: Order },
}
use self::MarkerEntry::*;

// impl<E: EntryTraits, I: TreeIndex<E>> IndexGet<usize> for MarkerOp<E, I> {
//     type Output = Self;
//
//     fn index_get(&self, offset: usize) -> Self {
//         match self {
//             Ins { ptr, .. } => Ins { len: 1, ptr: *ptr },
//             Del { order, .. } => Del { len: 1, order: order + offset as u32 }
//         }
//     }
// }

impl<E: EntryTraits, I: TreeIndex<E>> MarkerEntry<E, I> {
    fn len_mut(&mut self) -> &mut u32 {
        match self {
            // Go go gadget optimizer
            Ins { len, .. } => len,
            Del { len, .. } => len,
        }
    }

    pub fn unwrap_ptr(&self) -> NonNull<NodeLeaf<E, I>> {
        match self {
            Ins { ptr, .. } => *ptr,
            _ => panic!("Expected ptr in MarkerEntry")
        }
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> SplitableSpan for MarkerEntry<E, I> {
    fn len(&self) -> usize {
        match self {
            Ins { len, .. } => *len as usize,
            Del { len, .. } => *len as usize,
        }
    }

    fn truncate(&mut self, at: usize) -> Self {
        match self {
            Ins { len, ptr } => {
                let remainder_len = *len - at as u32;
                *len = at as u32;
                Ins {
                    len: remainder_len,
                    ptr: *ptr
                }
            }
            Del { len, order } => {
                let remainder_len = *len - at as u32;
                *len = at as u32;
                Del {
                    len: remainder_len,
                    order: at as u32
                }
            }
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        match (self, other) {
            (Ins { ptr: ptr1, .. }, Ins { ptr: ptr2, .. })
                => ptr1 == ptr2,
            (Del { len, order: o1 }, Del { order: o2, .. }) => { o1 + len == *o2 }
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        *self.len_mut() += other.len() as u32;
    }

    fn prepend(&mut self, other: Self) {
        match (self, other) {
            (Ins { len, .. }, Ins { len: len2, .. }) => { *len += len2; },
            (Del { len, order: o1 }, Del { len: len2, order: o2 }) => {
                *o1 = o2;
                *len += len2;
            }
            _ => panic!("Unexpected prepend")
        }
    }
}

// impl<E: EntryTraits, I: TreeIndex<E>> Index<usize> for MarkerEntry<E, I> {
//     type Output = MarkerOp<E, I>;
//
//     fn index(&self, index: usize) -> &Self::Output {
//         match self.op {
//             Ins(ptr) => Ins(ptr),
//             Del(order) => { Del(order + index as u32) }
//         }
//         // &self.ptr
//     }
// }



impl<E: EntryTraits, I: TreeIndex<E>> Default for MarkerEntry<E, I> {
    fn default() -> Self {
        MarkerEntry::Ins { ptr: NonNull::dangling(), len: 0}
    }
}

// impl<E: EntryTraits, I: TreeIndex<E>> EntryTraits for MarkerEntry<E, I> {
//     type Item = NonNull<NodeLeaf<E, I>>;
//
//     fn truncate_keeping_right(&mut self, at: usize) -> Self {
//         let left = Self {
//             len: at as _,
//             ptr: self.ptr
//         };
//         self.len -= at as u32;
//         left
//     }
//
//     fn contains(&self, _loc: Self::Item) -> Option<usize> {
//         panic!("Should never be used")
//         // if self.ptr == loc { Some(0) } else { None }
//     }
//
//     fn is_valid(&self) -> bool {
//         // TODO: Replace this with a real nullptr.
//         self.ptr != NonNull::dangling()
//     }
//
//     fn at_offset(&self, _offset: usize) -> Self::Item {
//         self.ptr
//     }
// }

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
use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use std::cmp::Ordering::*;
use crate::rle::{RleKey, RleKeyed, AppendRLE};
use humansize::{FileSize, file_size_opts};

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Rle<V: SplitableSpan + Clone + Debug + Sized>(pub(crate) Vec<V>);

impl<V: SplitableSpan + Clone + Debug + Sized> Rle<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    /// Append a new value to the end of the RLE list. This method is fast - O(1) average time.
    /// The new item will extend the last entry in the list if possible.
    pub fn append(&mut self, val: V) {
        self.0.append_rle(val);
    }

    // Forward to vec.
    pub fn last(&self) -> Option<&V> { self.0.last() }
    pub fn num_entries(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn iter(&self) -> std::slice::Iter<V> { self.0.iter() }

    pub fn print_stats(&self, name: &str, _detailed: bool) {
        let size = std::mem::size_of::<V>();
        println!("-------- {} RLE --------", name);
        println!("number of {} byte entries: {}", size, self.0.len());
        println!("size: {}", (self.0.capacity() * size).file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("(efficient size: {})", (self.0.len() * size).file_size(file_size_opts::CONVENTIONAL).unwrap());

        // for item in self.0[..100].iter() {
        //     println!("{:?}", item);
        // }
    }
}

// impl<K: Copy + Eq + Ord + Add<Output = K> + Sub<Output = K> + AddAssign, V: Copy + Eq> RLE<K, V> {
impl<V: SplitableSpan + RleKeyed + Clone + Debug + Sized> Rle<V> {
    pub(crate) fn search(&self, needle: RleKey) -> Result<usize, usize> {
        self.0.binary_search_by(|entry| {
            let key = entry.get_rle_key();
            if needle < key { Greater }
            else if needle >= key + entry.len() as u32 { Less }
            else { Equal }
        })
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some((found value, internal offset))
    pub fn find(&self, needle: RleKey) -> Option<(&V, RleKey)> {
        // TODO: This seems to still work correctly if I change Greater to Less and vice versa.
        // Make sure I'm returning the right values here!!
        self.search(needle).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry, needle - entry.get_rle_key())
        })
    }

    pub fn insert(&mut self, val: V) {
        let idx = self.search(val.get_rle_key()).expect_err("Item already exists");

        // Extend the next / previous item if possible
        if idx >= 1 {
            let prev = &mut self.0[idx - 1];
            if prev.can_append(&val) {
                prev.append(val);
                return;
            }
        }

        if idx < self.0.len() {
            let next = &mut self.0[idx];
            debug_assert!(val.get_rle_key() + val.len() as u32 <= next.get_rle_key(), "Items overlap");

            if val.can_append(next) {
                next.prepend(val);
                return
            }
        }

        self.0.insert(idx, val);
    }
}

impl<V: EntryTraits + RleKeyed> Rle<V> {
    pub fn get(&self, idx: RleKey) -> V::Item {
        let (v, offset) = self.find(idx).unwrap();
        v.at_offset(offset as usize)
    }
}

// Seems kinda redundant but eh.
impl<V: SplitableSpan + Clone + Debug + Sized> AppendRLE<V> for Rle<V> {
    fn append_rle(&mut self, item: V) { self.append(item); }
    fn append_reversed_rle(&mut self, _item: V) { unimplemented!(); }
}

// impl<V: EntryTraits> Index<usize> for RLE<V> {
//     type Output = V::Item;
//
//     fn index(&self, index: usize) -> &Self::Output {
//         &self.get(index as RLEKey)
//     }
// }

#[cfg(test)]
mod tests {
    use crate::order::OrderSpan;
    use crate::rle::simple_rle::Rle;
    use crate::rle::KVPair;

    #[test]
    fn rle_finds_at_offset() {
        let mut rle: Rle<KVPair<OrderSpan>> = Rle::new();

        rle.append(KVPair(1, OrderSpan { order: 1000, len: 2 }));
        assert_eq!(rle.find(1), Some((&KVPair(1, OrderSpan { order: 1000, len: 2 }), 0)));
        assert_eq!(rle.find(2), Some((&KVPair(1, OrderSpan { order: 1000, len: 2 }), 1)));
        assert_eq!(rle.find(3), None);

        // This should get appended.
        rle.append(KVPair(3, OrderSpan { order: 1002, len: 1 }));
        assert_eq!(rle.find(3), Some((&KVPair(1, OrderSpan { order: 1000, len: 3 }), 2)));
        assert_eq!(rle.0.len(), 1);
    }

    #[test]
    fn insert_inside() {
        let mut rle: Rle<KVPair<OrderSpan>> = Rle::new();

        rle.insert(KVPair(5, OrderSpan { order: 1000, len: 2}));
        // Prepend
        rle.insert(KVPair(3, OrderSpan { order: 998, len: 2}));
        assert_eq!(rle.0.len(), 1);

        // Append
        rle.insert(KVPair(7, OrderSpan { order: 1002, len: 5}));
        assert_eq!(rle.0.len(), 1);

        // Items which cannot be merged
        rle.insert(KVPair(1, OrderSpan { order: 1, len: 1}));
        assert_eq!(rle.0.len(), 2);

        rle.insert(KVPair(100, OrderSpan { order: 40, len: 1}));
        assert_eq!(rle.0.len(), 3);

        // dbg!(&rle);
    }

    // #[test]
    // fn align() {
    //     use std::mem::{size_of, align_of};
    //     #[repr(transparent)]
    //     struct A(u32);
    //     // #[repr(packed)]
    //     struct B(u64, u32);
    //     // #[repr(packed)]
    //     struct C(B, u32);
    //
    //     dbg!(size_of::<A>(), align_of::<A>());
    //     dbg!(size_of::<B>(), align_of::<B>());
    //     dbg!(size_of::<C>(), align_of::<C>());
    // }
}
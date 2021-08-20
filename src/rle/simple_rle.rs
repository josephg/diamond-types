use crate::range_tree::{EntryTraits, Searchable};
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use std::cmp::Ordering::*;
use crate::rle::{RleKey, RleKeyed, AppendRLE, RleSpanHelpers};
use humansize::{FileSize, file_size_opts};
use std::iter::FromIterator;

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct Rle<V: SplitableSpan + Clone + Sized>(pub(crate) Vec<V>);

impl<V: SplitableSpan + Clone + Sized> Rle<V> {
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
impl<V: SplitableSpan + RleKeyed + Clone + Sized> Rle<V> {
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
        self.search(needle).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry, needle - entry.get_rle_key())
        })
    }

    /// This method is similar to find, except instead of returning None when the value doesn't
    /// exist in the RLE list, we return the position in the empty span.
    ///
    /// This method assumes the "base" of the RLE is 0.
    pub fn find_sparse(&self, needle: RleKey) -> (Result<&V, RleKey>, RleKey) {
        match self.search(needle) {
            Ok(idx) => {
                let entry = &self.0[idx];
                (Ok(entry), needle - entry.get_rle_key())
            }
            Err(idx) => {
                if idx == 0 {
                    (Err(0), needle)
                } else {
                    let end = self.0[idx - 1].end();
                    (Err(end), needle - end)
                }
            }
        }
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found, item is returned by mutable reference as Some((&mut item, offset)).
    pub fn find_mut(&mut self, needle: RleKey) -> Option<(&mut V, RleKey)> {
        self.search(needle).ok().map(move |idx| {
            let entry = &mut self.0[idx];
            let offset = needle - entry.get_rle_key();
            (entry, offset)
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

impl<V: SplitableSpan + Clone + Sized> FromIterator<V> for Rle<V> {
    fn from_iter<T: IntoIterator<Item=V>>(iter: T) -> Self {
        let mut rle = Self::new();
        for item in iter {
            rle.append(item);
        }
        rle
    }
}

impl<V: SplitableSpan + Clone + Sized> Extend<V> for Rle<V> {
    fn extend<T: IntoIterator<Item=V>>(&mut self, iter: T) {
        for item in iter {
            self.append(item);
        }
    }
}

// impl<'a, V: 'a + SplitableSpan + Clone + Sized> FromIterator<&'a V> for Rle<V> {
//     fn from_iter<T: IntoIterator<Item=&'a V>>(iter: T) -> Self {
//         let mut rle = Self::new();
//         for item in iter {
//             rle.append(item.clone());
//         }
//         rle
//     }
// }

impl<V: EntryTraits + Searchable + RleKeyed> Rle<V> {
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

    #[test]
    fn test_find_sparse() {
        let mut rle: Rle<KVPair<OrderSpan>> = Rle::new();

        assert_eq!(rle.find_sparse(0), (Err(0), 0));
        assert_eq!(rle.find_sparse(10), (Err(0), 10));

        rle.insert(KVPair(15, OrderSpan { order: 40, len: 2}));
        assert_eq!(rle.find_sparse(10), (Err(0), 10));
        assert_eq!(rle.find_sparse(15), (Ok(&rle.0[0]), 0));
        assert_eq!(rle.find_sparse(16), (Ok(&rle.0[0]), 1));
        assert_eq!(rle.find_sparse(17), (Err(17), 0));
        assert_eq!(rle.find_sparse(20), (Err(17), 3));
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
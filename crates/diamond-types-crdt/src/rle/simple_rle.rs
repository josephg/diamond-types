use std::cmp::Ordering::*;
use std::fmt::Debug;
use std::iter::{FromIterator, Cloned};
use std::ops::{Index, Range};
use std::slice::SliceIndex;

use humansize::{file_size_opts, FileSize};

use rle::{AppendRle, MergeableIterator, MergeIter};
use rle::{HasLength, MergableSpan, Searchable};

use crate::rle::{RleKey, RleKeyed, RleSpanHelpers};

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct RleVec<V: HasLength + MergableSpan + Clone + Sized>(pub Vec<V>);

impl<V: HasLength + MergableSpan + Clone + Sized> RleVec<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    /// Append a new value to the end of the RLE list. This method is fast - O(1) average time.
    /// The new item will extend the last entry in the list if possible.
    ///
    /// Returns true if the item was merged into the previous item. False if it was appended new.
    pub fn push(&mut self, val: V) -> bool {
        self.0.push_rle(val)
    }

    pub fn push_will_merge(&self, item: &V) -> bool {
        if let Some(v) = self.last() {
            v.can_append(item)
        } else { false }
    }

    // Forward to vec.
    pub fn last(&self) -> Option<&V> { self.0.last() }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn iter(&self) -> std::slice::Iter<V> { self.0.iter() }
    pub fn iter_merged(&self) -> MergeIter<Cloned<std::slice::Iter<V>>> { self.0.iter().cloned().merge_spans() }

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
impl<V: HasLength + MergableSpan + RleKeyed + Clone + Sized> RleVec<V> {
    pub(crate) fn find_index(&self, needle: RleKey) -> Result<usize, usize> {
        self.0.binary_search_by(|entry| {
            let key = entry.get_rle_key();
            if needle < key { Greater }
            else if needle >= key + entry.len() { Less }
            else { Equal }
        })
    }

    // /// This is a variant of find_index for data sets where we normally know the index (via
    // /// iteration).
    // pub(crate) fn find_hinted(&self, needle: RleKey, hint: &mut usize) -> Result<usize, usize> {
    //     if self.is_empty() { return Err(0); }
    //
    //     if *hint < self.0.len() {
    //         let e = &self.0[*hint];
    //         if needle >= e.get_rle_key() && needle < e.end() {
    //             return Ok(*hint);
    //         } else if needle < e.get_rle_key() {
    //             if hint > 0 {
    //                 todo!()
    //             } else {
    //                 *hint = 0;
    //                 return Err()
    //             }
    //         } else {
    //             debug_assert!(needle >= e.end());
    //         }
    //     }
    //     todo!()
    // }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some(found value).
    pub fn find(&self, needle: RleKey) -> Option<&V> {
        self.find_index(needle).ok().map(|idx| {
            &self.0[idx]
        })
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some((found value, internal offset))
    pub fn find_with_offset(&self, needle: RleKey) -> Option<(&V, RleKey)> {
        self.find_index(needle).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry, needle - entry.get_rle_key())
        })
    }

    /// Same as list.find(needle) except for lists where there are no gaps in the RLE list.
    pub fn find_packed(&self, needle: RleKey) -> (&V, RleKey) {
        self.find_with_offset(needle).unwrap()
    }

    /// This method is similar to find, except instead of returning None when the value doesn't
    /// exist in the RLE list, we return the position in the empty span.
    ///
    /// This method assumes the "base" of the RLE is 0.
    pub fn find_sparse(&self, needle: RleKey) -> (Result<&V, RleKey>, RleKey) {
        match self.find_index(needle) {
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
        self.find_index(needle).ok().map(move |idx| {
            let entry = &mut self.0[idx];
            let offset = needle - entry.get_rle_key();
            (entry, offset)
        })
    }

    pub fn insert(&mut self, val: V) {
        let idx = self.find_index(val.get_rle_key()).expect_err("Item already exists");

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
            debug_assert!(val.get_rle_key() + val.len() <= next.get_rle_key(), "Items overlap");

            if val.can_append(next) {
                next.prepend(val);
                return
            }
        }

        self.0.insert(idx, val);
    }

    /// Search forward from idx until we find needle. idx is modified. Returns either the item if
    /// successful, or the key of the subsequent item.
    pub(crate) fn search_scanning_sparse(&self, needle: RleKey, idx: &mut usize) -> Result<&V, RleKey> {
        while *idx < self.len() {
            // TODO: Is this bounds checking? It shouldn't need to... Fix if it is.
            let e = &self[*idx];
            if needle < e.end() {
                return if needle >= e.get_rle_key() {
                    Ok(e)
                } else {
                    Err(e.get_rle_key())
                };
            }

            *idx += 1;
        }
        Err(RleKey::MAX)
    }

    pub(crate) fn search_scanning_packed(&self, needle: RleKey, idx: &mut usize) -> &V {
        self.search_scanning_sparse(needle, idx).unwrap()
    }

    /// Search backwards from idx until we find needle. idx is modified. Returns either the item or
    /// the end of the preceeding range. Note the end could be == needle. (But cannot be greater
    /// than it).
    pub(crate) fn search_scanning_backwards_sparse(&self, needle: RleKey, idx: &mut usize) -> Result<&V, RleKey> {
        // This conditional looks inverted given we're looping backwards, but I'm using
        // wrapping_sub - so when we reach the end the index wraps around and we'll hit usize::MAX.
        while *idx < self.len() {
            let e = &self[*idx];
            if needle >= e.get_rle_key() {
                return if needle < e.end() {
                    Ok(e)
                } else {
                    Err(e.end())
                };
            }
            *idx = idx.wrapping_sub(1);
        }
        Err(0)
    }

    /// Visit each item or gap in this (sparse) RLE list, ending at end with the passed visitor
    /// method.
    pub fn for_each_sparse<F>(&self, end: RleKey, mut visitor: F)
    where F: FnMut(Result<&V, Range<RleKey>>) {
        let mut key = 0;

        for e in self.iter() {
            let next_key = e.get_rle_key();
            if key < next_key {
                // Visit the empty range
                visitor(Err(key..next_key));
            }

            // Ok now visit the entry we found.
            visitor(Ok(e));
            key = e.end();
            debug_assert!(key <= end);
        }
        // And visit the remainder, if there is any.
        if key < end {
            visitor(Err(key..end));
        }
    }
}

impl<V: HasLength + MergableSpan + Clone + Sized> FromIterator<V> for RleVec<V> {
    fn from_iter<T: IntoIterator<Item=V>>(iter: T) -> Self {
        let mut rle = Self::new();
        for item in iter {
            rle.push(item);
        }
        rle
    }
}

impl<V: HasLength + MergableSpan + Clone + Sized> Extend<V> for RleVec<V> {
    fn extend<T: IntoIterator<Item=V>>(&mut self, iter: T) {
        for item in iter {
            self.push(item);
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

impl<V: HasLength + MergableSpan + Searchable + RleKeyed> RleVec<V> {
    pub fn get(&self, idx: RleKey) -> V::Item {
        let (v, offset) = self.find_with_offset(idx).unwrap();
        v.at_offset(offset as usize)
    }
}

// Seems kinda redundant but eh.
impl<V: HasLength + MergableSpan + Clone + Debug + Sized> AppendRle<V> for RleVec<V> {
    fn push_rle(&mut self, item: V) -> bool { self.push(item) }
    fn push_reversed_rle(&mut self, _item: V) -> bool { unimplemented!(); }
}

impl<T: HasLength + MergableSpan, I: SliceIndex<[T]>> Index<I> for RleVec<T> {
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::order::TimeSpan;
    use crate::rle::KVPair;
    use crate::rle::simple_rle::RleVec;

    #[test]
    fn rle_finds_at_offset() {
        let mut rle: RleVec<KVPair<TimeSpan>> = RleVec::new();

        rle.push(KVPair(1, TimeSpan { start: 1000, len: 2 }));
        assert_eq!(rle.find_with_offset(1), Some((&KVPair(1, TimeSpan { start: 1000, len: 2 }), 0)));
        assert_eq!(rle.find_with_offset(2), Some((&KVPair(1, TimeSpan { start: 1000, len: 2 }), 1)));
        assert_eq!(rle.find_with_offset(3), None);

        // This should get appended.
        rle.push(KVPair(3, TimeSpan { start: 1002, len: 1 }));
        assert_eq!(rle.find_with_offset(3), Some((&KVPair(1, TimeSpan { start: 1000, len: 3 }), 2)));
        assert_eq!(rle.0.len(), 1);
    }

    #[test]
    fn insert_inside() {
        let mut rle: RleVec<KVPair<TimeSpan>> = RleVec::new();

        rle.insert(KVPair(5, TimeSpan { start: 1000, len: 2}));
        // Prepend
        rle.insert(KVPair(3, TimeSpan { start: 998, len: 2}));
        assert_eq!(rle.0.len(), 1);

        // Append
        rle.insert(KVPair(7, TimeSpan { start: 1002, len: 5}));
        assert_eq!(rle.0.len(), 1);

        // Items which cannot be merged
        rle.insert(KVPair(1, TimeSpan { start: 1, len: 1}));
        assert_eq!(rle.0.len(), 2);

        rle.insert(KVPair(100, TimeSpan { start: 40, len: 1}));
        assert_eq!(rle.0.len(), 3);

        // dbg!(&rle);
    }

    #[test]
    fn test_find_sparse() {
        let mut rle: RleVec<KVPair<TimeSpan>> = RleVec::new();

        assert_eq!(rle.find_sparse(0), (Err(0), 0));
        assert_eq!(rle.find_sparse(10), (Err(0), 10));

        rle.insert(KVPair(15, TimeSpan { start: 40, len: 2}));
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

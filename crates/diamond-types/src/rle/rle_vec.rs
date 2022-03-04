use std::cmp::Ordering::*;
use std::fmt::Debug;
use std::iter::{FromIterator, Cloned};
use std::ops::{Index, Range};
use std::slice::SliceIndex;

use humansize::{file_size_opts, FileSize};

use rle::{AppendRle, HasLength, MergableSpan, MergeableIterator, MergeIter, SplitableSpan, SplitableSpanCtx};
use rle::Searchable;
use crate::localtime::TimeSpan;

use crate::rle::{RleKeyed, RleKeyedAndSplitable, RleSpanHelpers};

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RleVec<V: HasLength + MergableSpan + Sized>(pub Vec<V>);

impl<V: HasLength + MergableSpan + Sized> RleVec<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    /// Append a new value to the end of the RLE list. This method is fast - O(1) average time.
    /// The new item will extend the last entry in the list if possible.
    ///
    /// Returns true if the item was merged into the previous item. False if it was appended new.
    pub fn push(&mut self, val: V) -> bool {
        self.0.push_rle(val)
    }

    #[allow(unused)]
    pub fn push_will_merge(&self, item: &V) -> bool {
        if let Some(v) = self.last() {
            v.can_append(item)
        } else { false }
    }

    // Forward to vec.
    pub fn last(&self) -> Option<&V> { self.0.last() }

    #[allow(unused)]
    pub fn num_entries(&self) -> usize { self.0.len() }

    /// Returns past the end of the last key.
    pub fn end(&self) -> usize where V: RleKeyed {
        if let Some(v) = self.last() {
            v.end()
        } else {
            0
        }
    }

    #[allow(unused)]
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
    pub(crate) fn find_index(&self, needle: usize) -> Result<usize, usize> {
        self.0.binary_search_by(|entry| {
            let key = entry.rle_key();
            if needle < key { Greater }
            else if needle >= key + entry.len() { Less }
            else { Equal }
        })
    }

    // /// This is a variant of find_index for data sets where we normally know the index (via
    // /// iteration).
    // pub(crate) fn find_hinted(&self, needle: usize, hint: &mut usize) -> Result<usize, usize> {
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
    pub fn find(&self, needle: usize) -> Option<&V> {
        self.find_index(needle).ok().map(|idx| {
            &self.0[idx]
        })
    }

    /// Same as list.find_with_offset(needle) except for lists where there are no gaps in the RLE list.
    pub fn find_packed(&self, needle: usize) -> &V {
        self.find(needle).unwrap()
    }

    /// Find the item at range, cloning and trimming it down to size. This is generally less
    /// efficient than using find_with_offset and friends, but its much more convenient.
    ///
    /// Note the returned value might be smaller than the passed range.
    #[allow(unused)]
    pub fn find_packed_and_split(&self, range: TimeSpan) -> V where V: SplitableSpan {
        self.find_packed_and_split_ctx(range, &())
    }

    #[allow(unused)]
    pub fn find_packed_and_split_ctx(&self, range: TimeSpan, ctx: &V::Ctx) -> V where V: SplitableSpanCtx {
        let (item, offset) = self.find_packed_with_offset(range.start);
        let mut item = item.clone();
        item.truncate_keeping_right_ctx(offset, ctx);
        if item.len() > range.len() {
            item.truncate_ctx(range.len(), ctx);
        }
        item
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some((found value, internal offset))
    pub fn find_with_offset(&self, needle: usize) -> Option<(&V, usize)> {
        self.find_index(needle).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry, needle - entry.rle_key())
        })
    }

    /// Same as list.find_with_offset(needle) except for lists where there are no gaps in the RLE list.
    pub fn find_packed_with_offset(&self, needle: usize) -> (&V, usize) {
        self.find_with_offset(needle).unwrap()
    }

    // pub fn find_packed_range(&self, needle: TimeSpan) -> (&V, TimeSpan) {
    //     let (v, offset) = self.find_packed(needle.start);
    //
    //     (v,
    // }

    /// This method is similar to find, except instead of returning None when the value doesn't
    /// exist in the RLE list, we return the position in the empty span.
    ///
    /// This method assumes the "base" of the RLE is 0.
    ///
    /// Returns (Ok(elem), offset) if item is found, otherwise (Err(void range), offset into void)
    #[allow(unused)]
    pub fn find_sparse(&self, needle: usize) -> (Result<&V, TimeSpan>, usize) {
        match self.find_index(needle) {
            Ok(idx) => {
                let entry = &self.0[idx];
                (Ok(entry), needle - entry.rle_key())
            }
            Err(idx) => {
                let next_key = if let Some(entry) = self.0.get(idx) {
                    entry.rle_key()
                } else {
                    usize::MAX
                };

                if idx == 0 {
                    (Err((0..next_key).into()), needle)
                } else {
                    let end = self.0[idx - 1].end();
                    (Err((end..next_key).into()), needle - end)
                }
            }
        }
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found, item is returned by mutable reference as Some((&mut item, offset)).
    #[allow(unused)]
    pub fn find_mut(&mut self, needle: usize) -> Option<(&mut V, usize)> {
        self.find_index(needle).ok().map(move |idx| {
            let entry = &mut self.0[idx];
            let offset = needle - entry.rle_key();
            (entry, offset)
        })
    }

    /// Insert an item at this location in the RLE list. This method is O(n) as it needs to shift
    /// subsequent elements forward.
    #[allow(unused)]
    pub fn insert(&mut self, val: V) {
        let idx = self.find_index(val.rle_key()).expect_err("Item already exists");

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
            debug_assert!(val.rle_key() + val.len() <= next.rle_key(), "Items overlap");

            if val.can_append(next) {
                next.prepend(val);
                return
            }
        }

        self.0.insert(idx, val);
    }

    /// Remove an item. This may need to shuffle indexes around. This method is O(n) with the number
    /// of items between this entry and the end of the list.
    ///
    /// This method silently ignores requests to delete ranges we don't have.
    pub fn remove_ctx(&mut self, mut deleted_range: TimeSpan, ctx: &V::Ctx) where V: SplitableSpanCtx {
        // Fast case - the requested entry is at the end.
        loop {
            if let Some(last) = self.0.last_mut() {
                let last_span = last.span();

                // Range is past the end of the list. Nothing to do here!
                if deleted_range.start >= last_span.end { return; }

                // Need slow approach.
                if deleted_range.end < last_span.end { break; }

                if deleted_range.start <= last_span.start {
                    // Remove entire last entry.
                    self.0.pop();
                    if deleted_range.start == last_span.start {
                        // Easiest case. We're done.
                        return;
                    }
                } else {
                    // Truncate last entry and return.
                    last.truncate_from_ctx(deleted_range.start, ctx);
                    return;
                }
            } else {
                // The list is empty. Nothing more to do.
                return;
            }
        }

        // Slow case - the requested range is in the middle of the list somewhere. We need to carve
        // it out.
        let mut idx = match self.find_index(deleted_range.start) {
            Ok(idx) => idx,
            Err(idx) => {
                if let Some(entry) = self.0.get(idx) {
                    deleted_range.truncate_keeping_right_from(entry.rle_key());
                } else { return; }
                idx
            }
        };

        loop {
            if idx >= self.0.len() { break; }
            let e = &mut self.0[idx];

            debug_assert!(e.rle_key() <= deleted_range.start);

            // There's 4 cases here.

            let e_end = e.end();

            let keep_start = e.rle_key() < deleted_range.start;
            let keep_end = e_end > deleted_range.end();
            match (keep_start, keep_end) {
                (false, false) => {
                    // Remove the entry and iterate.
                    self.0.remove(idx);
                },

                (false, true) => {
                    // Trim the start, trim the end.
                    e.truncate_keeping_right_from_ctx(deleted_range.start, ctx);
                    break;
                },

                (true, false) => {
                    // Trim the end
                    e.truncate_from_ctx(deleted_range.start, ctx);
                    idx += 1;
                }

                (true, true) => {
                    // Trim in the middle.
                    let mut remainder = e.truncate_from_ctx(deleted_range.start, ctx);
                    remainder.truncate_keeping_right_from_ctx(deleted_range.end, ctx);
                    self.insert(remainder);
                    break;
                }
            }

            if e_end == deleted_range.end() { break; }
        }
    }

    /// Search forward from idx until we find needle. idx is modified. Returns either the item if
    /// successful, or the key of the subsequent item.
    #[allow(unused)]
    pub(crate) fn search_scanning_sparse(&self, needle: usize, idx: &mut usize) -> Result<&V, usize> {
        while *idx < self.0.len() {
            // TODO: Is this bounds checking? It shouldn't need to... Fix if it is.
            let e = &self[*idx];
            if needle < e.end() {
                return if needle >= e.rle_key() {
                    Ok(e)
                } else {
                    Err(e.rle_key())
                };
            }

            *idx += 1;
        }
        Err(usize::MAX)
    }

    #[allow(unused)]
    pub(crate) fn search_scanning_packed(&self, needle: usize, idx: &mut usize) -> &V {
        self.search_scanning_sparse(needle, idx).unwrap()
    }

    /// Search backwards from idx until we find needle. idx is modified. Returns either the item or
    /// the end of the preceeding range. Note the end could be == needle. (But cannot be greater
    /// than it).
    #[allow(unused)]
    pub(crate) fn search_scanning_backwards_sparse(&self, needle: usize, idx: &mut usize) -> Result<&V, usize> {
        // This conditional looks inverted given we're looping backwards, but I'm using
        // wrapping_sub - so when we reach the end the index wraps around and we'll hit usize::MAX.
        while *idx < self.0.len() {
            let e = &self[*idx];
            if needle >= e.rle_key() {
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
    #[allow(unused)]
    pub fn for_each_sparse<F>(&self, end: usize, mut visitor: F)
    where F: FnMut(Result<&V, Range<usize>>) {
        let mut key = 0;

        for e in self.iter() {
            let next_key = e.rle_key();
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

    /// Check that the RLE is contiguous and packed. Panic if not.
    #[allow(unused)]
    pub(crate) fn check_packed(&self) {
        let mut expect_next = 0;
        for (i, entry) in self.0.iter().enumerate() {
            if i != 0 {
                assert_eq!(entry.rle_key(), expect_next);
            }
            expect_next = entry.end();
        }
    }
}

impl<V: HasLength + MergableSpan + Sized> FromIterator<V> for RleVec<V> {
    fn from_iter<T: IntoIterator<Item=V>>(iter: T) -> Self {
        let mut rle = Self::new();
        for item in iter {
            rle.push(item);
        }
        rle
    }
}

impl<V: HasLength + MergableSpan + Sized> Extend<V> for RleVec<V> {
    fn extend<T: IntoIterator<Item=V>>(&mut self, iter: T) {
        for item in iter {
            self.push(item);
        }
    }
}

impl<V: HasLength + MergableSpan + Sized> Default for RleVec<V> {
    fn default() -> Self {
        Self(Vec::default())
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
    pub fn get(&self, idx: usize) -> V::Item {
        let (v, offset) = self.find_with_offset(idx).unwrap();
        v.at_offset(offset)
    }
}

// Seems kinda redundant but eh.
impl<V: HasLength + MergableSpan + Debug + Sized> AppendRle<V> for RleVec<V> {
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

fn id_clone<V: Clone>(v: &V) -> V {
    v.clone()
}

impl<V: HasLength + SplitableSpan + RleKeyed + MergableSpan> RleVec<V> {
    #[allow(unused)]
    pub fn iter_range_packed(&self, range: TimeSpan) -> RleVecRangeIter<V, V, impl Fn(&V) -> V> {
        self.iter_range_map_packed_ctx(range, &(), id_clone)
    }
}

impl<V: HasLength + SplitableSpanCtx + RleKeyed + MergableSpan> RleVec<V> {
    pub fn iter_range_packed_ctx<'a>(&'a self, range: TimeSpan, ctx: &'a V::Ctx) -> RleVecRangeIter<'a, V, V, impl Fn(&V) -> V> {
        self.iter_range_map_packed_ctx(range, ctx, id_clone)
    }
}

impl<V: HasLength + RleKeyed + MergableSpan> RleVec<V> {
    pub fn iter_range_map_packed<I: SplitableSpan + HasLength, F: Fn(&V) -> I>(&self, range: TimeSpan, map_fn: F) -> RleVecRangeIter<V, I, F> {
        self.iter_range_map_packed_ctx(range, &(), map_fn)
    }

    pub fn iter_range_map_packed_ctx<'a, I: SplitableSpanCtx + HasLength, F: Fn(&V) -> I>(&'a self, range: TimeSpan, ctx: &'a I::Ctx, map_fn: F) -> RleVecRangeIter<'a, V, I, F> {
        let idx = self.find_index(range.start).unwrap();

        let entry = &self.0[idx];
        let offset = range.start - entry.rle_key();

        RleVecRangeIter {
            offset,
            idx,
            len_remaining: range.len(),
            map_fn,
            data: &self.0,
            ctx,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RleVecRangeIter<'a, V: HasLength + MergableSpan, I: HasLength + SplitableSpanCtx, F: Fn(&V) -> I> {
    offset: usize,
    idx: usize,
    len_remaining: usize,
    map_fn: F,
    data: &'a [V],

    ctx: &'a I::Ctx, // This could have a different lifetime specifier.
}

impl<'a, V: HasLength + MergableSpan, I: HasLength + SplitableSpanCtx, F: Fn(&V) -> I> Iterator for RleVecRangeIter<'a, V, I, F> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len_remaining == 0 || self.idx >= self.data.len() { return None; }

        let mut item = (self.map_fn)(&self.data[self.idx]);
        if self.offset > 0 {
            assert!(self.offset < item.len());
            item.truncate_keeping_right_ctx(self.offset, self.ctx);
            self.offset = 0;
        }

        if item.len() > self.len_remaining {
            item.truncate_ctx(self.len_remaining, self.ctx);
            self.len_remaining = 0;
        } else {
            self.idx += 1;
            self.len_remaining -= item.len();
        }

        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rle_iter_range() {
        let mut rle: RleVec<TimeSpan> = RleVec::new();
        rle.push((0..10).into());

        // This is a sad example.
        let items = rle.iter_range_packed((5..8).into()).collect::<Vec<_>>();
        assert_eq!(&items, &[(5..8).into()]);
    }

    // use crate::order::OrderSpan;
    // use crate::rle::KVPair;
    // use crate::rle::simple_rle::RleVec;
    //
    // #[test]
    // fn rle_finds_at_offset() {
    //     let mut rle: RleVec<KVPair<OrderSpan>> = RleVec::new();
    //
    //     rle.push(KVPair(1, OrderSpan { order: 1000, len: 2 }));
    //     assert_eq!(rle.find_with_offset(1), Some((&KVPair(1, OrderSpan { order: 1000, len: 2 }), 0)));
    //     assert_eq!(rle.find_with_offset(2), Some((&KVPair(1, OrderSpan { order: 1000, len: 2 }), 1)));
    //     assert_eq!(rle.find_with_offset(3), None);
    //
    //     // This should get appended.
    //     rle.push(KVPair(3, OrderSpan { order: 1002, len: 1 }));
    //     assert_eq!(rle.find_with_offset(3), Some((&KVPair(1, OrderSpan { order: 1000, len: 3 }), 2)));
    //     assert_eq!(rle.0.len(), 1);
    // }
    //
    // #[test]
    // fn insert_inside() {
    //     let mut rle: RleVec<KVPair<OrderSpan>> = RleVec::new();
    //
    //     rle.insert(KVPair(5, OrderSpan { order: 1000, len: 2}));
    //     // Prepend
    //     rle.insert(KVPair(3, OrderSpan { order: 998, len: 2}));
    //     assert_eq!(rle.0.len(), 1);
    //
    //     // Append
    //     rle.insert(KVPair(7, OrderSpan { order: 1002, len: 5}));
    //     assert_eq!(rle.0.len(), 1);
    //
    //     // Items which cannot be merged
    //     rle.insert(KVPair(1, OrderSpan { order: 1, len: 1}));
    //     assert_eq!(rle.0.len(), 2);
    //
    //     rle.insert(KVPair(100, OrderSpan { order: 40, len: 1}));
    //     assert_eq!(rle.0.len(), 3);
    //
    //     // dbg!(&rle);
    // }
    //
    // #[test]
    // fn test_find_sparse() {
    //     let mut rle: RleVec<KVPair<OrderSpan>> = RleVec::new();
    //
    //     assert_eq!(rle.find_sparse(0), (Err(0), 0));
    //     assert_eq!(rle.find_sparse(10), (Err(0), 10));
    //
    //     rle.insert(KVPair(15, OrderSpan { order: 40, len: 2}));
    //     assert_eq!(rle.find_sparse(10), (Err(0), 10));
    //     assert_eq!(rle.find_sparse(15), (Ok(&rle.0[0]), 0));
    //     assert_eq!(rle.find_sparse(16), (Ok(&rle.0[0]), 1));
    //     assert_eq!(rle.find_sparse(17), (Err(17), 0));
    //     assert_eq!(rle.find_sparse(20), (Err(17), 3));
    // }

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

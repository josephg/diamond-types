use std::cmp::Ordering::{Equal, Greater, Less};
use std::fmt::Debug;
use std::iter::Cloned;
use std::ops::{Index, Range};
use std::slice::SliceIndex;
use humansize::{DECIMAL, format_size};
use rle::{AppendRle, HasLength, HasRleKey, MergableSpan, MergeIter, RleDRun, Searchable, SplitableSpan, SplitableSpanCtx};
use crate::DTRange;
use crate::rle::rle_vec::{RleStats, RleVecRangeIter};
use crate::rle::{RleKeyedAndSplitable, RleSpanHelpers};

/// This is a variant of RleVec which guarantees that all items are "packed". That is, there are
/// no gaps between items.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RlePackedVec3<V: PackedRleItem>(pub Vec<V::Packed>, usize);


pub trait PackedRleItem: HasRleKey + MergableSpan {
    type Packed;

    // Pack item, returning the packed form and the end position.
    //
    // These functions take a reference for efficiency. The type must support .clone anyway.
    fn pack(&self) -> (Self::Packed, usize);
    fn unpack(packed: &Self::Packed, end: usize) -> Self;

    fn packed_key(packed: &Self::Packed) -> usize;

    // This is a helper function since it comes up a bunch.
    fn unpack_tuple((p, e): (&Self::Packed, usize)) -> Self {
        Self::unpack(p, e)
    }

    // I could instead implement this by requiring that Self::Packed impl MergableSpan somehow
    // - but that's more annoying to implement, and I need to pass a_end in somehow. Eh.
    fn can_append_packed(a: &Self::Packed, a_end: usize, b: &Self::Packed) -> bool;
    fn append_packed(item: &mut Self::Packed, item_end: usize, other: Self::Packed);
}



// /// This trait shows up in a few places where we need a way to try and merge items which do not
// /// store their own size.
// ///
// /// I'd love a way to unify this with Mergeable but ???.
// pub trait MergableAtAbsolute {
//     // /// Try to append other to self. If possible, self is modified (if necessary) and true is
//     // /// returned.
//     // fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool;
//     fn can_append(&mut self, other: &Self, at_key: usize) -> bool;
//     fn append(&mut self, other: Self, at_key: usize);
// }

impl<V: PackedRleItem> RlePackedVec3<V> {
    pub fn new() -> Self { Self(Vec::new(), 0) }

    /// Append a new value to the end of the RLE list. This method is fast - O(1) average time.
    /// The new item will extend the last entry in the list if possible.
    ///
    /// Returns true if the item was merged into the previous item. False if it was appended new.
    pub fn push(&mut self, val: V) -> bool {
        let old_end = self.1;
        assert_eq!(val.rle_key(), old_end, "Attempt to push non-packed items into RlePackedVec");

        let (val_packed, new_end) = val.pack();
        self.1 = new_end;

        if let Some(last) = self.0.last_mut() {
            if V::can_append_packed(last, old_end, &val_packed) {
                V::append_packed(last, old_end, val_packed);
                return true;
            }
        }

        self.0.push(val_packed);
        false
    }

    // pub fn push2(&mut self, val: BoundItem<V>) -> bool {
    //     self.push(val.0, val.1)
    // }

    // /// Returns true if the item was merged into the previous item. False if it was appended new.
    // pub fn push(&mut self, val: impl Into<V> + HasRleKey) -> bool {
    //     let start = val.rle_key();
    //     debug_assert_eq!(start, self.end());
    //     let val = val.into();
    //     debug_assert!(start < val.end_rle_key());
    //
    //     self.push_raw(val)
    // }

    /// Returns past the end of the last key.
    pub fn end(&self) -> usize {
        self.1
    }

    pub fn end_key_for_idx(&self, idx: usize) -> usize {
        match self.0.get(idx + 1) {
            Some(v) => V::packed_key(v),
            None => self.1,
        }
    }

    pub fn get_raw(&self, idx: usize) -> Option<(&V::Packed, usize)> {
        let item = self.0.get(idx)?;
        let end = self.end_key_for_idx(idx);
        Some((item, end))
    }

    pub fn get(&self, idx: usize) -> Option<V> {
        self.get_raw(idx).map(V::unpack_tuple)
    }

    // pub fn start_key_for_idx(&self, idx: usize) -> usize {
    //     assert!(idx < self.0.len(), "Index past end of vec");
    //
    //     if idx == 0 {
    //         0
    //     } else {
    //         self.0[idx - 1].end_rle_key()
    //     }
    // }

    // Forward to vec.
    // pub fn last_entry_raw(&self) -> Option<&V> { self.0.last() }

    pub fn first_entry_raw(&self) -> Option<(&V::Packed, usize)> {
        // This is how [x].last() is implemented. Super cool.
        match self.0.as_slice() {
            [first, second, ..] => { Some((first, V::packed_key(second))) },
            [first] => { Some((first, self.1)) },
            [] => { None },
        }
    }

    pub fn first_entry(&self) -> Option<V> {
        self.first_entry_raw().map(V::unpack_tuple)
    }

    #[allow(unused)]
    pub fn num_entries(&self) -> usize { self.0.len() }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    #[inline]
    pub fn iter_raw(&self) -> std::slice::Iter<V::Packed> { self.0.iter() }

    pub fn iter_raw_from_idx(&self, idx: usize) -> std::slice::Iter<V::Packed> { self.0[idx..].iter() }

    pub fn iter(&self) -> RlePackedVecIter<'_, V> {
        RlePackedVecIter::new(self.iter_raw(), self.1)
    }

    // pub fn iter_merged(&self) -> MergeIter<Cloned<std::slice::Iter<V>>> { self.0.iter().cloned().merge_spans() }

    // pub fn get_stats(&self) -> RleStats {
    //     RleStats {
    //         entry_byte_size: size_of::<V>(),
    //         len: self.0.len(),
    //         capacity: self.0.capacity(),
    //     }
    // }
    //
    // pub fn print_stats(&self, name: &str, _detailed: bool) {
    //     let size = size_of::<V>();
    //     println!("-------- {} PACKED RLE 2 --------", name);
    //     println!("number of {} byte entries: {}", size, self.0.len());
    //     println!("allocated size: {}", format_size(
    //         self.0.capacity() * size,
    //         DECIMAL
    //     ));
    //     println!("(used size: {})", format_size(
    //         self.0.len() * size,
    //         DECIMAL
    //     ));
    //
    //     // for item in self.0[..100].iter() {
    //     //     println!("{:?}", item);
    //     // }
    // }

    /// Find the index of the requested item via binary search. Returns None if the needle is past
    /// the end of the RLE vec.
    pub fn find_index(&self, needle: usize) -> Option<usize> {
        // This is a bit weird. We need to search for the *last* item with a key <= end_key.
        // The built in binary search method doesn't do this. I could fork binary search - but since
        // the error result of binary search returns the resulting index, well, we can just return
        // that directly.
        let result = self.0.binary_search_by_key(&needle, V::packed_key);

        match result {
            Err(idx) => {
                if needle >= self.1 {
                    return None;
                }

                // Should be impossible since the first item always starts at 0 and we're not empty.
                debug_assert_ne!(idx, 0);

                if cfg!(debug_assertions) {
                    let i = idx - 1;
                    assert!(i < self.0.len());
                    assert!(needle >= V::packed_key(&self.0[i]));
                    assert!(needle < self.end_key_for_idx(i));
                }

                Some(idx - 1)
            },
            Ok(idx) => {
                if cfg!(debug_assertions) {
                    assert!(idx < self.0.len());
                    assert!(needle >= V::packed_key(&self.0[idx]));
                    assert!(needle < self.end_key_for_idx(idx));
                }

                Some(idx)
            },
        }
    }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some(found value).
    pub fn find_raw(&self, needle: usize) -> Option<(&V::Packed, usize)> {
        self.find_index(needle).map(|idx| {
            (&self.0[idx], self.end_key_for_idx(idx))
        })
    }

    pub fn find(&self, needle: usize) -> Option<V> {
        self.find_raw(needle).map(V::unpack_tuple)
    }

    // /// Find the item at range, cloning and trimming it down to size. This is generally less
    // /// efficient than using find_with_offset and friends, but its much more convenient.
    // ///
    // /// Note the returned value might be smaller than the passed range.
    // ///
    // /// The start of the item is the range start.
    // #[allow(unused)]
    // pub fn find_and_split(&self, range: DTRange) -> V where V: SplitableSpan {
    //     self.find_and_split_ctx(range, &())
    // }
    //
    // /// Returns None if the range start is past the end of the vec.
    // #[allow(unused)]
    // pub fn find_and_split_ctx(&self, range: DTRange, ctx: &V::Ctx) -> Option<V> where V: SplitableSpanCtx {
    //     let (item, offset) = self.find_with_offset(range.start)?;
    //     let mut item = item.clone();
    //     item.truncate_keeping_right_ctx(offset, ctx);
    //     if item.len() > range.len() {
    //         item.truncate_ctx(range.len(), ctx);
    //     }
    //     item
    // }

    /// Find an entry in the list with the specified key using binary search.
    ///
    /// If found returns Some((found value, value end, offset within value))
    pub fn find_with_offset_raw(&self, needle: usize) -> Option<(&V::Packed, usize, usize)> {
        let (item, end) = self.find_raw(needle)?;
        let start = V::packed_key(item);
        Some((item, end, needle - start))
    }

    /// If found, returns Some((value, offset)).
    pub fn find_with_offset(&self, needle: usize) -> Option<(V, usize)> {
        let item = self.find(needle)?;
        let start = item.rle_key();
        Some((item, needle - start))
    }


    // pub fn find_packed_range(&self, needle: TimeSpan) -> (&V, TimeSpan) {
    //     let (v, offset) = self.find_packed(needle.start);
    //
    //     (v,
    // }

    // /// Find an entry in the list with the specified key using binary search.
    // ///
    // /// If found, item is returned by mutable reference as Some((&mut item, offset)).
    // #[allow(unused)]
    // pub fn find_mut(&mut self, needle: usize) -> Option<ItemAt<&mut V>> {
    //     self.find_index(needle).map(|idx| {
    //         // unsafe { self.0.get_unchecked_mut(idx) }
    //         ItemAt(
    //             self.start_key_for_idx(idx),
    //             &mut self.0[idx]
    //         )
    //     })
    // }

    // pub fn contains_needle(&self, needle: usize) -> bool {
    //     !self.is_empty() && self.find_index(needle).is_ok()
    // }

    /// Replace the specified range in the vec with the item. This function is O(n) as it may need
    /// to shuffle items around.
    pub fn set_range(&mut self, _start: usize, _item: V) {
        // TODO: Use the implementation from ost IndexTree.
        todo!()
    }

    // /// Assert there's no possibility for items to be further compacted
    // pub(crate) fn check_fully_merged(&self) {
    //     for i in 1..self.0.len() {
    //         assert!(!self.0[i-1].can_append(&self.0[i]));
    //     }
    // }
}

impl<X: HasLength, V: PackedRleItem> FromIterator<X> for RlePackedVec3<V> where V: From<X> {
    fn from_iter<T: IntoIterator<Item=X>>(iter: T) -> Self {
        let mut rle = Self::new();
        for item in iter {
            rle.push(item.into());
        }
        rle
    }
}

// impl<X, V: HasRleKey + MergableAtAbsolute> Extend<X> for RlePackedVec2<V> where V: From<X> {
//     fn extend<T: IntoIterator<Item=X>>(&mut self, iter: T) {
//         for item in iter {
//             self.push_raw(item.into());
//         }
//     }
// }

impl<V: PackedRleItem> Default for RlePackedVec3<V> {
    fn default() -> Self {
        Self(Vec::default(), 0)
    }
}

// impl<V: HasLength + MergableSpan + Searchable + HasRleKey> RlePackedVec<V> {
//     pub fn get(&self, idx: usize) -> V::Item {
//         let (v, offset) = self.find_packed_with_offset(idx);
//         v.at_offset(offset)
//     }
// }

// // Seems kinda redundant but eh.
// impl<V: HasLength + MergableSpan + Debug + Sized> AppendRle<V> for RlePackedVec<V> {
//     fn push_rle(&mut self, item: V) -> bool { self.push(item) }
//     fn push_reversed_rle(&mut self, _item: V) -> bool { unimplemented!(); }
// }

// // This works, but it just returns the raw values. It's up to the caller to (inconveniently) figure
// // out the starting key. Might be better to just leave this out?
// impl<V, I: SliceIndex<[V]>> Index<I> for RlePackedVec3<V> {
//     type Output = I::Output;
//
//     #[inline]
//     fn index(&self, index: I) -> &Self::Output {
//         self.0.index(index)
//     }
// }


#[derive(Debug, Clone)]
pub struct RlePackedVecIter<'a, V: PackedRleItem> {
    head: Option<&'a V::Packed>,
    rest: std::slice::Iter<'a, V::Packed>,
    end: usize,
}

impl<'a, V: PackedRleItem> RlePackedVecIter<'a, V> {
    fn new(mut inner_iter: std::slice::Iter<'a, V::Packed>, end: usize) -> Self {
        Self {
            head: inner_iter.next(),
            rest: inner_iter,
            end
        }
    }
}

impl<'a, V: PackedRleItem> Iterator for RlePackedVecIter<'a, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        // We'll return the head item, and shuffle the next item from rest in so we can
        // figure out the end point for this item.
        let item = self.head?;

        self.head = self.rest.next();
        let end = if let Some(next_item) = self.head {
            V::packed_key(next_item)
        } else {
            self.end
        };

        Some(V::unpack(item, end))
    }
}

// impl<'a, V: HasRleKey + MergableAtAbsolute> Iterator for RlePackedVecIter<'a, V> {
//     type Item = ItemAt<&'a V>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         let val = self.inner_iter.next()?;
//         let start = self.item_start;
//         self.item_start = val.end_rle_key();
//         Some(ItemAt(start, val))
//     }
// }

// fn id_clone<V: Clone>(v: &V) -> V {
//     v.clone()
// }

// We could just use .iter().map() - and thats pretty sensible in most cases. But this inline
// approach lets us avoid a .clone(). (Is this a good idea? Not sure!)
//
// TODO: Could split this into two iterators - one to iterate through (Range, &V) and another
// which wraps that, and clones and splits.
#[derive(Debug, Clone)]
pub struct RlePackedVecRangeIter<'a, V: PackedRleItem, I: SplitableSpanCtx, F: Fn(&V::Packed, usize) -> I> {
    head: Option<&'a V::Packed>,
    rest: std::slice::Iter<'a, V::Packed>,

    range: DTRange,
    ctx: &'a I::Ctx, // This could have a different lifetime specifier.
    map_fn: F,
}

impl<V: PackedRleItem + SplitableSpanCtx> RlePackedVec3<V> {
    pub fn iter_range(&self, range: DTRange) -> RlePackedVecRangeIter<V, V, impl Fn(&V::Packed, usize) -> V> where V: SplitableSpan {
        self.iter_range_ctx(range, &())
    }

    pub fn iter_range_ctx<'a>(&'a self, range: DTRange, ctx: &'a V::Ctx) -> RlePackedVecRangeIter<'a, V, V, impl Fn(&V::Packed, usize) -> V> {
        self.iter_range_map_ctx(range, ctx, V::unpack)
    }
}

impl<V: PackedRleItem> RlePackedVec3<V> {
    // Yeah these map functions are dirty, but only at compile time. At runtime they should be free.
    pub fn iter_range_map<I: SplitableSpan + HasLength, F: Fn(&V::Packed, usize) -> I>(&self, range: DTRange, map_fn: F) -> RlePackedVecRangeIter<V, I, F>
    {
        self.iter_range_map_ctx(range, &(), map_fn)
    }

    // pub fn iter_range_into<I: SplitableSpan + HasLength + From<ItemAt<V>>>(&self, range: DTRange) -> RlePackedVecRangeIter<V, I, impl Fn(ItemAt<&V>) -> I>
    // {
    //     self.iter_range_map_ctx(range, &(), |ItemAt(start, v)| I::from(ItemAt(start, v.clone())))
    // }

    pub fn iter_range_map_ctx<'a, I: SplitableSpanCtx, F: Fn(&V::Packed, usize) -> I>(&'a self, mut range: DTRange, ctx: &'a I::Ctx, map_fn: F) -> RlePackedVecRangeIter<'a, V, I, F> {
        let start_idx = self
            .find_index(range.start)
            .unwrap_or(self.0.len()); // If you request a range outside of the vec, we should return an empty iterator.

        let mut inner_iter = self.0[start_idx..].iter();
        let head = inner_iter.next();

        // We need the end while iterating to know the endpoint of the last item.
        range.end = range.end.min(self.1);
        range.start = range.start.min(self.1); // And the start should always be earlier than that.

        RlePackedVecRangeIter {
            head,
            rest: inner_iter,

            range,
            ctx,
            map_fn
        }
    }
}

impl<'a, V: PackedRleItem, I: HasLength + SplitableSpanCtx, F: Fn(&V::Packed, usize) -> I> Iterator for RlePackedVecRangeIter<'a, V, I, F> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.head?;

        self.head = self.rest.next();
        let item_end = if let Some(next_item) = self.head {
            V::packed_key(next_item)
        } else {
            self.range.end
        };

        let item_start = V::packed_key(item);
        debug_assert!(item_end >= self.range.start);

        if item_start >= self.range.end { return None; }

        let mut i = (self.map_fn)(item, item_end);
        if item_end > self.range.end {
            i.truncate_ctx(self.range.end - item_start, self.ctx);
        }
        if item_start < self.range.start {
            i.truncate_keeping_right_ctx(self.range.start - item_start, self.ctx);
        }
        Some(i)
    }
}


impl PackedRleItem for DTRange {
    type Packed = usize; // Just the start position.

    fn pack(&self) -> (Self::Packed, usize) {
        (self.start, self.end)
    }

    fn unpack(packed: &Self::Packed, end: usize) -> Self {
        DTRange { start: *packed, end }
    }

    fn packed_key(packed: &Self::Packed) -> usize {
        *packed
    }

    fn can_append_packed(_a: &Self::Packed, a_end: usize, b: &Self::Packed) -> bool {
        a_end == *b
    }

    fn append_packed(_item: &mut Self::Packed, _item_end: usize, _other: Self::Packed) {
        // We do nothing here, since the only thing we would do is update the end position - but
        // thats taken care of for us.
    }
}

impl<T: Clone + Eq> PackedRleItem for RleDRun<T> {
    type Packed = (usize, T); // Start, val. Should probably be a struct...

    fn pack(&self) -> (Self::Packed, usize) {
        ((self.start, self.val.clone()), self.end)
    }

    fn unpack(packed: &Self::Packed, end: usize) -> Self {
        Self {
            start: packed.0,
            end,
            val: packed.1.clone(),
        }
    }

    fn packed_key(packed: &Self::Packed) -> usize {
        packed.0
    }

    fn can_append_packed(a: &Self::Packed, a_end: usize, b: &Self::Packed) -> bool {
        // a_end == b_start && a.val == b.val.
        a_end == b.0 && a.1 == b.1
    }

    fn append_packed(_item: &mut Self::Packed, _item_end: usize, _other: Self::Packed) {} // Nothing to do here.
}

#[cfg(test)]
mod tests {
    use rle::SplitableSpanHelpers;
    use crate::rle::RleVec;
    use super::*;

    #[test]
    fn rle_iter_range() {
        let mut rle: RlePackedVec3<DTRange> = RlePackedVec3::new();
        rle.push((0..10).into());

        // This is a sad example.
        let items = rle.iter_range((5..8).into()).collect::<Vec<_>>();
        assert_eq!(&items, &[(5..8).into()]);
    }

    #[test]
    fn iter_empty() {
        let rle: RlePackedVec3<DTRange> = RlePackedVec3::new();
        let entries_a = rle.iter().collect::<Vec<_>>();
        // let entries_b = rle.iter_range_map((0..0).into(), |x, end| *x).collect::<Vec<_>>();
        let entries_c = rle.iter_range((0..0).into()).collect::<Vec<_>>();
        assert!(entries_a.is_empty());
        // assert!(entries_b.is_empty());
        assert!(entries_c.is_empty());
    }

    #[test]
    fn find_index_is_correct() {
        let mut rle: RlePackedVec3<RleDRun<u8>> = RlePackedVec3::new();
        // rle.push_raw(RangeEnd)
        rle.push(RleDRun { val: 1, start: 0, end: 5 });
        rle.push(RleDRun { val: 1, start: 5, end: 10 }); // Should be merged.
        rle.push(RleDRun { val: 2, start: 10, end: 20 });

        let vals = rle.iter().collect::<Vec<_>>();
        assert_eq!(vals.as_slice(), &[
            RleDRun { val: 1, start: 0, end: 10 },
            RleDRun { val: 2, start: 10, end: 20 },
        ]);

        assert_eq!(rle.find(0), Some(RleDRun { start: 0, end: 10, val: 1 }));
        assert_eq!(rle.find(5), Some(RleDRun { start: 0, end: 10, val: 1 }));
        assert_eq!(rle.find(9), Some(RleDRun { start: 0, end: 10, val: 1 }));
        assert_eq!(rle.find(10), Some(RleDRun { start: 10, end: 20, val: 2 }));
        assert_eq!(rle.find(19), Some(RleDRun { start: 10, end: 20, val: 2 }));
        assert_eq!(rle.find(20), None);
    }

    // use crate::order::OrderSpan;
    // use crate::rle::KVPair;
    // use crate::rle::simple_rle::RlePackedVec;
    //
    // #[test]
    // fn rle_finds_at_offset() {
    //     let mut rle: RlePackedVec<KVPair<OrderSpan>> = RlePackedVec::new();
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
    //     let mut rle: RlePackedVec<KVPair<OrderSpan>> = RlePackedVec::new();
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
    //     let mut rle: RlePackedVec<KVPair<OrderSpan>> = RlePackedVec::new();
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

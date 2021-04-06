use smallvec::SmallVec;
use std::ops::Index;
use std::fmt::Debug;

// An entry is expected to contain multiple items.
pub trait SplitListEntry {
    /// A single item, returned by indexing into the entry. Many implementations will just have this
    /// also return a SplitListEntry.
    type Item;

    /// The number of child items in the entry
    fn len(&self) -> usize;

    /// Split the entry, returning the part of the entry which was jettisoned. After truncating at
    /// `pos`, self.len() == `pos` and the returned value contains the rest of the items.
    ///
    /// ```ignore
    /// let initial_len = entry.len();
    /// let rest = entry.truncate(truncate_at);
    /// assert!(initial_len == truncate_at + rest.len());
    /// ```
    ///
    /// `at` parameter must obey *0 < at < entry.len()*
    fn truncate(&mut self, at: usize) -> Self;

    /// See if the other item can be appended to self. `can_append` will always be called
    /// immediately before `append`.
    fn can_append(&self, other: &Self) -> bool;
    fn append(&mut self, other: Self);
}

const DEFAULT_BUCKET_SIZE: usize = 10;
const BUCKET_INLINED_SIZE: usize = 13;

// At the high level, we've got a vector of items
#[derive(Clone, Debug)]
// struct SplitList<Entry: SplitListEntry<Item=Item>, Item> where Entry: Index<usize, Output=Item> {
pub struct SplitList<Entry> where Entry: SplitListEntry {
    /// The number of items in each bucket. Fixed at list creation time.
    // TODO: Consider making this a static type parameter.
    bucket_size: usize,

    content: Vec<Bucket<Entry>>,

    total_len: usize,
}

// Each bucket stores a few entries. How many? I have no idea - need to benchmark it!
type Bucket<T> = SmallVec<[T; BUCKET_INLINED_SIZE]>;

// where Entry: SplitListEntry<Item=Item>
// fn append_bucket_entry<Entry: SplitListEntry>(bucket: &mut Bucket<Entry>, entry: Entry) {
//     // See if we can append it to the end
//     if let Some(last) = bucket.last_mut() {
//         if last.can_append(&entry) {
//             last.append(entry);
//         } else {
//             bucket.push(entry);
//         }
//     } else {
//         bucket.push(entry);
//     }
// }

#[derive(Copy, Clone, Debug)]
struct BucketCursor {
    idx: usize,
    offset: usize
}

impl BucketCursor {
    fn roll_next<Entry>(&mut self, bucket: &Bucket<Entry>) where Entry: SplitListEntry {
        let entry = &bucket[self.idx];
        if self.offset == entry.len() {
            self.idx += 1;
            self.offset = 0;
        }
    }

    fn zero() -> Self {
        BucketCursor { idx: 0, offset: 0 }
    }
}

impl<Entry, Item> SplitList<Entry> where Entry: SplitListEntry<Item=Item>, Entry: Debug {
    pub fn new() -> Self {
        Self::new_with_bucket_size(DEFAULT_BUCKET_SIZE)
    }

    pub fn new_with_bucket_size(bucket_size: usize) -> Self {
        Self {
            bucket_size,
            content: Vec::new(),
            total_len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.total_len
    }

    /// Go from an external position to a triple of (container idx, bucket offset, bucket cursor)
    ///
    /// NOTE: stick_idx only sticks to the end of an entry inside a bucket. It does not stick to
    /// the end of a bucket if the index lands at the bucket's end. This is needed for
    /// replace_range below.
    ///
    /// NOTE: This method can return cursors past the end of the collection!
    fn get_internal_idx(&self, index: usize, stick_end: bool) -> (usize, usize, BucketCursor) {
        // Edge cases are the worst.
        if index == 0 {
            return (0, 0, BucketCursor::zero());
        }

        // if index == self.total_len {
        //     // Point at the end of the last element.
        //     debug_assert!(self.content.len() > 0);
        //     let idx = self.content.len() - 1;
        //     let bucket = &self.content[idx];
        //     debug_assert!(bucket.len() > 0);
        //     let bucket_idx = bucket.len() - 1;
        //     return (idx, BucketCursor { idx: bucket_idx, offset: bucket[bucket_idx].len() });
        // }

        // assert!(index < self.total_len, "Item past end of list");
        // Not sure what we should do if the requested position is the last element...

        let bucket_id = index / self.bucket_size;
        let bucket_offset = index - (bucket_id * self.bucket_size);

        if bucket_offset == 0 {
            // This might return past the end of the collection.
            // This will hit when the collection is empty, or an index exactly divides bucket_size.
            return (bucket_id, 0, BucketCursor::zero());
        } else {
            debug_assert!(bucket_id < self.content.len());
            let mut offset = bucket_offset;
            let bucket = &self.content[bucket_id];
            for (idx, item) in bucket.iter().enumerate() {
                let len = item.len();
                debug_assert!(len > 0, "List item has length of 0");
                if offset < len || (stick_end && offset == len) {
                    return (bucket_id, bucket_offset, BucketCursor { idx, offset });
                } else {
                    offset -= len;
                }
            }
            unreachable!();
        }
    }

    // pub fn append_entry(&mut self, mut entry: Entry) {
    //     let new_entry_len = entry.len();
    //     let mut remaining_len = new_entry_len;
    //     let mut room_in_last_bucket = self.content.len() * self.bucket_size - self.total_len;
    //     debug_assert!(room_in_last_bucket < self.bucket_size);
    //
    //     // I'm sure there's a cleaner way to write this, but its escaping me at the moment.
    //     // I think this is probably correct, but I suspect it could be simpler.
    //     while remaining_len > 0 {
    //         if room_in_last_bucket == 0 {
    //             self.content.push(Bucket::new());
    //             room_in_last_bucket = self.bucket_size;
    //         }
    //
    //         if room_in_last_bucket > remaining_len {
    //             // Just insert the whole item at end of the bucket.
    //             // self.content.last_mut().unwrap().push(entry);
    //             append_bucket_entry(&mut self.content.last_mut().unwrap(), entry);
    //             break;
    //         } else {
    //             // Split the item and insert as much as we can.
    //             let remainder = entry.truncate(room_in_last_bucket);
    //             append_bucket_entry(&mut self.content.last_mut().unwrap(), entry);
    //             entry = remainder;
    //             remaining_len -= room_in_last_bucket;
    //             room_in_last_bucket = 0;
    //             debug_assert_eq!(entry.len(), remaining_len);
    //         }
    //     }
    //     self.total_len += new_entry_len;
    // }


    /// Insert the entry into the bucket, ignoring sizes.
    ///
    /// Return any truncated content, and the index at which it should be inserted.
    fn insert_at(bucket: &mut Bucket<Entry>, entry: Entry, cursor: &mut BucketCursor) -> Option<Entry> {
        // TODO: Make this an associated function on Bucket or something.
        if cursor.offset == 0 {
            cursor.offset = entry.len();
            bucket.insert(cursor.idx, entry);
            return None
        }

        let item = &mut bucket[cursor.idx];
        let existing_len = item.len();

        let remainder = if cursor.offset == existing_len {
            None
        } else {
            Some(item.truncate(cursor.offset))
        };

        // The cursor now points to the end of the current element.
        debug_assert_eq!(item.len(), cursor.offset);

        // Try to append.
        if item.can_append(&entry) {
            cursor.offset += entry.len();
            item.append(entry);
            remainder
        } else {
            // Cursor at the end of the inserted element.
            cursor.idx += 1;
            cursor.offset = entry.len();
            bucket.insert(cursor.idx, entry); // TODO: Does this work past the end of the list?
            remainder
        }
    }

    pub(super) fn replace_range(&mut self, index: usize, mut entry: Entry) {
        // self.check();

        // println!("replace_range called. Index {} entry {:?} into set {:#?}", index, entry, self);
        let (mut bucket_idx, bucket_offset, mut cursor) = self.get_internal_idx(index, true);
        let new_entry_len = entry.len();
        let mut remaining_entry_len = new_entry_len;
        debug_assert!(remaining_entry_len > 0);

        let mut room_in_bucket = self.bucket_size - bucket_offset;

        loop {
            assert!(bucket_idx <= self.content.len());

            // Allow sneaky appending to the end of the list.
            if bucket_idx == self.content.len() {
                assert_eq!(cursor.idx, 0);
                assert_eq!(cursor.offset, 0);
                assert_eq!(room_in_bucket, self.bucket_size);
                self.content.push(Bucket::new());
            }

            debug_assert!(room_in_bucket > 0);
            let (remainder_for_next_bucket, mut len_replaced_here) = if room_in_bucket >= remaining_entry_len {
                // The element fits in this bucket
                (None, remaining_entry_len)
            } else {
                // We'll spill some of the element into the next bucket.
                (Some(entry.truncate(room_in_bucket)), room_in_bucket)
            };

            // Step 1: We'll insert the actual content we have at the current cursor position.
            // This may truncate an existing entry, and if so it'll be returned as remainder.
            let bucket = &mut self.content[bucket_idx];

            let remainder = Self::insert_at(bucket, entry, &mut cursor);
            // println!("Inserted remainder at - {:?} {:#?}", remainder, &self);
            // let mut bucket = &mut self.content[bucket_idx];

            // Step 2: If we displaced an item, discard or re-insert part of it.
            if let Some(mut remainder) = remainder {
                let remainder_len = remainder.len();

                // 3 cases:
                // If remainder_len < replaced_here, toss it and remove more elements in the bucket
                // If remainder_len == replaced_here, we're done
                // If remainder_len > replaced_here, chop it up and insert the remaining piece.
                if remainder_len > len_replaced_here {
                    // Chop chop! Discard the start of remainder and insert the rest with insert_at.
                    let remainder = remainder.truncate(len_replaced_here);
                    let r2 = Self::insert_at(bucket, remainder, &mut cursor);
                    // The cursor should be at the end of the existing element, so we should never
                    // be truncating anything else in turn in insert_at.
                    assert!(r2.is_none());
                    debug_assert!(remainder_for_next_bucket.is_none());
                    break; // I mean, we're done here now, right??
                } else {
                    len_replaced_here -= remainder_len;
                    drop(remainder);
                }
            }

            // Step 3: If there's still more content to delete in this bucket, scan through and
            // truncate or discard items from the bucket.
            while len_replaced_here > 0 {
                cursor.roll_next(bucket);
                if cursor.idx >= bucket.len() {
                    // We have more to remove, but we're at the end of the list and there's nothing
                    // here.
                    // dbg!(len_replaced_here, cursor, bucket_idx);
                    // dbg!(&self);
                    debug_assert_eq!(bucket_idx + 1, self.content.len());
                    break;
                }

                let here_len = bucket[cursor.idx].len();
                if len_replaced_here >= here_len {
                    // Discard this item. TODO: This would be more efficient en masse with a memmove
                    bucket.remove(cursor.idx);
                    len_replaced_here -= here_len;
                } else {
                    // The item is smaller than len_replaced_here. Truncate in place.
                    // dbg!(len_replaced_here);
                    bucket[cursor.idx] = bucket[cursor.idx].truncate(len_replaced_here);
                    break;
                }
            }

            if let Some(r) = remainder_for_next_bucket {
                entry = r;
                remaining_entry_len -= room_in_bucket;
                room_in_bucket = self.bucket_size;
                bucket_idx += 1;
                cursor = BucketCursor { offset: 0, idx: 0 };
            } else {
                // Ok all done!
                break;
            }
        }

        // If this was used to insert, we need to update total length.
        self.total_len = self.total_len.max(index + new_entry_len);

        // dbg!(&self);
        // self.check();
    }

    #[allow(unused)]
    pub fn append_entry(&mut self, entry: Entry) {
        self.replace_range(self.total_len, entry);
    }

    #[allow(unused)]
    pub(super) fn check(&self) {
        let mut counted_len = 0;

        for (idx, bucket) in self.content.iter().enumerate() {
            assert!(bucket.len() > 0, "Found empty bucket, which is invalid.");

            let mut bucket_len = 0;
            for entry in bucket {
                bucket_len += entry.len();
            }
            if idx + 1 != self.content.len() {
                // Every bucket except the last should be full.
                assert_eq!(bucket_len, self.bucket_size, "Internal bucket is not full");
            }
            counted_len += bucket_len;
        }

        assert_eq!(counted_len, self.total_len, "Total length does not match item count");
    }

    #[allow(unused)]
    pub fn print_stats(&self) {
        let mut size_counts = vec!();
        let mut bucket_item_counts = vec!();
        for bucket in &self.content {
            let bucket_count = bucket.len();
            if bucket_count >= bucket_item_counts.len() {
                bucket_item_counts.resize(bucket_count + 1, 0);
            }
            bucket_item_counts[bucket_count] += 1;

            for entry in bucket {
                let len = entry.len();
                if len >= size_counts.len() {
                    size_counts.resize(len + 1, 0);
                }
                size_counts[len] += 1;
            }
        }

        println!("bucket item counts {:#?}", bucket_item_counts);
        println!("size counts {:#?}", size_counts);
        for (i, len) in size_counts.iter().enumerate() {
            println!("{} count: {}", i, len);
        }


    }
}

impl<Entry, Item> Index<usize> for SplitList<Entry> where Entry: SplitListEntry<Item=Item> + Index<usize, Output=Item>, Entry: Debug {
    type Output = Item;

    fn index(&self, index: usize) -> &Self::Output {
        let (bucket_idx, _, cursor) = self.get_internal_idx(index, false);
        &self.content[bucket_idx][cursor.idx][cursor.offset]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple example where entries are runs of positive or negative items.
    impl SplitListEntry for i32 {
        type Item = bool; // Negative runs = false, positive = true.

        fn len(&self) -> usize {
            return self.abs() as usize;
        }

        fn truncate(&mut self, at: usize) -> Self {
            let at = at as i32;
            // dbg!(at, *self);
            assert!(at > 0 && at < self.abs());
            assert_ne!(*self, 0);

            let abs = self.abs();
            let sign = self.signum();
            *self = at * sign;
            return (abs - at) * sign;
        }

        fn can_append(&self, other: &Self) -> bool {
            self.signum() == other.signum()
        }

        fn append(&mut self, other: Self) {
            assert!(self.can_append(&other));
            *self += other;
        }
    }

    #[test]
    fn foo() {
        let mut list: SplitList<i32> = SplitList::new_with_bucket_size(50);

        list.append_entry(123);
        list.check();
        assert_eq!(list.len(), 123);

        list.append_entry(2);
        list.check();
        // Check the added content was appended inline
        assert_eq!(list.content.last().unwrap().len(), 1);
        assert_eq!(list.len(), 125);


        list.replace_range(2, -1);
        // dbg!(list);
        list.replace_range(1, -4);
        list.replace_range(0, 4);
        list.replace_range(0, 5);
        // dbg!(list);
    }

    #[test]
    fn foo2() {
        // Regression.
        let mut list: SplitList<i32> = SplitList::new_with_bucket_size(50);

        list.append_entry(-12);
        list.append_entry(20);
        list.replace_range(8, 4);
        list.check();
        // dbg!(list);
    }
}
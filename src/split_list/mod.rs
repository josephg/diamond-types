use smallvec::SmallVec;
use std::ops::Index;

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
    /// ```
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

// At the high level, we've got a vector of items
#[derive(Clone, Debug)]
// struct SplitList<Entry: SplitListEntry<Item=Item>, Item> where Entry: Index<usize, Output=Item> {
struct SplitList<Entry> where Entry: SplitListEntry {
    /// The number of items in each bucket. Fixed at list creation time.
    // TODO: Consider making this a static type parameter.
    bucket_size: usize,

    content: Vec<Bucket<Entry>>,

    total_len: usize,
}

// Each bucket stores a few entries. How many? I have no idea - need to benchmark it!
type Bucket<T> = SmallVec<[T; 3]>;

// where Entry: SplitListEntry<Item=Item>
fn append_bucket_entry<Entry: SplitListEntry>(bucket: &mut Bucket<Entry>, entry: Entry) {
    // See if we can append it to the end
    if let Some(last) = bucket.last_mut() {
        if last.can_append(&entry) {
            last.append(entry);
        } else {
            bucket.push(entry);
        }
    } else {
        bucket.push(entry);
    }
}

impl<Entry, Item> SplitList<Entry> where Entry: SplitListEntry<Item=Item> {
    pub fn new() -> Self {
        Self::new_with_bucket_size(50)
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

    pub fn append_entry(&mut self, mut entry: Entry) {
        let new_entry_len = entry.len();
        let mut remaining_len = new_entry_len;
        let mut room_in_last_bucket = self.content.len() * self.bucket_size - self.total_len;
        debug_assert!(room_in_last_bucket < self.bucket_size);

        // I'm sure there's a cleaner way to write this, but its escaping me at the moment.
        // I think this is probably correct, but I suspect it could be simpler.
        while remaining_len > 0 {
            if room_in_last_bucket == 0 {
                self.content.push(Bucket::new());
                room_in_last_bucket = self.bucket_size;
            }

            if room_in_last_bucket > remaining_len {
                // Just insert the whole item at end of the bucket.
                // self.content.last_mut().unwrap().push(entry);
                append_bucket_entry(&mut self.content.last_mut().unwrap(), entry);
                break;
            } else {
                // Split the item and insert as much as we can.
                let remainder = entry.truncate(room_in_last_bucket);
                append_bucket_entry(&mut self.content.last_mut().unwrap(), entry);
                entry = remainder;
                remaining_len -= room_in_last_bucket;
                room_in_last_bucket = 0;
                debug_assert_eq!(entry.len(), remaining_len);
            }
        }
        self.total_len += new_entry_len;
    }

    pub(super) fn check(&self) {
        let mut counted_len = 0;

        for (idx, bucket) in self.content.iter().enumerate() {
            let mut bucket_len = 0;
            for entry in bucket {
                bucket_len += entry.len();
            }
            if idx + 1 != self.content.len() {
                // Every bucket except the last should be full.
                assert_eq!(bucket_len, self.bucket_size);
            }
            counted_len += bucket_len;
        }

        assert_eq!(counted_len, self.total_len);
    }
}

impl<Entry, Item> Index<usize> for SplitList<Entry> where Entry: SplitListEntry<Item=Item> + Index<usize, Output=Item> {
    type Output = Item;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < self.total_len, "Indexing past end of list");
        let bucket_id = index / self.bucket_size;
        let bucket = &self.content[bucket_id];

        let mut remainder = index - (bucket_id * self.bucket_size);
        for item in bucket {
            let len = item.len();
            debug_assert!(len > 0, "List item has length of 0");
            if len > remainder {
                return &item[remainder];
            } else {
                remainder -= len;
            }
        }

        panic!("Internal constraint violation - Bucket does not contain item");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    struct TestEntry(i32);

    /// Simple example where entries are runs of positive or negative items.
    impl SplitListEntry for TestEntry {
        type Item = bool; // Negative runs = false, positive = true.

        fn len(&self) -> usize {
            return self.0 as usize;
        }

        fn truncate(&mut self, at: usize) -> Self {
            let at = at as i32;
            assert!(at > 0 && at < self.0);
            assert_ne!(self.0, 0);

            let abs = self.0.abs();
            let sign = self.0.signum();
            self.0 = at * sign;
            return TestEntry((abs - at) * sign);
        }

        fn can_append(&self, other: &Self) -> bool {
            self.0.signum() == other.0.signum()
        }

        fn append(&mut self, other: Self) {
            assert!(self.can_append(&other));
            self.0 += other.0;
        }
    }

    // impl Index<usize> for TestEntry {
    //     type Output = bool;
    //
    //     fn index(&self, index: usize) -> &Self::Output {
    //         &(self.0 < 0)
    //     }
    // }

    #[test]
    fn foo() {
        let mut list: SplitList<TestEntry> = SplitList::new_with_bucket_size(50);

        list.append_entry(TestEntry(123));
        list.check();
        assert_eq!(list.len(), 123);

        list.append_entry(TestEntry(2));
        list.check();
        // Check the added content was appended inline
        assert_eq!(list.content.last().unwrap().len(), 1);
        assert_eq!(list.len(), 125);
    }
}
/// This is a variant of simple_rle which allows spans to be replaced. Internally it uses a vec with
/// gaps every X entries when appending. The gaps are consumed when items are spilled.

use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use crate::rle::RLEKey;
use std::mem;

const GAP: u32 = u32::MAX;

// Each entry has a key (which we search by), a span and a value at that key.
// Gaps are identified by a key which is set to GAP (u32::MAX).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MutRle<V: SplitableSpan + Copy + Debug + Sized> {
    content: Vec<(RLEKey, V)>,
    /// We'll insert a gap every X entries
    gap_frequency: u8,
    /// The number of appends remaining before the next gap is inserted in the data structure
    next_gap: u8,

    shuffles: usize,
}

impl<V: SplitableSpan + Copy + Debug + Sized + Default> MutRle<V> {
    pub fn new(gap_frequency: u8) -> Self {
        Self {
            content: Vec::new(),
            gap_frequency,
            next_gap: gap_frequency,

            shuffles: 0,
        }
    }

    /// Stolen and modified from the standard library:
    /// https://doc.rust-lang.org/std/vec/struct.Vec.html#method.binary_search_by -> src.
    /// Returns (idx, offset into item at idx).
    pub fn find_idx(&self, needle: RLEKey) -> Option<(usize, RLEKey)>
    {
        // println!("---- {}", needle);
        let mut size = self.content.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mut mid = left + size / 2;

            // SAFETY: the call is made safe by the following invariants:
            // - `mid >= 0`
            // - `mid < size`: `mid` is limited by `[left; right)` bound.
            let mut entry = unsafe { self.content.get_unchecked(mid) };
            // dbg!(left, right, mid, entry);
            while entry.0 == GAP {
                mid -= 1;
                entry = unsafe { self.content.get_unchecked(mid) };
            }

            if needle < entry.0 {
                right = mid;
            } else {
                let offset = needle - entry.0;
                if offset >= entry.1.len() as u32 {
                    left = mid + 1;
                    // Skip gaps. This is probably not the most efficient way to do this.
                    while left < right && unsafe { self.content.get_unchecked(left) }.0 == GAP {
                        left += 1;
                    }
                } else {
                    return Some((mid, offset));
                }
            }

            size = right - left;
        }
        None
    }

    pub fn find(&self, needle: RLEKey) -> Option<(V, RLEKey)> {
        self.find_idx(needle).map(|(idx, offset)| {
            (self.content[idx].1, offset)
        })
    }

    /// Inserts the passed entry into the (start of) the specified index. Shuffles subsequent
    /// entries forward in the list until the next gap.
    fn shuffle_insert_before(&mut self, mut new_entry: (RLEKey, V), mut idx: usize) {
        // TODO: Consider rewriting this to scan and use ptr::copy instead.
        while idx < self.content.len() {
            let old_entry = &mut self.content[idx];
            if old_entry.0 == GAP {
                *old_entry = new_entry;
                return;
            } else if new_entry.0 + new_entry.1.len() as u32 == old_entry.0 && new_entry.1.can_append(&old_entry.1) {
                new_entry.1.append(old_entry.1);
                *old_entry = new_entry;
                return;
            } else {
                // shuffle shuffle
                mem::swap(old_entry, &mut new_entry);
                self.shuffles += 1;
                idx += 1;
            }
        }
        self.content.push(new_entry);
    }

    /// Wrapper around shuffle_insert_before which tries to prepend before scanning.
    /// Returns index at which item was actually inserted.
    fn shuffle_insert_after(&mut self, mut new_entry: (RLEKey, V), mut idx: usize) -> usize {
        let old_entry = &mut self.content[idx];
        if old_entry.0 + old_entry.1.len() as u32 == new_entry.0 && old_entry.1.can_append(&new_entry.1) {
            old_entry.1.append(new_entry.1);
            idx
        } else {
            self.shuffle_insert_before(new_entry, idx + 1);
            idx + 1
        }
    }

    // Returns remainder, which should be re-inserted by caller.
    fn clear_range(&mut self, mut idx: usize, mut offset: RLEKey, clear_end_key: RLEKey) -> Option<(RLEKey, V)> {
        while idx < self.content.len() {
            let entry = &mut self.content[idx];
            if entry.0 >= clear_end_key { break; }

            let mut remainder = if offset > 0 {
                (entry.0 + offset, entry.1.truncate(offset as _))
            } else {
                let k = entry.0;
                entry.0 = GAP;
                // I'm leaving entry.1 alone. I could clear it - its Copy so it shouldn't matter.
                (k, entry.1)
            };
            offset = 0;

            if remainder.0 + remainder.1.len() as u32 <= clear_end_key {
                // Discard and advance.
                idx += 1;
            } else {
                // Delete a portion of remainder, and re-insert the rest.
                remainder.1 = remainder.1.truncate((clear_end_key - remainder.0) as usize);
                remainder.0 = clear_end_key;
                return Some(remainder);
            }
        }
        None
    }

    pub fn replace_range(&mut self, base: RLEKey, val: V) {
        match self.find_idx(base) {
            None => {
                // This is currently only supported if we're appending.
                if let Some(entry) = self.content.last() {
                    assert!(entry.0 + entry.1.len() as u32 <= base);
                }
                self.append(base, val);
            }
            Some((mut idx, mut offset)) => {
                let remainder = self.clear_range(idx, offset, base + val.len() as u32);
                idx = self.shuffle_insert_after((base, val), idx);
                if let Some(remainder) = remainder {
                    self.shuffle_insert_after(remainder, idx);
                }
            }
        }
    }

    pub fn append(&mut self, base: RLEKey, val: V) {
        if let Some((ref last_base, ref mut v)) = self.content.last_mut() {
            if base == *last_base + v.len() as u32 && v.can_append(&val) {
                v.append(val);
                return;
            }
        }

        if self.next_gap == 0 {
            self.content.push((GAP, V::default()));
            self.next_gap = self.gap_frequency - 1;
        } else {
            self.next_gap -= 1;
        }

        self.content.push((base, val));
    }

    pub fn last(&self) -> Option<&(RLEKey, V)> {
        self.content.last()
        // if self.content.len() == 0 { return None; }
        //
        // let mut idx = self.content.len() - 1;
        // while self.content[idx].0 == GAP { idx -= 1; }
        // Some(&self.content[idx])
    }

    // pub fn num_entries(&self) -> usize { self.0.len() }

    pub fn check(&self) {
        // The first and last entries (if they exist) must never be gaps.
        if !self.content.is_empty() {
            assert_ne!(self.content[0].0, GAP);
            assert_ne!(self.content.last().unwrap().0, GAP);
        }
    }

    pub fn print_stats(&self, _detailed: bool) {
        let size = std::mem::size_of::<(RLEKey, V)>();
        println!("-------- Mutable RLE --------");
        println!("number of {} byte entries: {}", size, self.content.len());
        println!("size: {}", self.content.capacity() * size);
        println!("shuffles: {}", self.shuffles);

        let filled = self.content.iter().fold(0, |acc, x| {
            if x.0 == GAP { acc } else { acc + 1 }
        });
        println!("(efficient size: {})", filled * size);
    }
}

impl<V: EntryTraits> MutRle<V> {
    pub fn get(&self, idx: RLEKey) -> V::Item {
        let (v, offset) = self.find(idx).unwrap();
        v.at_offset(offset as usize)
    }
}

#[cfg(test)]
mod tests {
    use crate::order::OrderMarker;
    use crate::rle::mutable_rle::MutRle;

    #[test]
    fn smoke_test() {
        let mut rle: MutRle<OrderMarker> = MutRle::new(10);

        rle.append(1, OrderMarker { order: 1000, len: 2 });
        assert_eq!(rle.find(1), Some((OrderMarker { order: 1000, len: 2 }, 0)));
        assert_eq!(rle.find(2), Some((OrderMarker { order: 1000, len: 2 }, 1)));
        assert_eq!(rle.find(3), None);
    }

    #[test]
    fn appends() {
        let mut rle: MutRle<OrderMarker> = MutRle::new(10);
        rle.append(1, OrderMarker { order: 1000, len: 2 });

        // This should get appended.
        rle.append(3, OrderMarker { order: 1002, len: 1 });
        assert_eq!(rle.find(3), Some((OrderMarker { order: 1000, len: 3 }, 2)));
        assert_eq!(rle.content.len(), 1);
    }

    #[test]
    fn gaps() {
        let mut rle: MutRle<OrderMarker> = MutRle::new(1);
        rle.append(1, OrderMarker { order: 1000, len: 1 });
        rle.append(3, OrderMarker { order: 1011, len: 2 });
        rle.append(5, OrderMarker { order: 1022, len: 1 });
        assert_eq!(rle.content.len(), 5);

        assert_eq!(rle.find(3), Some((OrderMarker { order: 1011, len: 2 }, 0)));
        assert_eq!(rle.find(4), Some((OrderMarker { order: 1011, len: 2 }, 1)));
    }

    #[test]
    fn mutate() {
        let mut rle: MutRle<OrderMarker> = MutRle::new(3);
        rle.append(0, OrderMarker { order: 1000, len: 5 });
        rle.replace_range(1, OrderMarker { order: 2000, len: 2 });

        assert_eq!(rle.content.len(), 3);
        assert_eq!(rle.content[0], (0, OrderMarker { order: 1000, len: 1 }));
        assert_eq!(rle.content[1], (1, OrderMarker { order: 2000, len: 2 }));
        assert_eq!(rle.content[2], (3, OrderMarker { order: 1003, len: 2 }));

        rle.replace_range(2, OrderMarker { order: 3000, len: 3 });
        assert_eq!(rle.content.len(), 3);
        assert_eq!(rle.content[0], (0, OrderMarker { order: 1000, len: 1 }));
        assert_eq!(rle.content[1], (1, OrderMarker { order: 2000, len: 1 }));
        assert_eq!(rle.content[2], (2, OrderMarker { order: 3000, len: 3 }));

        // dbg!(&rle);
    }
}
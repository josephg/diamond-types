use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

type RLEKey = u32;

pub trait RLEEntry: Deref<Target = Self::Value> + DerefMut + Debug {
    type Value: SplitableSpan + Copy + Debug + Sized;
    fn get_key(&self) -> RLEKey;
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct RLEPair<V>(pub RLEKey, pub V);

impl<V: SplitableSpan + Copy + Debug + Sized> RLEEntry for RLEPair<V> {
    type Value = V;
    fn get_key(&self) -> u32 { self.0 }
}

impl<V: SplitableSpan + Copy + Debug + Sized> Deref for RLEPair<V> {
    type Target = V;
    fn deref(&self) -> &Self::Target { &self.1 }
}
impl<V: SplitableSpan + Copy + Debug + Sized> DerefMut for RLEPair<V> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.1 }
}

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
// pub struct RLE<K: Copy + Eq + Ord, V: Copy + Eq>(Vec<(Range<K>, V)>);
pub struct Rle<E: RLEEntry>(Vec<E>);

// impl<K: Copy + Eq + Ord + Add<Output = K> + Sub<Output = K> + AddAssign, V: Copy + Eq> RLE<K, V> {
impl<E: RLEEntry> Rle<E> {
    pub fn new() -> Self { Self(Vec::new()) }

    // Returns (found value, at offset) if found.
    pub fn find(&self, needle: RLEKey) -> Option<(E::Value, RLEKey)> {
        match self.0.binary_search_by_key(&needle, |e| e.get_key()) {
            Ok(idx) => {
                // If we find it exactly, we must be at offset 0 into the span.
                Some((*self.0[idx], 0))
            }
            Err(idx) => {
                // Check to see if the item actually exists in the previous entry.
                if idx >= 1 {
                    let e = &self.0[idx - 1];
                    let v = e.deref();
                    let offset = needle - e.get_key();
                    if offset < v.len() as RLEKey {
                        Some((*v, offset))
                    } else { None }
                } else { None }
            }
        }
    }

    pub fn append(&mut self, entry: E) {
        if let Some(last_entry) = self.0.last_mut() {
            if entry.get_key() == last_entry.get_key() + last_entry.len() as u32
                    && last_entry.can_append(&*entry) {
                last_entry.append(*entry);
                return;
            }
        }

        self.0.push(entry);
    }

    pub fn last(&self) -> Option<&E> {
        self.0.last()
    }

    pub fn num_entries(&self) -> usize { self.0.len() }

    pub fn print_stats(&self, detailed: bool) {
        println!("*** RLE");
        println!("number of {} byte entries: {}", std::mem::size_of::<E>(), self.0.len());
        println!("size: {}", self.0.capacity() * std::mem::size_of::<E>());
        println!("(efficient size: {})", self.0.len() * std::mem::size_of::<E>());
        // println!("{:?}", &self.0[..1000]);
    }
}

impl<E: EntryTraits, V: RLEEntry<Value=E>> Rle<V> {
    pub fn get(&self, idx: RLEKey) -> E::Item {
        let (v, offset) = self.find(idx).unwrap();
        v.at_offset(offset as usize)
    }
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
    use crate::universal::simple_rle::*;
    use crate::order::OrderMarker;

    #[test]
    fn rle_finds_at_offset() {
        let mut rle: Rle<RLEPair<OrderMarker>> = Rle::new();

        rle.append(RLEPair (1, OrderMarker { order: 1000, len: 2 }));
        assert_eq!(rle.find(1), Some((OrderMarker { order: 1000, len: 2 }, 0)));
        assert_eq!(rle.find(2), Some((OrderMarker { order: 1000, len: 2 }, 1)));
        assert_eq!(rle.find(3), None);

        // This should get appended.
        rle.append(RLEPair (3, OrderMarker { order: 1002, len: 1 }));
        assert_eq!(rle.find(3), Some((OrderMarker { order: 1000, len: 3 }, 2)));
        assert_eq!(rle.0.len(), 1);
    }
}
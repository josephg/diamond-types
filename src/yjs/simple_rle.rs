use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;

type RLEKey = u32;

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
// pub struct RLE<K: Copy + Eq + Ord, V: Copy + Eq>(Vec<(Range<K>, V)>);
pub struct Rle<V: SplitableSpan + Copy + Debug + Sized>(Vec<(RLEKey, V)>);

// impl<K: Copy + Eq + Ord + Add<Output = K> + Sub<Output = K> + AddAssign, V: Copy + Eq> RLE<K, V> {
impl<V: SplitableSpan + Copy + Debug + Sized> Rle<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    // Returns (found value, at offset) if found.
    pub fn find(&self, needle: RLEKey) -> Option<(V, RLEKey)> {
        match self.0.binary_search_by_key(&needle, |(base, _)| *base) {
            Ok(idx) => {
                // If we find it exactly, we must be at offset 0 into the span.
                Some((self.0[idx].1, 0))
            }
            Err(idx) => {
                // Check to see if the item actually exists in the previous entry.
                if idx >= 1 {
                    let (base, v) = &self.0[idx - 1];
                    let offset = needle - base;
                    if offset < v.len() as RLEKey {
                        Some((*v, offset))
                    } else { None }
                } else { None }
            }
        }
    }

    pub fn append(&mut self, base: RLEKey, val: V) {
        if let Some((ref last_base, ref mut v)) = self.0.last_mut() {
            if base == *last_base + v.len() as u32 && v.can_append(&val) {
                v.append(val);
                return;
            }
        }

        self.0.push((base, val));
    }

    pub fn last(&self) -> Option<&(RLEKey, V)> {
        self.0.last()
    }
}

impl<V: EntryTraits> Rle<V> {
    pub fn get(&self, idx: RLEKey) -> V::Item {
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
    use crate::yjs::simple_rle::*;
    use crate::order::OrderMarker;

    #[test]
    fn rle_finds_at_offset() {
        let mut rle: Rle<OrderMarker> = Rle::new();

        rle.append(1, OrderMarker { order: 1000, len: 2 });
        assert_eq!(rle.find(1), Some((OrderMarker { order: 1000, len: 2 }, 0)));
        assert_eq!(rle.find(2), Some((OrderMarker { order: 1000, len: 2 }, 1)));
        assert_eq!(rle.find(3), None);

        // This should get appended.
        rle.append(3, OrderMarker { order: 1002, len: 1 });
        assert_eq!(rle.find(3), Some((OrderMarker { order: 1000, len: 3 }, 2)));
        assert_eq!(rle.0.len(), 1);
    }
}
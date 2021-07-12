use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use std::cmp::Ordering::*;
use crate::rle::{RleKey, RleKeyed};

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
// pub struct RLE<K: Copy + Eq + Ord, V: Copy + Eq>(Vec<(Range<K>, V)>);
// pub struct Rle<V: SplitableSpan + Clone + Debug + Sized>(Vec<(RleKey, V)>);
pub struct Rle<V: SplitableSpan + RleKeyed + Clone + Debug + Sized>(Vec<V>);

// impl<K: Copy + Eq + Ord + Add<Output = K> + Sub<Output = K> + AddAssign, V: Copy + Eq> RLE<K, V> {
impl<V: SplitableSpan + RleKeyed + Clone + Debug + Sized> Rle<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    // Returns (found value, at offset) if found.
    pub fn find(&self, needle: RleKey) -> Option<(&V, RleKey)> {
        // TODO: This seems to still work correctly if I change Greater to Less and vice versa.
        // Make sure I'm returning the right values here!!
        self.0.binary_search_by(|entry| {
            let key = entry.get_rle_key();
            if needle < key { Greater }
            else if needle >= key + entry.len() as u32 { Less }
            else { Equal }
        }).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry, needle - entry.get_rle_key())
        })
    }

    pub fn append(&mut self, val: V) {
        if let Some(v) = self.0.last_mut() {
            if v.can_append(&val) {
                v.append(val);
                return;
            }
        }

        self.0.push(val);
    }

    pub fn last(&self) -> Option<&V> {
        self.0.last()
    }

    pub fn num_entries(&self) -> usize { self.0.len() }

    pub fn print_stats(&self, name: &str, _detailed: bool) {
        let size = std::mem::size_of::<V>();
        println!("-------- {} RLE --------", name);
        println!("number of {} byte entries: {}", size, self.0.len());
        println!("size: {}", self.0.capacity() * size);
        println!("(efficient size: {})", self.0.len() * size);

        // for item in self.0[..100].iter() {
        //     println!("{:?}", item);
        // }
    }
}

impl<V: EntryTraits + RleKeyed> Rle<V> {
    pub fn get(&self, idx: RleKey) -> V::Item {
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
    use crate::order::OrderMarker;
    use crate::rle::simple_rle::Rle;
    use crate::rle::RlePair;

    #[test]
    fn rle_finds_at_offset() {
        let mut rle: Rle<RlePair<OrderMarker>> = Rle::new();

        rle.append(RlePair(1, OrderMarker { order: 1000, len: 2 }));
        assert_eq!(rle.find(1), Some((&RlePair(1, OrderMarker { order: 1000, len: 2 }), 0)));
        assert_eq!(rle.find(2), Some((&RlePair(1, OrderMarker { order: 1000, len: 2 }), 1)));
        assert_eq!(rle.find(3), None);

        // This should get appended.
        rle.append(RlePair(3, OrderMarker { order: 1002, len: 1 }));
        assert_eq!(rle.find(3), Some((&RlePair(1, OrderMarker { order: 1000, len: 3 }), 2)));
        assert_eq!(rle.0.len(), 1);
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
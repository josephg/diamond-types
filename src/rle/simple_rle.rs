use crate::range_tree::EntryTraits;
use crate::splitable_span::SplitableSpan;
use std::fmt::Debug;
use std::cmp::Ordering::*;
use crate::rle::RLEKey;

// Each entry has a key (which we search by), a span and a value at that key.
#[derive(Clone, Eq, PartialEq, Debug)]
// pub struct RLE<K: Copy + Eq + Ord, V: Copy + Eq>(Vec<(Range<K>, V)>);
pub struct Rle<V: SplitableSpan + Copy + Debug + Sized>(Vec<(RLEKey, V)>);

// impl<K: Copy + Eq + Ord + Add<Output = K> + Sub<Output = K> + AddAssign, V: Copy + Eq> RLE<K, V> {
impl<V: SplitableSpan + Copy + Debug + Sized> Rle<V> {
    pub fn new() -> Self { Self(Vec::new()) }

    // Returns (found value, at offset) if found.
    pub fn find(&self, needle: RLEKey) -> Option<(V, RLEKey)> {
        // TODO: This seems to still work correctly if I change Greater to Less and vice versa.
        // Make sure I'm returning the right values here!!
        self.0.binary_search_by(|entry| {
            if needle < entry.0 { Greater }
            else if needle >= entry.0 + entry.1.len() as u32 { Less }
            else { Equal }
        }).ok().map(|idx| {
            let entry = &self.0[idx];
            (entry.1, needle - entry.0)
        })
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

    pub fn num_entries(&self) -> usize { self.0.len() }

    pub fn print_stats(&self, _detailed: bool) {
        let size = std::mem::size_of::<(RLEKey, V)>();
        println!("-------- RLE --------");
        println!("number of {} byte entries: {}", size, self.0.len());
        println!("size: {}", self.0.capacity() * size);
        println!("(efficient size: {})", self.0.len() * size);

        // for item in self.0[..100].iter() {
        //     println!("{:?}", item);
        // }
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
    use crate::order::OrderMarker;
    use crate::rle::simple_rle::Rle;

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
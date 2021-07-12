use super::*;
use std::ptr::NonNull;
use std::mem::take;

impl<E: EntryTraits, I: TreeIndex<E>> NodeLeaf<E, I> {
    // Note this doesn't return a Pin<Box<Self>> like the others. At the point of creation, there's
    // no reason for this object to be pinned. (Is that a bad idea? I'm not sure.)
    pub(super) unsafe fn new() -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()))
    }

    pub(super) fn new_with_parent(parent: ParentPtr<E, I>) -> Self {
        Self {
            parent,
            data: [E::default(); NUM_LEAF_ENTRIES],
            num_entries: 0,
            _pin: PhantomPinned,
            _drop: PrintDropLeaf,
        }
    }

    // pub fn find2(&self, loc: CRDTLocation) -> (ClientSeq, Option<usize>) {
    //     let mut raw_pos: ClientSeq = 0;

    //     for i in 0..NUM_ENTRIES {
    //         let entry = self.data[i];
    //         if entry.is_invalid() { break; }

    //         if entry.loc.client == loc.client && entry.get_seq_range().contains(&loc.seq) {
    //             if entry.len > 0 {
    //                 raw_pos += loc.seq - entry.loc.seq;
    //             }
    //             return (raw_pos, Some(i));
    //         } else {
    //             raw_pos += entry.get_text_len()
    //         }
    //     }
    //     (raw_pos, None)
    // }

    pub fn find(&self, loc: E::Item) -> Option<Cursor<E, I>> {
        for i in 0..self.len_entries() {
            let entry: E = self.data[i];

            if let Some(offset) = entry.contains(loc) {
                debug_assert!(offset < entry.len());
                // let offset = if entry.is_insert() { entry_offset } else { 0 };

                return Some(Cursor::new(
                    unsafe { NonNull::new_unchecked(self as *const _ as *mut _) },
                    i,
                    offset
                ))
            }
        }
        None
    }

    // Find a given text offset within the node
    // Returns (index, offset within entry)
    pub fn find_offset<F>(&self, mut offset: usize, stick_end: bool, entry_to_num: F) -> Option<(usize, usize)>
        where F: Fn(E) -> usize {
        for i in 0..self.len_entries() {
            // if offset == 0 {
            //     return Some((i, 0));
            // }

            let entry: E = self.data[i];
            if !entry.is_valid() { break; }

            // let text_len = entry.content_len();
            let entry_len = entry_to_num(entry);
            if offset < entry_len || (stick_end && entry_len == offset) {
                // Found it.
                return Some((i, offset));
            } else {
                offset -= entry_len
            }
        }

        if offset == 0 { // Special case for the first inserted element - we may never enter the loop.
            Some((self.len_entries(), 0))
        } else { None }
    }

    // pub(super) fn actually_count_entries(&self) -> usize {
    //     self.data.iter()
    //     .position(|e| e.loc.client == CLIENT_INVALID)
    //     .unwrap_or(NUM_ENTRIES)
    // }
    pub(super) fn len_entries(&self) -> usize {
        self.num_entries as usize
    }

    // Recursively (well, iteratively) ascend and update all the counts along
    // the way up. TODO: Move this - This method shouldn't be in NodeLeaf.
    pub(super) fn update_parent_count(&mut self, amt: I::IndexUpdate) {
        if amt == I::IndexUpdate::default() { return; }

        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;

        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe {
                        I::update_offset_by_marker(&mut r.as_mut().count, &amt);
                        // r.as_mut().count = r.as_ref().count.wrapping_add(amt as usize); }
                    }
                    break;
                },
                ParentPtr::Internal(mut n) => {
                    let idx = unsafe { n.as_mut() }.find_child(child).unwrap();
                    let c = &mut unsafe { n.as_mut() }.data[idx].0;
                    // :(
                    I::update_offset_by_marker(c, &amt);
                    // *c = c.wrapping_add(amt as u32);

                    // And recurse.
                    child = NodePtr::Internal(n);
                    parent = unsafe { n.as_mut() }.parent;
                },
            };
        }
    }

    pub(super) fn flush_index_update(&mut self, marker: &mut I::IndexUpdate) {
        // println!("flush {:?}", marker);
        let amt = take(marker);
        self.update_parent_count(amt);
    }

    pub(super) fn count_items(&self) -> usize {
        if I::can_count_items() {
            // Optimization using the index. TODO: check if this is actually faster.
            let offset = match self.parent {
                ParentPtr::Root(root) => {
                    unsafe { root.as_ref() }.count
                }
                ParentPtr::Internal(node) => {
                    let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self as *const _ as *mut _) });
                    let idx = unsafe { node.as_ref() }.find_child(child).unwrap();
                    unsafe { node.as_ref() }.data[idx].0
                }
            };
            I::count_items(offset)
        } else {
            // Count items the boring way. Hopefully this will optimize tightly.
            self.data[..self.num_entries as usize].iter().fold(0, |sum, elem| {
                sum + elem.len()
            })
        }
    }
}

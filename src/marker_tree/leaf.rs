use super::*;
// use std::mem;
use std::ptr::{self, NonNull};

impl<E: EntryTraits> NodeLeaf<E> {
    // Note this doesn't return a Pin<Box<Self>> like the others. At the point of creation, there's
    // no reason for this object to be pinned. (Is that a bad idea? I'm not sure.)
    pub(super) unsafe fn new() -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()))
    }

    pub(super) fn new_with_parent(parent: ParentPtr<E>) -> Self {
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

    // pub fn find(&self, loc: CRDTLocation) -> Option<Cursor<'_, E>> {
    pub fn find(&self, loc: CRDTLocation) -> Option<Cursor<E>> {
        for i in 0..self.len_entries() {
            let entry: E = self.data[i];

            if let Some(entry_offset) = entry.contains(loc) {
                let offset = if entry.is_insert() { entry_offset } else { 0 };

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
    pub fn find_offset(&self, mut offset: usize, stick_end: bool) -> Option<(usize, usize)> {
        for i in 0..self.len_entries() {
            // if offset == 0 {
            //     return Some((i, 0));
            // }

            let entry: E = self.data[i];
            if entry.is_invalid() { break; }

            let text_len = entry.content_len();
            if offset < text_len || (stick_end && text_len == offset) {
                // Found it.
                return Some((i, offset));
            } else {
                offset -= text_len
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
    pub(super) fn update_parent_count(&mut self, amt: i32) {
        if amt == 0 { return; }
        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;

        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe { r.as_mut().count = r.as_ref().count.wrapping_add(amt as usize); }
                    break;
                },
                ParentPtr::Internal(mut n) => {
                    let idx = unsafe { n.as_mut() }.find_child(child).unwrap();
                    let c = &mut unsafe { n.as_mut() }.data[idx].0;
                    // :(
                    *c = c.wrapping_add(amt as u32);

                    // And recurse.
                    child = NodePtr::Internal(n);
                    parent = unsafe { n.as_mut() }.parent;
                },
            };
        }
    }
}

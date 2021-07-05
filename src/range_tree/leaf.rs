use super::*;
use std::ptr::NonNull;
use std::mem::replace;

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
        // dbg!(&self, offset, stick_end);
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
    fn update_indexes(&mut self, idx_update: I::IndexUpdate) {
        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;

        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe {
                        // This doesn't matter for pivot updates. Its just going to store the
                        // minimum position at the root for no reason.
                        I::update_index_by(&mut r.as_mut().count, &idx_update);
                    }
                    break;
                },
                ParentPtr::Internal(mut n) => {
                    let idx = unsafe { n.as_mut() }.find_child(child).unwrap();
                    let c = &mut unsafe { n.as_mut() }.data[idx].0;
                    I::update_index_by(c, &idx_update);

                    // For pivot updates, we only propagate the update if the index is 0.
                    // Note if you wanted a duel index type thing, this approach wouldn't work.
                    if !I::needs_span_update() && idx != 0 { break; }

                    // And iterate.
                    child = NodePtr::Internal(n);
                    parent = unsafe { n.as_mut() }.parent;
                },
            };
        }
    }

    pub(super) fn flush_index_update(&mut self, marker: &mut I::IndexUpdate) {
        // println!("flush {:?}", marker);
        let amt = replace(marker, I::new_update());
        if !I::update_is_needed(&amt) { return; }
        self.update_indexes(amt);
    }
}

impl<E: EntryTraits + AbsolutelyPositioned> NodeLeaf<E, AbsPositionIndex> {
    pub fn find_at_position(&self, target_pos: usize, stick_end: bool) -> (usize, usize) {
        println!("find_at_position {} {:#?}", target_pos, self);
        // dbg!(&self, offset, stick_end);

        // Could binary search here. I don't think it makes much difference given the data fits
        // in a cache line.
        // let target_pos = target_pos as u32;
        // debug_assert!(self.num_entries >= 1);
        // debug_assert!(self.data[0].pos() <= target_pos as u32);

        // This could be implemented using iter().position(), but I think this is cleaner.
        // self.data.iter().position(|&entry| {
        //     debug_assert!(entry.is_valid());
        //
        //     let cur_pos = entry.pos() as usize;
        //     let cur_len = entry.len();
        //     cur_pos <= target_pos && target_pos < cur_len + cur_len
        // }).map(|i|
        for i in 0..self.len_entries() {
            let entry = self.data[i];
            debug_assert!(entry.is_valid());

            let cur_pos = entry.pos() as usize;
            let cur_len = entry.len();

            // Should we try to stick_end here?
            if target_pos < cur_pos { return (i, 0); }

            let offset = target_pos - cur_pos;
            if offset >= 0 && offset < cur_len {
                return (i, offset);
            } else if offset == cur_len {
                return if stick_end { (i, cur_len) }
                else { (i + 1, 0) }
            }
        }

        (self.len_entries(), 0)
    }
}

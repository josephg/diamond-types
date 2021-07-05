use super::*;
use std::mem::{self, MaybeUninit};

impl<E: EntryTraits, I: TreeIndex<E>> NodeInternal<E, I> {
    pub(super) fn new_with_parent(parent: ParentPtr<E, I>) -> Pin<Box<Self>> {
        // From the example in the docs:
        // https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        let mut children: [MaybeUninit<InternalEntry<E, I>>; NUM_NODE_CHILDREN] = unsafe {
            MaybeUninit::uninit().assume_init() // Safe because `MaybeUninit`s don't require init.
        };
        for elem in &mut children[..] {
            *elem = MaybeUninit::new((I::IndexEntry::default(), None));
        }
        Box::pin(Self {
            parent,
            // data: [(I::IndexOffset::default(), None); NUM_NODE_CHILDREN],
            data: unsafe {
                // Using transmute_copy because otherwise we run into a nonsense compiler error.
                mem::transmute_copy::<_, [InternalEntry<E, I>; NUM_NODE_CHILDREN]>(&children)
            },
            _pin: PhantomPinned,
            _drop: PrintDropInternal,
        })
    }

    /// Finds the child at some given offset. Returns the remaining offset within the found child.
    pub(super) fn find_child_at_offset<F>(&self, raw_pos: usize, stick_end: bool, offset_to_num: &F)
        -> Option<(usize, NodePtr<E, I>)>
            where F: Fn(I::IndexEntry) -> usize {

        let mut offset_remaining = raw_pos;

        for (count, elem) in self.data.iter() {
            if let Some(elem) = elem.as_ref() {
                let count = offset_to_num(*count);
                if offset_remaining < count || (stick_end && offset_remaining == count) {
                    // let elem_box = elem.unwrap();
                    return Some((offset_remaining, unsafe { elem.as_ptr() }))
                } else {
                    offset_remaining -= count;
                    // And continue.
                }
            } else { return None }
        }
        None
    }

    pub(super) fn project_data_mut(self: Pin<&mut Self>) -> &mut [InternalEntry<E, I>; NUM_NODE_CHILDREN] {
        unsafe {
            &mut self.get_unchecked_mut().data
        }
    }

    /// Insert a new item in the tree. This DOES NOT update the child counts in
    /// the parents. (So the tree will be in an invalid state after this has been called.)
    pub(super) fn splice_in(&mut self, idx: usize, count: I::IndexEntry, elem: Node<E, I>) {
        let mut buffer = (count, Some(elem));

        // TODO: Is this actually any better than the equivalent code below??
        // dbg!(idx);
        // println!("self data {:#?}", self.data);
        // Doing this with a memcpy seems better but this is buggy, and I'm not sure why.
        // let old_len = self.count_children();
        // unsafe {
        //     std::ptr::copy(&self.data[idx], &mut self.data[idx + 1], old_len - idx);
        // }
        // self.data[idx] = buffer;
        for i in idx..NUM_NODE_CHILDREN {
            mem::swap(&mut buffer, &mut self.data[i]);
            if buffer.1.is_none() { break; }
        }
        debug_assert!(buffer.1.is_none(), "tried to splice in to a node that was full");
        // println!("self data {:#?}", self.data);
    }

    pub(super) fn count_children(&self) -> usize {
        self.data.iter()
        .position(|(_, c)| c.is_none())
        .unwrap_or(NUM_NODE_CHILDREN)
    }

    /// This will panic if there aren't any children, but any node in the tree *must* have children
    /// so this should be fine in practice.
    // pub(super) fn last_child(&self) -> (I::IndexOffset, NodePtr<E, I>) {
    //     let last = &self.data[self.count_children() - 1];
    //     unsafe { (last.0, last.1.unwrap().as_ptr()) }
    // }
    pub(super) fn last_child(&self) -> NodePtr<E, I> {
        let last = &self.data[self.count_children() - 1];
        unsafe { last.1.as_ref().unwrap().as_ptr() }
    }

    pub(super) fn find_child(&self, child: NodePtr<E, I>) -> Option<usize> {
        self.data.iter()
        .position(|(_, c)| c.as_ref()
            .map_or(false, |n| n.ptr_eq(child))
        )
    }

    pub(super) unsafe fn to_parent_ptr(&self) -> ParentPtr<E, I> {
        ParentPtr::Internal(ref_to_nonnull(self))
    }
}


impl<E: EntryTraits + AbsolutelyPositioned> NodeInternal<E, AbsPositionIndex> {
    pub(super) fn find_child_at_position(&self, target_pos: usize, stick_end: bool)
                                          -> Option<NodePtr<E, AbsPositionIndex>> {

        let target_pos = target_pos as u32;
        debug_assert!(self.data[0].1.is_some());
        assert!(self.data[0].0 <= target_pos); // Could just return None here.

        for (idx, (pivot, elem)) in self.data[1..].iter().enumerate() {
            if *pivot > target_pos || elem.is_none() {
                return Some(unsafe { self.data[idx - 1].1.as_ref().unwrap().as_ptr() });
            }
        }
        None
    }
}
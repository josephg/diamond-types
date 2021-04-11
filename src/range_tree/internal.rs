use super::*;
use std::mem::{self, MaybeUninit};

impl<E: EntryTraits> NodeInternal<E> {
    pub(super) fn new_with_parent(parent: ParentPtr<E>) -> Pin<Box<Self>> {
        // From the example in the docs:
        // https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        let mut children: [MaybeUninit<(ItemCount, Option<Node<E>>)>; NUM_NODE_CHILDREN] = unsafe {
            MaybeUninit::uninit().assume_init() // Safe because `MaybeUninit`s don't require init.
        };
        for elem in &mut children[..] {
            *elem = MaybeUninit::new((0, None));
        }
        Box::pin(Self {
            parent,
            data: unsafe {
                mem::transmute::<_, [(ItemCount, Option<Node<E>>); NUM_NODE_CHILDREN]>(children)
            },
            _pin: PhantomPinned,
            _drop: PrintDropInternal,
        })
    }

    pub(super) fn get_child_ptr(&self, raw_pos: usize, stick_end: bool) -> Option<(ItemCount, NodePtr<E>)> {
        let mut offset_remaining = raw_pos as ItemCount;

        for (count, elem) in self.data.iter() {
            let count = *count;
            if let Some(elem) = elem.as_ref() {
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

    pub(super) fn project_data_mut(self: Pin<&mut Self>) -> &mut [(ItemCount, Option<Node<E>>); NUM_NODE_CHILDREN] {
        unsafe {
            &mut self.get_unchecked_mut().data
        }
    }

    /// Insert a new item in the tree. This DOES NOT update the child counts in
    /// the parents. (So the tree will be in an invalid state after this has been called.)
    pub(super) fn splice_in(&mut self, idx: usize, count: u32, elem: Node<E>) {
        let mut buffer = (count as u32, Some(elem));

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

    pub(super) fn find_child(&self, child: NodePtr<E>) -> Option<usize> {
        self.data.iter()
        .position(|(_, c)| c.as_ref()
            .map_or(false, |n| n.ptr_eq(child))
        )
    }

    pub(super) unsafe fn to_parent_ptr(&self) -> ParentPtr<E> {
        ParentPtr::Internal(ref_to_nonnull(self))
    }
}


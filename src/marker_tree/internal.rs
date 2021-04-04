use super::*;
use std::mem::{self, MaybeUninit};

impl NodeInternal {
    pub(super) fn new_with_parent(parent: ParentPtr) -> Pin<Box<Self>> {
        // From the example in the docs:
        // https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        let mut children: [MaybeUninit<(ItemCount, Option<Node>)>; MAX_CHILDREN] = unsafe {
            MaybeUninit::uninit().assume_init() // Safe because `MaybeUninit`s don't require init.
        };
        for elem in &mut children[..] {
            *elem = MaybeUninit::new((0, None));
        }
        Box::pin(Self {
            parent,
            data: unsafe {
                mem::transmute::<_, [(ItemCount, Option<Node>); MAX_CHILDREN]>(children)
            },
            _pin: PhantomPinned,
            _drop: PrintDropInternal,
        })
    }

    pub(super) fn get_child_ptr(&self, raw_pos: u32, stick_end: bool) -> Option<(u32, NodePtr)> {
        let mut offset_remaining = raw_pos;

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

    pub(super) fn project_data_mut(self: Pin<&mut Self>) -> &mut [(ItemCount, Option<Node>); MAX_CHILDREN] {
        unsafe {
            &mut self.get_unchecked_mut().data
        }
    }

    /// Insert a new item in the tree. This DOES NOT update the child counts in
    /// the parents. (So the tree will be in an invalid state after this has been called.)
    pub(super) fn splice_in(&mut self, idx: usize, count: u32, elem: Node) {
        let mut buffer = (count, Some(elem));
        for i in idx..MAX_CHILDREN {
            mem::swap(&mut buffer, &mut self.data[i]);
            if buffer.1.is_none() { break; }
        }
        debug_assert!(buffer.1.is_none(), "tried to splice in to a node that was full");
    }

    pub(super) fn count_children(&self) -> usize {
        self.data.iter()
        .position(|(_, c)| c.is_none())
        .unwrap_or(MAX_CHILDREN)
    }

    pub(super) fn find_child(&self, child: NodePtr) -> Option<usize> {
        self.data.iter()
        .position(|(_, c)| c.as_ref()
            .map_or(false, |n| n.ptr_eq(child))
        )
    }

    pub(super) unsafe fn to_parent_ptr(&self) -> ParentPtr {
        ParentPtr::Internal(ref_to_nonnull(self))
    }
}


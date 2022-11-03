use super::*;
use std::mem::{self, MaybeUninit};

impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> NodeInternal<E, I, IE, LE> {
    pub fn new_with_parent(parent: ParentPtr<E, I, IE, LE>) -> Pin<Box<Self>> {
        // From the example in the docs:
        // https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        let mut children: [MaybeUninit<Option<Node<E, I, IE, LE>>>; IE] = unsafe {
            MaybeUninit::uninit().assume_init() // Safe because `MaybeUninit`s don't require init.
        };
        for elem in &mut children[..] {
            elem.write(None);
        }
        Box::pin(Self {
            parent,
            // data: [(I::IndexOffset::default(), None); NUM_NODE_CHILDREN],
            metrics: [I::Value::default(); IE],
            children: unsafe {
                // Using transmute_copy because otherwise we run into a nonsense compiler error.
                mem::transmute_copy::<_, [Option<Node<E, I, IE, LE>>; IE]>(&children)
            },
            _pin: PhantomPinned,
        })
    }

    /// Finds the child at some given offset. Returns the remaining offset within the found child.
    /// This is a massive hotspot for the code.
    pub(crate) fn find_child_at_offset<F>(&self, raw_pos: usize, stick_end: bool, offset_to_num: &F)
                                   -> Option<(usize, NodePtr<E, I, IE, LE>)>
            where F: Fn(I::Value) -> usize {

        let mut offset_remaining = raw_pos;

        for (idx, elem) in self.children.iter().enumerate() {
            let Some(elem) = elem.as_ref() else { return None; };

            let count = offset_to_num(self.metrics[idx]);
            if offset_remaining < count || (stick_end && offset_remaining == count) {
                // let elem_box = elem.unwrap();
                return Some((offset_remaining, unsafe { elem.as_ptr() }))
            } else {
                offset_remaining -= count;
                // And continue.
            }
        }
        None
    }

    pub fn set_entry(self: Pin<&mut Self>, idx: usize, count: I::Value, child: Option<Node<E, I, IE, LE>>) {
        unsafe {
            let ptr = self.get_unchecked_mut();
            ptr.metrics[idx] = count;
            ptr.children[idx] = child;
        }
    }

    // pub(super) fn project_data_mut(self: Pin<&mut Self>) -> &mut [InternalEntry<E, I, IE, LE>; NUM_NODE_CHILDREN] {
    //     unsafe {
    //         &mut self.get_unchecked_mut().data
    //     }
    // }

    /// Insert a new item in the tree. This DOES NOT update the child counts in
    /// the parents. (So the tree will be in an invalid state after this has been called.)
    pub fn splice_in(&mut self, idx: usize, mut count: I::Value, elem: Node<E, I, IE, LE>) {
        // let mut buffer = (count, Some(elem));
        let mut elem_buffer = Some(elem);

        // TODO: Is this actually any better than the equivalent code below??
        // dbg!(idx);
        // println!("self data {:#?}", self.data);
        // Doing this with a memcpy seems better but this is buggy, and I'm not sure why.
        // let old_len = self.count_children();
        // unsafe {
        //     std::ptr::copy(&self.data[idx], &mut self.data[idx + 1], old_len - idx);
        // }
        // self.data[idx] = buffer;
        for i in idx..IE {
            mem::swap(&mut count, &mut self.metrics[i]);
            mem::swap(&mut elem_buffer, &mut self.children[i]);
            if elem_buffer.is_none() { break; }
        }
        debug_assert!(elem_buffer.is_none(), "tried to splice in to a node that was full");
        // println!("self data {:#?}", self.data);
    }

    pub fn count_children(&self) -> usize {
        self.children.iter()
        .position(|c| c.is_none())
        .unwrap_or(IE)
    }

    /// This will panic if there aren't any children, but any node in the tree *must* have children
    /// so this should be fine in practice.
    // pub(super) fn last_child(&self) -> (I::IndexOffset, NodePtr<E, I, IE, LE>) {
    //     let last = &self.data[self.count_children() - 1];
    //     unsafe { (last.0, last.1.unwrap().as_ptr()) }
    // }
    pub fn last_child(&self) -> NodePtr<E, I, IE, LE> {
        let last = &self.children[self.count_children() - 1];
        unsafe { last.as_ref().unwrap().as_ptr() }
    }

    pub fn find_child(&self, child: NodePtr<E, I, IE, LE>) -> Option<usize> {
        self.children.iter()
        .position(|c| c.as_ref()
            .map_or(false, |n| n.ptr_eq(child))
        )
    }

    pub unsafe fn to_parent_ptr(&self) -> ParentPtr<E, I, IE, LE> {
        ParentPtr::Internal(ref_to_nonnull(self))
    }
}


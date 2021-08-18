// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// use std::cell::Cell;
use std::fmt::Debug;
use std::marker;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

pub use entry::*;
pub use index::*;
pub use root::DeleteResult;

pub use crate::range_tree::index::TreeIndex;

// The common data structures are:
mod cursor;
mod root;
mod leaf;
mod internal;
mod entry;
mod mutations;
mod index;

#[cfg(test)]
mod fuzzer;

// pub(crate) use cursor::Cursor;

#[cfg(debug_assertions)]
const NUM_NODE_CHILDREN: usize = 8; // This needs to be minimum 8.
#[cfg(not(debug_assertions))]
const NUM_NODE_CHILDREN: usize = 16;


// Must fit in u8, and must be >= 4 due to limitations in splice_insert.
#[cfg(debug_assertions)]
const NUM_LEAF_ENTRIES: usize = 4;
#[cfg(not(debug_assertions))]
const NUM_LEAF_ENTRIES: usize = 32;


// This is the root of the tree. There's a bit of double-deref going on when you
// access the first node in the tree, but I can't think of a clean way around
// it.
#[derive(Debug)]
pub struct RangeTree<E: EntryTraits, I: TreeIndex<E>> {
    // count: usize,
    count: I::IndexValue,
    root: Node<E, I>,

    // Usually inserts and deletes are followed by more inserts / deletes at the same location.
    // We cache the last cursor position so we can reuse cursors between edits.
    // TODO: Currently unused.
    // last_cursor: Cell<Option<(usize, Cursor<E, I>)>>,

    _pin: marker::PhantomPinned,
}

// The warning here is an error - the bound can't be removed.
// #[allow(type_alias_bounds)]
// type InternalEntry<E, I: TreeIndex<E>> = (I::IndexOffset, Option<Node<E, I>>);

/// An internal node in the B-tree
#[derive(Debug)]
struct NodeInternal<E: EntryTraits, I: TreeIndex<E>> {
    parent: ParentPtr<E, I>,
    // Pairs of (count of subtree elements, subtree contents).
    // Left packed. The nodes are all the same type.
    // ItemCount only includes items which haven't been deleted.
    index: [I::IndexValue; NUM_NODE_CHILDREN],
    children: [Option<Node<E, I>>; NUM_NODE_CHILDREN],
    _pin: PhantomPinned, // Needed because children have parent pointers here.
    _drop: PrintDropInternal,
}

/// A leaf node in the B-tree. Except the root, each child stores MAX_CHILDREN/2 - MAX_CHILDREN
/// entries.
#[derive(Debug)]
pub struct NodeLeaf<E: EntryTraits, I: TreeIndex<E>> {
    parent: ParentPtr<E, I>,
    num_entries: u8, // Number of entries which have been populated
    data: [E; NUM_LEAF_ENTRIES],
    _pin: PhantomPinned, // Needed because cursors point here.
    _drop: PrintDropLeaf
}

#[derive(Debug)]
enum Node<E: EntryTraits, I: TreeIndex<E>> {
    Internal(Pin<Box<NodeInternal<E, I>>>),
    Leaf(Pin<Box<NodeLeaf<E, I>>>),
}

// I hate that I need this, but its used all over the place when traversing the tree.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum NodePtr<E: EntryTraits, I: TreeIndex<E>> {
    Internal(NonNull<NodeInternal<E, I>>),
    Leaf(NonNull<NodeLeaf<E, I>>),
}

// TODO: Consider just reusing NodePtr for this.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ParentPtr<E: EntryTraits, I: TreeIndex<E>> {
    Root(NonNull<RangeTree<E, I>>),
    Internal(NonNull<NodeInternal<E, I>>)
}

#[derive(Clone, Debug, PartialEq, Eq)]
// pub struct Cursor<'a, E: EntryTraits> {
pub struct Cursor<E: EntryTraits, I: TreeIndex<E>> {
// pub struct Cursor {
    node: NonNull<NodeLeaf<E, I>>,
    pub idx: usize,
    pub offset: usize, // This doesn't need to be usize, but the memory size of Cursor doesn't matter.
    // _marker: marker::PhantomData<&'a MarkerTree<E>>,
}

// impl<E: EntryTraits> Iterator for Cursor<'_, E> {
impl<E: EntryTraits, I: TreeIndex<E>> Iterator for Cursor<E, I> {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        // When the cursor is past the end, idx is an invalid value.
        if self.idx == usize::MAX {
            return None;
        }

        // The cursor is at the end of the current element. Its a bit dirty doing this twice but
        // This will happen for a fresh cursor in an empty document, or when iterating using a
        // cursor made by some other means.
        if self.idx >= unsafe { self.node.as_ref() }.len_entries() {
            let has_next = self.next_entry();
            if !has_next {
                self.idx = usize::MAX;
                return None;
            }
        }

        let current = self.get_raw_entry();
        // Move the cursor forward preemptively for the next call to next().
        let has_next = self.next_entry();
        if !has_next {
            self.idx = usize::MAX;
        }
        Some(current)
    }
}

/// Iterator for all the items inside the entries. Unlike entry iteration we use the offset here.
#[derive(Debug)]
pub struct ItemIterator<E: EntryTraits, I: TreeIndex<E>>(Cursor<E, I>);

impl<E: EntryTraits, I: TreeIndex<E>> Iterator for ItemIterator<E, I> {
    type Item = E::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // I'll set idx to an invalid value
        if self.0.idx == usize::MAX {
            None
        } else {
            let entry = self.0.get_raw_entry();
            let len = entry.len();
            let item = entry.at_offset(self.0.offset);
            self.0.offset += 1;

            if self.0.offset >= len {
                // Skip to the next entry for the next query.
                let has_next = self.0.next_entry();
                if !has_next {
                    // We're done.
                    self.0.idx = usize::MAX;
                }
            }
            Some(item)
        }
    }
}


#[derive(Clone, Debug)]
struct PrintDropLeaf;

// For debugging.

// impl Drop for PrintDropLeaf {
//     fn drop(&mut self) {
//         eprintln!("DROP LEAF {:?}", self);
//     }
// }

#[derive(Clone, Debug)]
struct PrintDropInternal;

// impl Drop for PrintDropInternal {
//     fn drop(&mut self) {
//         eprintln!("DROP INTERNAL {:?}", self);
//     }
// }

// unsafe fn pinbox_to_nonnull<T>(box_ref: &Pin<Box<T>>) -> NonNull<T> {
//     NonNull::new_unchecked(box_ref.as_ref().get_ref() as *const _ as *mut _)
// }

/// Unsafe because NonNull wraps a mutable pointer. Callers must take care of mutability!
unsafe fn ref_to_nonnull<T>(val: &T) -> NonNull<T> {
    NonNull::new_unchecked(val as *const _ as *mut _)
}

/// Helper when a notify function is not needed
pub fn null_notify<E, Node>(_e: E, _node: Node) {}

impl<E: EntryTraits, I: TreeIndex<E>> Node<E, I> {
    /// Unsafe: Created leaf has a dangling parent pointer. Must be set after initialization.
    unsafe fn new_leaf() -> Self {
        Node::Leaf(Box::pin(NodeLeaf::new()))
    }
    // fn new_with_parent(parent: ParentPtr) -> Self {
    //     Node::Leaf(Box::pin(NodeLeaf::new_with_parent(parent)))
    // }

    fn set_parent(&mut self, parent: ParentPtr<E, I>) {
        unsafe {
            match self {
                Node::Leaf(l) => l.as_mut().get_unchecked_mut().parent = parent,
                Node::Internal(i) => i.as_mut().get_unchecked_mut().parent = parent,
            }
        }
    }

    // pub fn get_parent(&self) -> ParentPtr {
    //     match self {
    //         Node::Leaf(l) => l.parent,
    //         Node::Internal(i) => i.parent,
    //     }
    // }

    pub(super) fn is_leaf(&self) -> bool {
        match self {
            Node::Leaf(_) => true,
            Node::Internal(_) => false,
        }
    }

    fn unwrap_leaf(&self) -> &NodeLeaf<E, I> {
        match self {
            Node::Leaf(l) => l.as_ref().get_ref(),
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }

    fn unwrap_into_leaf(self) -> Pin<Box<NodeLeaf<E, I>>> {
        match self {
            Node::Leaf(l) => l,
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }

    fn unwrap_leaf_mut(&mut self) -> Pin<&mut NodeLeaf<E, I>> {
        match self {
            Node::Leaf(l) => l.as_mut(),
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    // fn unwrap_internal(&self) -> &NodeInternal {
    //     match self {
    //         Node::Internal(n) => n.as_ref().get_ref(),
    //         Node::Leaf(_) => panic!("Expected internal node"),
    //     }
    // }
    fn unwrap_internal_mut(&mut self) -> Pin<&mut NodeInternal<E, I>> {
        match self {
            Node::Internal(n) => n.as_mut(),
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }

    /// Unsafe: The resulting NodePtr is mutable and doesn't have an associated lifetime.
    unsafe fn as_ptr(&self) -> NodePtr<E, I> {
        match self {
            Node::Internal(n) => {
                NodePtr::Internal(ref_to_nonnull(n.as_ref().get_ref()))
            },
            Node::Leaf(n) => {
                NodePtr::Leaf(ref_to_nonnull(n.as_ref().get_ref()))
            },
        }
    }

    fn ptr_eq(&self, ptr: NodePtr<E, I>) -> bool {
        match (self, ptr) {
            (Node::Internal(n), NodePtr::Internal(ptr)) => {
                std::ptr::eq(n.as_ref().get_ref(), ptr.as_ptr())
            },
            (Node::Leaf(n), NodePtr::Leaf(ptr)) => {
                std::ptr::eq(n.as_ref().get_ref(), ptr.as_ptr())
            },
            _ => panic!("Pointer type does not match")
        }
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> NodePtr<E, I> {
    fn unwrap_leaf(self) -> NonNull<NodeLeaf<E, I>> {
        match self {
            NodePtr::Leaf(l) => l,
            NodePtr::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }

    unsafe fn get_parent(&self) -> ParentPtr<E, I> {
        match self {
            NodePtr::Internal(n) => { n.as_ref().parent }
            NodePtr::Leaf(n) => { n.as_ref().parent }
        }
    }
}

impl<E: EntryTraits, I: TreeIndex<E>> ParentPtr<E, I> {
    fn unwrap_internal(self) -> NonNull<NodeInternal<E, I>> {
        match self {
            ParentPtr::Root(_) => { panic!("Expected internal node"); }
            ParentPtr::Internal(ptr) => { ptr }
        }
    }

    fn is_root(&self) -> bool {
        match self {
            ParentPtr::Root(_) => { true }
            ParentPtr::Internal(_) => { false }
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem::size_of;
    use crate::range_tree::*;
    use crate::order::OrderSpan;
    // use std::pin::Pin;

    #[test]
    fn option_node_size_is_transparent() {
        let node_size = size_of::<Node<OrderSpan, RawPositionIndex>>();
        let opt_node_size = size_of::<Option<Node<OrderSpan, RawPositionIndex>>>();
        assert_eq!(node_size, opt_node_size);

        // TODO: This fails, which means we're burning 8 bytes to simply store tags for each
        // pointer in a node. Despite all the items inside a node being the same type.
        // let item_size = size_of::<Pin<Box<NodeInternal<OrderSpan, RawPositionIndex>>>>();
        // assert_eq!(node_size, item_size);
    }
}
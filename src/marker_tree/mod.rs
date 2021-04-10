// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// The common data structures are:
mod cursor;
mod root;
mod leaf;
mod internal;
mod entry;
mod mutations;

// pub(crate) use cursor::Cursor;

use std::ptr::NonNull;
use std::marker;
use std::pin::Pin;

use super::common::*;
use std::marker::PhantomPinned;

pub use root::DeleteResult;
use std::fmt::Debug;
use crate::marker_tree::entry::EntryTraits;
pub use entry::Entry;
use std::cell::Cell;

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
pub struct MarkerTree<E: EntryTraits> {
    count: usize,
    root: Node<E>,

    // Usually inserts and deletes are followed by more inserts / deletes at the same location.
    // We cache the last cursor position so we can reuse cursors between edits.
    // TODO: Currently unused.
    last_cursor: Cell<Option<(usize, Cursor<E>)>>,

    _pin: marker::PhantomPinned,
}

#[derive(Debug)]
enum Node<E: EntryTraits> {
    Internal(Pin<Box<NodeInternal<E>>>),
    Leaf(Pin<Box<NodeLeaf<E>>>),
}

// I hate that I need this, but its used all over the place when traversing the tree.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum NodePtr<E: EntryTraits> {
    Internal(NonNull<NodeInternal<E>>),
    Leaf(NonNull<NodeLeaf<E>>),
}

// TODO: Consider just reusing NodePtr for this.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ParentPtr<E: EntryTraits> {
    Root(NonNull<MarkerTree<E>>),
    Internal(NonNull<NodeInternal<E>>)
}

/// An internal node in the B-tree
#[derive(Debug)]
struct NodeInternal<E: EntryTraits> {
    parent: ParentPtr<E>,
    // Pairs of (count of subtree elements, subtree contents).
    // Left packed. The nodes are all the same type.
    // ItemCount only includes items which haven't been deleted.
    data: [(ItemCount, Option<Node<E>>); NUM_NODE_CHILDREN],
    _pin: PhantomPinned, // Needed because children have parent pointers here.
    _drop: PrintDropInternal,
}

/// A leaf node in the B-tree. Except the root, each child stores MAX_CHILDREN/2 - MAX_CHILDREN
/// entries.
#[derive(Debug)]
pub struct NodeLeaf<E: EntryTraits> {
    parent: ParentPtr<E>,
    num_entries: u8, // Number of entries which have been populated
    // data: [Entry; NUM_ENTRIES],
    data: [E; NUM_LEAF_ENTRIES],
    _pin: PhantomPinned, // Needed because cursors point here.
    _drop: PrintDropLeaf
}

#[derive(Copy, Clone, Debug)]
// pub struct Cursor<'a, E: EntryTraits> {
pub struct Cursor<E: EntryTraits> {
// pub struct Cursor {
    node: NonNull<NodeLeaf<E>>,
    idx: usize,
    offset: usize, // This doesn't need to be usize, but the memory size of Cursor doesn't matter.
    // _marker: marker::PhantomData<&'a MarkerTree<E>>,
}

/// Helper struct to track pending size changes in the document which need to be propagated
#[derive(Debug)]
pub struct FlushMarker(isize);

impl Drop for FlushMarker {
    fn drop(&mut self) {
        if self.0 != 0 {
            if !std::thread::panicking() {
                panic!("Flush marker dropped without being flushed");
            }
        }
    }
}

impl FlushMarker {
    // TODO: This should take a Pin<> or be unsafe or something. This is unsound because we could
    // move node.
    fn flush<E: EntryTraits>(&mut self, node: &mut NodeLeaf<E>) {
        // println!("Flush marker flushing {}", self.0);
        node.update_parent_count(self.0 as i32);
        self.0 = 0;
    }

    fn flush_opt<E: EntryTraits>(opt: &mut Option<&mut Self>, node: &mut NodeLeaf<E>) {
        if let Some(marker) = opt {
            marker.flush(node);
        }
    }
}


// impl<E: EntryTraits> Iterator for Cursor<'_, E> {
impl<E: EntryTraits> Iterator for Cursor<E> {
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        // I'll set idx to an invalid value
        if self.idx == usize::MAX {
            None
        } else {
            let current = self.get_entry();
            let has_next = self.next_entry();
            if !has_next {
                self.idx = usize::MAX;
            }
            Some(current)
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



impl<E: EntryTraits> Node<E> {
    /// Unsafe: Created leaf has a dangling parent pointer. Must be set after initialization.
    unsafe fn new_leaf() -> Self {
        Node::Leaf(Box::pin(NodeLeaf::new()))
    }
    // fn new_with_parent(parent: ParentPtr) -> Self {
    //     Node::Leaf(Box::pin(NodeLeaf::new_with_parent(parent)))
    // }

    fn set_parent(&mut self, parent: ParentPtr<E>) {
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

    fn unwrap_leaf(&self) -> &NodeLeaf<E> {
        match self {
            Node::Leaf(l) => l.as_ref().get_ref(),
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_leaf_mut(&mut self) -> Pin<&mut NodeLeaf<E>> {
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
    fn unwrap_internal_mut(&mut self) -> Pin<&mut NodeInternal<E>> {
        match self {
            Node::Internal(n) => n.as_mut(),
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }

    /// Unsafe: The resulting NodePtr is mutable and doesn't have an associated lifetime.
    unsafe fn as_ptr(&self) -> NodePtr<E> {
        match self {
            Node::Internal(n) => {
                NodePtr::Internal(ref_to_nonnull(n.as_ref().get_ref()))
            },
            Node::Leaf(n) => {
                NodePtr::Leaf(ref_to_nonnull(n.as_ref().get_ref()))
            },
        }
    }

    fn ptr_eq(&self, ptr: NodePtr<E>) -> bool {
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

impl<E: EntryTraits> NodePtr<E> {
    fn unwrap_leaf(self) -> NonNull<NodeLeaf<E>> {
        match self {
            NodePtr::Leaf(l) => l,
            NodePtr::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
}
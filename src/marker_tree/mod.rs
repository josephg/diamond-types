// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// The btree implementation here is based off ropey
// (https://github.com/cessen/ropey/) since that has pretty good performance in
// most cases.

// The common data structures are 

mod cursor;
mod root;
mod leaf;
mod internal;

// pub(crate) use cursor::Cursor;

use std::ops::Range;
use std::ptr::NonNull;
use std::marker;
use std::pin::Pin;

use super::common::*;
use std::marker::PhantomPinned;

pub use root::DeleteResult;

#[cfg(debug_assertions)]
const MAX_CHILDREN: usize = 8; // This needs to be minimum 8.
#[cfg(not(debug_assertions))]
const MAX_CHILDREN: usize = 16;


// Must fit in u8.
#[cfg(debug_assertions)]
const NUM_ENTRIES: usize = 4;
#[cfg(not(debug_assertions))]
const NUM_ENTRIES: usize = 32;


// This is the root of the tree. There's a bit of double-deref going on when you
// access the first node in the tree, but I can't think of a clean way around
// it.
#[derive(Debug)]
pub struct MarkerTree {
    count: ItemCount,
    root: Node,
    _pin: marker::PhantomPinned,
}

#[derive(Debug)]
enum Node {
    Internal(Pin<Box<NodeInternal>>),
    Leaf(Pin<Box<NodeLeaf>>),
}

// I hate that I need this, but its used all over the place when traversing the tree.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum NodePtr {
    Internal(NonNull<NodeInternal>),
    Leaf(NonNull<NodeLeaf>),
}

// TODO: Consider just reusing NodePtr for this.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ParentPtr {
    Root(NonNull<MarkerTree>),
    Internal(NonNull<NodeInternal>)
}

/// An internal node in the B-tree
#[derive(Debug)]
struct NodeInternal /*<T: NodeT>*/ {
    parent: ParentPtr,
    // Pairs of (count of subtree elements, subtree contents).
    // Left packed. The nodes are all the same type.
    // ItemCount only includes items which haven't been deleted.
    data: [(ItemCount, Option<Node>); MAX_CHILDREN],
    _pin: PhantomPinned, // Needed because children have parent pointers here.
    _drop: PrintDropInternal,
}

/// A leaf node in the B-tree. Except the root, each child stores MAX_CHILDREN/2 - MAX_CHILDREN
/// entries.
#[derive(Debug)]
pub struct NodeLeaf {
    parent: ParentPtr,
    len: u8, // Number of entries which have been populated
    data: [Entry; NUM_ENTRIES],
    _pin: PhantomPinned, // Needed because cursors point here.
    _drop: PrintDropLeaf
}

#[derive(Debug, Copy, Clone, Default)]
struct Entry {
    loc: CRDTLocation,
    len: i32, // negative if the chunk was deleted. Never 0 - TODO: could use NonZeroI32
}


#[derive(Copy, Clone, Debug)]
pub struct Cursor<'a> { // TODO: Add this lifetime parameter back.
// pub struct Cursor {
    node: NonNull<NodeLeaf>,
    idx: usize,
    offset: u32, // usize? ??. This is the offset into the item at idx.
    _marker: marker::PhantomData<&'a MarkerTree>,
}

/// Helper struct to track pending size changes in the document which need to be propagated
#[derive(Debug)]
pub struct FlushMarker(i32);

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
    fn flush(&mut self, node: &mut NodeLeaf) {
        // println!("Flush marker flushing {}", self.0);
        node.update_parent_count(self.0);
        self.0 = 0;
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

impl Entry {
    fn get_seq_range(self) -> Range<ClientSeq> {
        self.loc.seq .. self.loc.seq + (self.len.abs() as ClientSeq)
    }

    fn get_content_len(&self) -> u32 {
        if self.len < 0 { 0 } else { self.len as u32 }
    }

    fn get_seq_len(&self) -> u32 {
        self.len.abs() as u32
    }

    fn trim_keeping_start(&mut self, cut_at: u32) {
        self.len = if self.len < 0 { -(cut_at as i32) } else { cut_at as i32 };
    }

    fn trim_keeping_end(&mut self, cut_at: u32) {
        self.loc.seq += cut_at;
        self.len += if self.len < 0 { cut_at as i32 } else { -(cut_at as i32) };
    }

    // Confusingly CLIENT_INVALID is used both for empty entries and the root entry. But the root
    // entry will never be a valid entry in the marker tree, so it doesn't matter.
    fn is_invalid(&self) -> bool {
        self.loc.agent == CLIENT_INVALID
    }

    fn is_insert(&self) -> bool {
        debug_assert!(self.len != 0);
        self.len > 0
    }

    fn is_delete(&self) -> bool {
        !self.is_insert()
    }
}


impl Node {
    /// Unsafe: Created leaf has a dangling parent pointer. Must be set after initialization.
    unsafe fn new_leaf() -> Self {
        Node::Leaf(Box::pin(NodeLeaf::new()))
    }
    // fn new_with_parent(parent: ParentPtr) -> Self {
    //     Node::Leaf(Box::pin(NodeLeaf::new_with_parent(parent)))
    // }

    fn set_parent(&mut self, parent: ParentPtr) {
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

    fn unwrap_leaf(&self) -> &NodeLeaf {
        match self {
            Node::Leaf(l) => l.as_ref().get_ref(),
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_leaf_mut(&mut self) -> Pin<&mut NodeLeaf> {
        match self {
            Node::Leaf(l) => l.as_mut(),
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_internal(&self) -> &NodeInternal {
        match self {
            Node::Internal(n) => n.as_ref().get_ref(),
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }
    fn unwrap_internal_mut(&mut self) -> Pin<&mut NodeInternal> {
        match self {
            Node::Internal(n) => n.as_mut(),
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }

    /// Unsafe: The resulting NodePtr is mutable and doesn't have an associated lifetime.
    unsafe fn as_ptr(&self) -> NodePtr {
        match self {
            Node::Internal(n) => {
                NodePtr::Internal(ref_to_nonnull(n.as_ref().get_ref()))
            },
            Node::Leaf(n) => {
                NodePtr::Leaf(ref_to_nonnull(n.as_ref().get_ref()))
            },
        }
    }

    fn ptr_eq(&self, ptr: NodePtr) -> bool {
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

impl NodePtr {
    fn unwrap_leaf(self) -> NonNull<NodeLeaf> {
        match self {
            NodePtr::Leaf(l) => l,
            NodePtr::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
}
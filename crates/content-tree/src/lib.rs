// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// use std::cell::Cell;
use std::fmt::Debug;
use std::marker;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

pub use index::*;
use rle::splitable_span::SplitableSpan;
pub use root::DeleteResult;

// The common data structures are:
mod unsafe_cursor;
mod root;
mod leaf;
mod internal;
mod mutations;
mod index;
mod safe_cursor;
pub mod testrange;

// pub(crate) use cursor::Cursor;

#[cfg(debug_assertions)]
pub const DEFAULT_IE: usize = 8; // This needs to be minimum 8.
#[cfg(not(debug_assertions))]
pub const DEFAULT_IE: usize = 10;

// Must fit in u8, and must be >= 4 due to limitations in splice_insert.
#[cfg(debug_assertions)]
pub const DEFAULT_LE: usize = 4;
#[cfg(not(debug_assertions))]
pub const DEFAULT_LE: usize = 32;

/// Simple empty helper trait naming all the properties needed by ContentTree.
pub trait ContentTraits: SplitableSpan + Copy + Debug + Default {}
impl<T: SplitableSpan + Copy + Debug + Default> ContentTraits for T {}

/// A ContentTree is an efficient packed list of RLE entries, allowing for arbitrary inserts and
/// deletes anywhere in the range.
///
/// ```rust
/// use content_tree::ContentTree;
/// use content_tree::testrange::TestRange;
///
/// let mut tree = ContentTree::new();
/// tree.push(TestRange { id: 0, len: 100, is_activated: true });
/// tree.push(TestRange { id: 100, len: 50, is_activated: true });
///
/// assert_eq!(tree.raw_iter().collect::<Vec<TestRange>>(), vec![
///     TestRange { id: 0, len: 150, is_activated: true }
/// ]);
/// ```
#[derive(Debug)]
pub struct ContentTreeRaw<E: ContentTraits, I: TreeIndex<E>, const INT_ENTRIES: usize, const LEAF_ENTRIES: usize> {
    count: I::IndexValue,

    // There's a bit of double-deref going on when you access the first node in the tree, but I
    // can't think of a clean way around it.
    root: Node<E, I, INT_ENTRIES, LEAF_ENTRIES>,

    // Usually inserts and deletes are followed by more inserts / deletes at the same location.
    // We cache the last cursor position so we can reuse cursors between edits.
    // TODO: Currently unused.
    // last_cursor: Cell<Option<(usize, Cursor<E, I, IE, LE>)>>,

    _pin: marker::PhantomPinned,
}

/// An alias for ContentTreeRaw which uses the default sizes for internal elements.
pub type ContentTreeWithIndex<E, I> = ContentTreeRaw<E, I, DEFAULT_IE, DEFAULT_LE>;
pub type ContentTree<E> = ContentTreeRaw<E, RawPositionIndex, DEFAULT_IE, DEFAULT_LE>;

/// An internal node in the B-tree
#[derive(Debug)]
pub(crate) struct NodeInternal<E: ContentTraits, I: TreeIndex<E>, const INT_ENTRIES: usize, const LEAF_ENTRIES: usize> {
    parent: ParentPtr<E, I, INT_ENTRIES, LEAF_ENTRIES>,
    // Pairs of (count of subtree elements, subtree contents).
    // Left packed. The nodes are all the same type.
    // ItemCount only includes items which haven't been deleted.
    index: [I::IndexValue; INT_ENTRIES],
    children: [Option<Node<E, I, INT_ENTRIES, LEAF_ENTRIES>>; INT_ENTRIES],
    _pin: PhantomPinned, // Needed because children have parent pointers here.
}

/// A leaf node in the B-tree. Except the root, each child stores MAX_CHILDREN/2 - MAX_CHILDREN
/// entries.
///
/// The details in a leaf node are private. This is exposed to allow consumers to build custom
/// indexes, allowing for fast cursor creation without needing to iterate through the tree.
///
/// See diamond-types for an example of this.
#[derive(Debug)]
pub struct NodeLeaf<E: ContentTraits, I: TreeIndex<E>, const INT_ENTRIES: usize, const LEAF_ENTRIES: usize> {
    parent: ParentPtr<E, I, INT_ENTRIES, LEAF_ENTRIES>,
    num_entries: u8, // Number of entries which have been populated
    data: [E; LEAF_ENTRIES],
    _pin: PhantomPinned, // Needed because cursors point here.

    next: Option<NonNull<Self>>,
}

#[derive(Debug)]
pub(crate) enum Node<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    Internal(Pin<Box<NodeInternal<E, I, IE, LE>>>),
    Leaf(Pin<Box<NodeLeaf<E, I, IE, LE>>>),
}

// I hate that I need this, but its used all over the place when traversing the tree.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum NodePtr<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    Internal(NonNull<NodeInternal<E, I, IE, LE>>),
    Leaf(NonNull<NodeLeaf<E, I, IE, LE>>),
}

// TODO: Consider just reusing NodePtr for this.
#[derive(Copy, Clone, Debug, Eq)]
pub(crate) enum ParentPtr<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    Root(NonNull<ContentTreeRaw<E, I, IE, LE>>),
    Internal(NonNull<NodeInternal<E, I, IE, LE>>)
}

/// A cursor into some location in a range tree.
///
/// Note the state of a cursor is weird in two situations:
/// - When a cursor points to a location in between two entries, the cursor could either point to
/// the end of the first entry or the start of the subsequent entry.
/// - When a tree is empty, the cursor points past the end of the tree.
///
/// Safety: This is unsafe because there's no associated lifetime on a cursor (its 'static).
///
/// The caller must ensure any reads and mutations through an UnsafeCursor are valid WRT the
/// mutability and lifetime of the implicitly referenced content tree. Use Cursor and MutCursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsafeCursor<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    node: NonNull<NodeLeaf<E, I, IE, LE>>,
    idx: usize,
    pub offset: usize, // This doesn't need to be usize, but the memory size of Cursor doesn't matter.
}

/// A cursor into an immutable ContentTree. A cursor is the primary way to read entries in the
/// content tree. A cursor points to a specific offset at a specific entry in a specific node in
/// the content tree.
#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Cursor<'a, E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    inner: UnsafeCursor<E, I, IE, LE>,
    marker: marker::PhantomData<&'a ContentTreeRaw<E, I, IE, LE>>,
}

/// A mutable cursor into a ContentTree. Mutable cursors inherit all the functionality of Cursor,
/// and can also be used to modify the content tree.
///
/// A mutable cursor mutably borrows the content tree. Only one mutable cursor can exist at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct MutCursor<'a, E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> {
    // TODO: Remove pub(crate).
    pub inner: UnsafeCursor<E, I, IE, LE>,
    marker: marker::PhantomData<&'a mut ContentTreeRaw<E, I, IE, LE>>,
}

// I can't use the derive() implementation of this because EntryTraits does not always implement
// PartialEq.
impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> PartialEq for ParentPtr<E, I, IE, LE> {
    fn eq(&self, other: &Self) -> bool {
        use ParentPtr::*;
        match (self, other) {
            (Root(a), Root(b)) => a == b,
            (Internal(a), Internal(b)) => a == b,
            _ => false
        }
    }
}

/// Unsafe because NonNull wraps a mutable pointer. Callers must take care of mutability!
unsafe fn ref_to_nonnull<T>(val: &T) -> NonNull<T> {
    NonNull::new_unchecked(val as *const _ as *mut _)
}

/// Helper when a notify function is not needed
pub fn null_notify<E, Node>(_e: E, _node: Node) {}

impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Node<E, I, IE, LE> {
    /// Unsafe: Created leaf has a dangling parent pointer. Must be set after initialization.
    // unsafe fn new_leaf() -> Self {
    //     Node::Leaf(Box::pin(NodeLeaf::new()))
    // }
    // fn new_with_parent(parent: ParentPtr) -> Self {
    //     Node::Leaf(Box::pin(NodeLeaf::new_with_parent(parent)))
    // }

    fn set_parent(&mut self, parent: ParentPtr<E, I, IE, LE>) {
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

    pub fn is_leaf(&self) -> bool {
        match self {
            Node::Leaf(_) => true,
            Node::Internal(_) => false,
        }
    }

    // fn unwrap_leaf(&self) -> &NodeLeaf<E, I, IE, LE> {
    //     match self {
    //         Node::Leaf(l) => l.as_ref().get_ref(),
    //         Node::Internal(_) => panic!("Expected leaf - found internal node"),
    //     }
    // }
    //
    // fn unwrap_into_leaf(self) -> Pin<Box<NodeLeaf<E, I, IE, LE>>> {
    //     match self {
    //         Node::Leaf(l) => l,
    //         Node::Internal(_) => panic!("Expected leaf - found internal node"),
    //     }
    // }

    fn unwrap_leaf_mut(&mut self) -> Pin<&mut NodeLeaf<E, I, IE, LE>> {
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
    fn unwrap_internal_mut(&mut self) -> Pin<&mut NodeInternal<E, I, IE, LE>> {
        match self {
            Node::Internal(n) => n.as_mut(),
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }

    /// Unsafe: The resulting NodePtr is mutable and doesn't have an associated lifetime.
    unsafe fn as_ptr(&self) -> NodePtr<E, I, IE, LE> {
        match self {
            Node::Internal(n) => {
                NodePtr::Internal(ref_to_nonnull(n.as_ref().get_ref()))
            },
            Node::Leaf(n) => {
                NodePtr::Leaf(ref_to_nonnull(n.as_ref().get_ref()))
            },
        }
    }

    fn ptr_eq(&self, ptr: NodePtr<E, I, IE, LE>) -> bool {
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

impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> NodePtr<E, I, IE, LE> {
    fn unwrap_leaf(self) -> NonNull<NodeLeaf<E, I, IE, LE>> {
        match self {
            NodePtr::Leaf(l) => l,
            NodePtr::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }

    unsafe fn get_parent(&self) -> ParentPtr<E, I, IE, LE> {
        match self {
            NodePtr::Internal(n) => { n.as_ref().parent }
            NodePtr::Leaf(n) => { n.as_ref().parent }
        }
    }
}

impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> ParentPtr<E, I, IE, LE> {
    fn unwrap_internal(self) -> NonNull<NodeInternal<E, I, IE, LE>> {
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
    use super::*;
    use crate::testrange::TestRange;

// use std::pin::Pin;

    #[test]
    fn option_node_size_is_transparent() {
        let node_size = size_of::<Node<TestRange, RawPositionIndex, DEFAULT_IE, DEFAULT_LE>>();
        let opt_node_size = size_of::<Option<Node<TestRange, RawPositionIndex, DEFAULT_IE, DEFAULT_LE>>>();
        assert_eq!(node_size, opt_node_size);

        // TODO: This fails, which means we're burning 8 bytes to simply store tags for each
        // pointer in a node. Despite all the items inside a node being the same type.
        // let item_size = size_of::<Pin<Box<NodeInternal<OrderSpan, RawPositionIndex>>>>();
        // assert_eq!(node_size, item_size);
    }
}

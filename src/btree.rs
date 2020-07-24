// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// The btree implementation here is based off ropey
// (https://github.com/cessen/ropey/) since that has pretty good performance in
// most cases.
#[allow(unused_variables)]

use std::ptr;
use std::ptr::{NonNull, copy, copy_nonoverlapping};
use std::ops::Range;
use std::marker;
use std::mem;
use std::mem::MaybeUninit;
use std::pin::Pin;
use super::common::*;

const MAX_CHILDREN: usize = 8; // This needs to be minimum 8.
const MIN_CHILDREN: usize = MAX_CHILDREN / 2;

const NUM_ENTRIES: usize = 4;

// More correct to use usize here but this will be fine in practice and faster.
type CharCount = u32;


// This is the root of the tree. There's a bit of double-deref going on when you
// access the first node in the tree, but I can't think of a clean way around
// it.
#[derive(Debug)]
pub struct MarkerTree {
    count: CharCount,
    // This is only ever None when the tree is being destroyed.
    root: Pin<Box<Node>>,
    // root: Option<Pin<Box<Node>>>,
    _pin: marker::PhantomPinned,
}

#[derive(Debug)]
enum Node {
    Internal(NodeInternal),
    Leaf(NodeLeaf),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ParentPtr {
    Root(NonNull<MarkerTree>),
    Internal(NonNull<NodeInternal>)
}

// Ugh I hate that I need this.
#[derive(Copy, Clone, Debug)]
enum NodePtr {
    Internal(NonNull<NodeInternal>),
    Leaf(NonNull<NodeLeaf>),
}

// trait NodeT: std::fmt::Debug {}
// impl<T> NodeT for NodeInternal<T> {}
// impl NodeT for NodeLeaf {}

#[derive(Debug)]
struct NodeInternal /*<T: NodeT>*/ {
    parent: ParentPtr,
    // Pairs of (count of subtree elements, subtree contents).
    // Left packed. The nodes are all the same type.
    // data: [(CharCount, Option<Box<Node>>); MAX_CHILDREN]
    data: [(CharCount, Option<Pin<Box<Node>>>); MAX_CHILDREN]
}

#[derive(Debug)]
pub struct NodeLeaf {
    parent: ParentPtr,
    data: [Entry; NUM_ENTRIES],
}

// struct NodeInternal {
//     children: [Box<Node>; MAX_CHILDREN],
// }

#[derive(Debug, Copy, Clone, Default)]
struct Entry {
    loc: CRDTLocation,
    len: i32, // negative if the chunk was deleted.
}

impl Entry {
    fn get_seq_range(self) -> Range<ClientSeq> {
        self.loc.seq .. self.loc.seq + (self.len.abs() as ClientSeq)
    }

    fn get_text_len(&self) -> u32 {
        if self.len < 0 { 0 } else { self.len as u32 }
    }

    fn get_seq_len(&self) -> u32 {
        self.len.abs() as u32
    }

    // These two methods would be cleaner if I wrote a split() function or something.
    fn keep_start(&mut self, cut_at: u32) {
        self.len = if self.len < 0 { -(cut_at as i32) } else { cut_at as i32 };
    }

    fn keep_end(&mut self, cut_at: u32) {
        self.loc.seq += cut_at;
        self.len += if self.len < 0 { cut_at as i32 } else { -(cut_at as i32) };
    }

    fn is_invalid(&self) -> bool {
        self.loc.client == CLIENT_INVALID
    }
}

impl NodeLeaf {
    // fn new() -> MaybeUninit<Self> {
    //     let leaf: MaybeUninit<Self> = MaybeUninit::uninit();
    //     unsafe { (*leaf.as_mut_ptr()).data = [INVALID_ENTRY; NUM_ENTRIES]; }
    //     leaf
    // }

    // unsafe fn bake_parent(leaf: MaybeUninit<Self>, parent: ParentPtr) -> Self {
    //     (*leaf.as_mut_ptr()).parent = parent;
    //     leaf.assume_init()
    // }

    unsafe fn new() -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()))
    }

    fn new_with_parent(parent: ParentPtr) -> Self {
        Self {
            parent,
            data: [Entry::default(); NUM_ENTRIES]
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

    pub fn find(&self, loc: CRDTLocation) -> Option<Cursor> {
        for i in 0..NUM_ENTRIES {
            let entry = self.data[i];
            if entry.is_invalid() { break; }

            if entry.loc.client == loc.client && entry.get_seq_range().contains(&loc.seq) {
                let offset = if entry.len > 0 {
                    loc.seq - entry.loc.seq
                } else { 0 };

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
    pub fn find_offset(&self, mut offset: u32, stick_end: bool) -> Option<(usize, u32)> {
        for i in 0..NUM_ENTRIES {
            if offset == 0 { // Need to specialcase this for the first inserted element.
                return Some((i, 0));
            }

            let entry = self.data[i];
            if entry.loc.client == CLIENT_INVALID { break; }

            let text_len = entry.get_text_len();
            if offset < text_len || (stick_end && text_len == offset) {
                // Found it.
                return Some((i, offset));
            } else {
                offset -= text_len
            }
        }
        None
    }

    fn count_entries(&self) -> usize {
        self.data.iter()
        .position(|e| e.loc.client == CLIENT_INVALID)
        .unwrap_or(NUM_ENTRIES)
    }

    // Recursively (well, iteratively) ascend and update all the counts along
    // the way up.
    fn update_parent_count(&mut self, amt: i32) {
        let mut child = NodePtr::Leaf(unsafe { NonNull::new_unchecked(self) });
        let mut parent = self.parent;
        loop {
            match parent {
                ParentPtr::Root(mut r) => {
                    unsafe { r.as_mut().count = r.as_ref().count.wrapping_add(amt as u32); }
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

    fn split_at<F>(&mut self, idx: usize, self_entries: usize, notify: &mut F) -> NonNull<NodeLeaf>
        where F: FnMut(CRDTLocation, ClientSeq, NonNull<NodeLeaf>)
    {
        unsafe {
            let mut new_node = Self::new(); // The new node has a danging parent pointer
            copy_nonoverlapping(&self.data[idx], &mut new_node.data[0], self_entries - idx);
            
            // "zero" out the old entries
            let mut stolen_length = 0;
            for e in &mut self.data[idx..NUM_ENTRIES] {
                if !e.is_invalid() {
                    stolen_length += e.get_text_len();
                    *e = Entry::default();
                }
            }

            eprintln!("split_at idx {} self_entries {} stolel_len {} self {:?}", idx, self_entries, stolen_length, &self);

            let mut inserted_node = Box::pin(Node::Leaf(new_node));
            // Ultimately ret is the pointer to the new item we'll end up returning.
            let new_leaf_ptr = NonNull::new_unchecked(inserted_node.unwrap_leaf_mut());

            for e in &inserted_node.unwrap_leaf().data[0..self_entries-idx] {
                notify(e.loc, e.get_seq_len(), new_leaf_ptr);
            }

            // Ok now we need to walk up the tree trying to insert. At each step
            // we will try and insert inserted_node into parent next to old_node
            // (topping out at the head).
            let mut old_node: NodePtr = NodePtr::Leaf(NonNull::new_unchecked(self));
            let mut parent = &mut self.parent;
            loop {
                // First try and simply emplace in the new element in the parent.
                if let ParentPtr::Internal(n) = parent {
                    let parent_ref = n.as_ref();
                    let count = parent_ref.count_children();
                    if count < MAX_CHILDREN {
                        // Great. Insert the new node into the parent and
                        // return.
                        *(inserted_node.get_parent_mut()) = ParentPtr::Internal(*n);
                        
                        let old_idx = parent_ref.find_child(old_node).unwrap();
                        let new_idx = old_idx + 1;

                        let parent_ref = n.as_mut();
                        parent_ref.data[old_idx].0 -= stolen_length;
                        parent_ref.splice_in(new_idx, stolen_length, inserted_node);

                        eprintln!("1");
                        return new_leaf_ptr;
                    }
                }

                // Ok so if we've gotten here we need to make a new internal
                // node filled with inserted_node, then move and all the goodies
                // from ParentPtr.
                match parent {
                    ParentPtr::Root(r) => {
                        // This is the simpler case. The new root will be a new
                        // internal node containing old_node and inserted_node.
                        let new_root = Box::pin(Node::Internal(NodeInternal::new_with_parent(ParentPtr::Root(*r))));
                        let mut old_root = mem::replace(&mut r.as_mut().root, new_root);
                        
                        // *(inserted_node.get_parent_mut()) = parent_ptr;
                        
                        let count = r.as_ref().count;
                        let new_root_ref = r.as_mut().root.unwrap_internal_mut();
                        let parent_ptr = ParentPtr::Internal(NonNull::new_unchecked(new_root_ref));
                        
                        // Reassign parents for each node
                        *(inserted_node.get_parent_mut()) = parent_ptr;
                        *(old_root.get_parent_mut()) = parent_ptr;
                        
                        new_root_ref.data[0] = (count - stolen_length, Some(old_root));
                        new_root_ref.data[1] = (stolen_length, Some(inserted_node));

                        eprintln!("2");
                        return new_leaf_ptr;
                    },
                    ParentPtr::Internal(n) => {
                        // And this is the complex case. We have MAX_CHILDREN+1
                        // items (in some order) to distribute between two
                        // internal nodes (one old, one new). Then we iterate up
                        // the tree.
                        let old_parent_ref = n.as_ref();
                        debug_assert!(old_parent_ref.count_children() == MAX_CHILDREN);

                        let mut new_parent = NodeInternal::new();
                        let old_idx = old_parent_ref.find_child(old_node).unwrap();
                        
                        let old_parent_ref = n.as_mut();
                        old_parent_ref.data[old_idx].0 -= stolen_length;
                        // Dividing this into cases makes it easier to reason
                        // about.
                        if old_idx < MAX_CHILDREN/2 {
                            // Move all items from MAX_CHILDREN/2..MAX_CHILDREN
                            // into new_parent, then splice inserted_node into
                            // old_parent.
                            for i in 0..MAX_CHILDREN/2 {
                                let element = mem::replace(&mut old_parent_ref.data[i + MAX_CHILDREN/2], (0, None));
                                new_parent.data[i] = element;
                            }

                            let new_idx = old_idx + 1;
                            old_parent_ref.splice_in(new_idx, stolen_length, inserted_node);
                        } else {
                            // The new element is in the second half of the
                            // group.
                            let new_idx = old_idx - MAX_CHILDREN/2 + 1;

                            let mut dest = 0;
                            let mut new_entry = (stolen_length, Some(inserted_node));
                            for src in MAX_CHILDREN/2..MAX_CHILDREN {
                                if dest == new_idx {
                                    new_parent.data[dest] = mem::take(&mut new_entry);
                                    dest += 1;
                                }

                                let element = mem::replace(&mut old_parent_ref.data[src], (0, None));
                                new_parent.data[dest] = element;

                                dest += 1;
                            }
                        }

                        old_node = NodePtr::Internal(*n);
                        inserted_node = Box::pin(Node::Internal(new_parent));
                        // And iterate up the tree.
                    },
                };
            }
        }
    }
}

impl NodeInternal {
    unsafe fn new() -> Self {
        Self::new_with_parent(ParentPtr::Root(NonNull::dangling()))
    }

    fn new_with_parent(parent: ParentPtr) -> Self {
        // From the example in the docs:
        // https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        let mut children: [MaybeUninit<(CharCount, Option<Pin<Box<Node>>>)>; MAX_CHILDREN] = unsafe {
            MaybeUninit::uninit().assume_init()
        };
        for elem in &mut children[..] {
            *elem = MaybeUninit::new((0, None));
        }
        Self {
            parent,
            data: unsafe {
                mem::transmute::<_, [(CharCount, Option<Pin<Box<Node>>>); MAX_CHILDREN]>(children)
            },
        }
    }

    fn get_child(&self, raw_pos: u32) -> Option<(u32, Pin<&Node>)> {
        let mut offset_remaining = raw_pos;

        self.data.iter().find_map(|(count, elem)| {
            if let Some(elem) = elem.as_ref() {
                if offset_remaining < *count {
                    // let elem_box = elem.unwrap();
                    Some((offset_remaining, elem.as_ref()))
                } else {
                    offset_remaining -= *count;
                    None
                }
            } else { None }
        })
    }

    fn get_child_mut(&mut self, raw_pos: u32) -> Option<(u32, Pin<&mut Node>)> {
        let mut offset_remaining = raw_pos;

        self.data.iter_mut().find_map(|(count, elem)| {
            if let Some(elem) = elem.as_mut() {
                if offset_remaining < *count {
                    Some((offset_remaining, elem.as_mut()))
                } else {
                    offset_remaining -= *count;
                    None
                }
            } else { None }
        })
    }

    fn splice_in(&mut self, idx: usize, count: u32, elem: Pin<Box<Node>>) {
        let mut buffer = (count, Some(elem));
        for i in idx..MAX_CHILDREN {
            mem::swap(&mut buffer, &mut self.data[i]);
            if buffer.1.is_none() { break; }
        }
        debug_assert!(buffer.1.is_none(), "tried to splice in to a node that was full");
    }

    fn count_children(&self) -> usize {
        self.data.iter()
        .position(|(_, c)| c.is_none())
        .unwrap_or(MAX_CHILDREN)
    }

    fn find_child(&self, child: NodePtr) -> Option<usize> {
        self.data.iter()
        .position(|(_, c)| c.as_ref()
            .map_or(false, |n| n.ptr_eq(child))
        )
    }

    // fn find_child(&self, child: &Node) -> Option<usize> {
    //     let mut pos = 0;
    //     self.data.iter().find_map(|(count, elem)| {
    //         if let Some(elem) = elem.as_ref() {
    //             if ptr::eq(elem.as_ref(), child) { Some(pos) } else { None }
    //         } else {
    //             None
    //         }
    //     })
    // }
}

#[derive(Copy, Clone, Debug)]
pub struct Cursor<'a> {
    node: NonNull<NodeLeaf>,
    idx: usize,
    offset: u32, // usize? ??. This is the offset into the item at idx.
    _marker: marker::PhantomData<&'a Node>,
}

impl<'a> Cursor<'a> {
    fn new(node: NonNull<NodeLeaf>, idx: usize, offset: u32) -> Self {
        Cursor {
            node, idx, offset, _marker: marker::PhantomData
        }
    }

    fn next_node(&mut self) -> Option<NodeLeaf> {
        unimplemented!();
    }
    
    fn prev_node(&mut self) -> Option<NodeLeaf> {
        unimplemented!();
    }

    // Move back to the previous entry. Returns true if it exists, otherwise
    // returns false if we're at the start of the doc already.
    fn prev_entry(&mut self) -> bool {
        if self.idx > 0 {
            self.idx -= 1;
            self.offset = self.get_entry().len as u32;
            true
        } else {
            // idx is 0. Go up as far as we can until we get to an index thats
            // not 0, or we hit the root.
            let node = unsafe { self.node.as_ref() };

            let mut parent = node.parent;
            let mut node_ptr = NodePtr::Leaf(self.node);
            loop {
                match parent {
                    ParentPtr::Root(_) => { return false; },
                    ParentPtr::Internal(n) => {
                        let node_ref = unsafe { n.as_ref() };
                        // Ok, find the previous child.
                        let idx = node_ref.find_child(node_ptr).unwrap();
                        // node_ptr = NodePtr::Internal(n);
                        if idx > 0 {
                            // Whew - now we can descend down from here.
                            node_ptr = pinnode_to_nodeptr(node_ref.data[idx - 1].1.as_ref().unwrap());
                            break;
                        } else {
                            // idx is 0. Keep climbing up the ladder.
                            node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                            parent = node_ref.parent;
                        }
                    }
                }
            }

            // Now back down. We just use node_ptr - idx is irrelevant now
            // because we can just take the last item each time.
            loop {
                match node_ptr {
                    NodePtr::Internal(n) => {
                        let node_ref = unsafe { n.as_ref() };
                        let num_children = node_ref.count_children();
                        assert!(num_children > 0);
                        node_ptr = pinnode_to_nodeptr(node_ref.data[num_children - 1].1.as_ref().unwrap());
                    },
                    NodePtr::Leaf(n) => {
                        // Finally.
                        let node_ref = unsafe { n.as_ref() };
                        self.idx = node_ref.count_entries();
                        self.offset = node_ref.data[self.idx].get_seq_len();
                        return true;
                    }
                }
            }
        }
    }

    fn get_pos(&self) -> u32 {
        let node = unsafe { self.node.as_ref() };
        
        let mut pos: u32 = 0;
        // First find out where we are in the current node.
        
        // TODO: This is a bit redundant - we could find out the local position
        // when we scan initially to initialize the cursor.
        for e in &node.data[0..self.idx] {
            pos += e.get_text_len();
        }
        let local_len = node.data[self.idx].len;
        if local_len > 0 { pos += self.offset; }

        // Ok, now iterate up to the root counting offsets as we go.

        let mut parent = node.parent;
        let mut node_ptr = NodePtr::Leaf(self.node);
        loop {
            match parent {
                ParentPtr::Root(_) => { break; }, // done.

                ParentPtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    let idx = node_ref.find_child(node_ptr).unwrap();

                    for (c, _) in &node_ref.data[0..idx] {
                        pos += c;
                    }

                    node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                    parent = node_ref.parent;
                }
            }
        }

        pos
    }

    fn get_entry(&self) -> &Entry {
        let node = unsafe { self.node.as_ref() };
        &node.data[self.idx]
    }
    
    pub fn tell(mut self) -> CRDTLocation {
        while self.idx == 0 || self.get_entry().len < 0 {
            let exists = self.prev_entry();
            if !exists { return CRDT_DOC_ROOT; }
        }

        let entry = self.get_entry(); // Shame this is called twice but eh.
        CRDTLocation {
            client: entry.loc.client,
            seq: entry.loc.seq + self.offset
        }
    }
}

fn pinbox_to_nonnull<T>(box_ref: &mut Pin<Box<T>>) -> NonNull<T> {
    unsafe {
        NonNull::new_unchecked(box_ref.as_mut().get_unchecked_mut())
    }
}

fn pinnode_to_nodeptr(box_ref: &Pin<Box<Node>>) -> NodePtr {
    let node_ref = box_ref.as_ref().get_ref();
    match node_ref {
        Node::Internal(n) => NodePtr::Internal(unsafe { NonNull::new_unchecked(n as *const _ as *mut _) }),
        Node::Leaf(n) => NodePtr::Leaf(unsafe { NonNull::new_unchecked(n as *const _ as *mut _) }),
    }
}

impl MarkerTree {
    pub fn new() -> Pin<Box<Self>> {
        let mut tree = Box::pin(unsafe { Self {
            count: 0,
            root: Box::pin(Node::new()),
            _pin: marker::PhantomPinned,
        } });

        unsafe {
            let ptr = tree.as_mut().get_unchecked_mut();
            *ptr.root.get_parent_mut() = ParentPtr::Root(NonNull::new_unchecked(ptr));
        }

        tree
    }

    pub fn cursor_at_pos<'a>(self: &'a Pin<Box<Self>>, raw_pos: u32, stick_end: bool) -> Cursor<'a> {
        // let mut node: *const Node = &*self.root.as_ref().unwrap().as_ref();
        let mut node: *const Node = &*self.root.as_ref();
        let mut offset_remaining = raw_pos;
        unsafe {
            while let Node::Internal(data) = &*node {
                let (offset, next) = data.get_child(offset_remaining).expect("Internal consistency violation");
                offset_remaining -= offset;
                node = next.get_ref();
            };

            let node = (*node).unwrap_leaf();
            let (idx, offset_remaining) = node.find_offset(offset_remaining, stick_end)
            .expect("Element does not contain entry");

            Cursor {
                node: NonNull::new_unchecked(node as *const _ as *mut _),
                idx,
                offset: offset_remaining,
                _marker: marker::PhantomData
            }
        }
    }

    // Make room at the current cursor location, splitting the current element
    // if necessary (and recursively splitting the btree node if there's no
    // room). The gap will be filled with junk and must be immediately
    // overwritten. (The location of the gap is returned via the cursor.)
    unsafe fn make_space_in_leaf<F>(cursor: &mut Cursor, gap: usize, notify: &mut F)
        where F: FnMut(CRDTLocation, ClientSeq, NonNull<NodeLeaf>)
    {
        let node = cursor.node.as_mut();
        
        {
            // let mut entry = &mut node.0[cursor.idx];
            // let seq_len = entry.get_seq_len();
            let seq_len = node.data[cursor.idx].get_seq_len();

            // If we're at the end of the current entry, skip it.
            if cursor.offset == seq_len {
                cursor.offset = 0;
                cursor.idx += 1;
                // entry = &mut node.0[cursor.idx];
            }
        }
        
        let space_needed = if cursor.offset > 0 {
            // We'll need an extra space to split the node.
            gap + 1
        } else {
            gap
        };

        if space_needed == 0 { return; } // 🤷‍♀️

        let filled_entries = node.count_entries();
        if filled_entries + space_needed > NUM_ENTRIES {
            // Split the entry in two. space_needed should always be 1 or 2, and
            // there needs to be room after splitting.
            debug_assert!(space_needed == 1 || space_needed == 2);
            debug_assert!(space_needed <= NUM_ENTRIES/2); // unnecessary but simplifies things.
            
            // By conventional btree rules, we should make sure each side of the
            // split has at least n/2 elements but in this case I don't think it
            // really matters. I'll do something reasonable that is clean and clear.
            if cursor.idx < NUM_ENTRIES/2 {
                // Put the new items at the end of the current node and
                // move everything afterward to a new node.
                let split_point = if cursor.offset == 0 { cursor.idx } else { cursor.idx + 1 };
                node.split_at(split_point, filled_entries, notify);
            } else {
                // Split in the middle of the current node. This involves a
                // little unnecessary copying - because we're copying the
                // elements into the new node then we'll split (and copy them
                // again) below but its ok for now. Memcpy is fast.

                // The other option here would be to use the index as a split
                // point and add padding into the new node to leave space.
                cursor.node = node.split_at(NUM_ENTRIES/2, filled_entries, notify);
                cursor.idx -= NUM_ENTRIES/2;
            }

            // unimplemented!("split");
        }

        let node = cursor.node.as_mut();

        // There's room in the node itself now. We need to reshuffle.
        let src_idx = cursor.idx;
        let dest_idx = src_idx + space_needed;

        copy(&node.data[src_idx], &mut node.data[dest_idx], filled_entries - src_idx);
        
        // Tidy up the edges
        node.data[dest_idx].keep_end(cursor.offset);
        
        if cursor.offset > 0 {
            node.data[src_idx].keep_start(cursor.offset);
            cursor.idx += 1;
            cursor.offset = 0;
        }
    }

    /**
     * Insert a new CRDT insert / delete at some raw position in the document
     */
    pub fn insert<F>(self: &Pin<Box<Self>>, mut cursor: Cursor, len: ClientSeq, new_loc: CRDTLocation, mut notify: F)
        where F: FnMut(CRDTLocation, ClientSeq, NonNull<NodeLeaf>)
    {
        if cfg!(debug_assertions) {
            self.as_ref().get_ref().check();
        }

        // First walk down the tree to find the location.
        // let mut node = self;

        // let mut cursor = self.cursor_at_pos(raw_pos, true);
        unsafe {
            // Insert has 3 cases:
            // - 1. The entry can be extended. We can do this inline.
            // - 2. The inserted text is at the end an entry, but the entry cannot
            //   be extended. We need to add 1 new entry to the leaf.
            // - 3. The inserted text is in the middle of an entry. We need to
            //   split the entry and insert a new entry in the middle. We need
            //   to add 2 new entries.

            let old_entry = &mut cursor.node.as_mut().data[cursor.idx];

            // We also want case 2 if the node is brand new...
            if cursor.idx == 0 && old_entry.loc.client == CLIENT_INVALID {
                *old_entry = Entry {
                    loc: new_loc,
                    len: len as i32,
                };
                cursor.node.as_mut().update_parent_count(len as i32);
                notify(new_loc, len, cursor.node);
            } else if old_entry.len > 0 && old_entry.len as u32 == cursor.offset
                    && old_entry.loc.client == new_loc.client
                    && old_entry.loc.seq + old_entry.len as u32 == new_loc.seq {
                // Case 1 - extend the entry.
                old_entry.len += len as i32;
                cursor.node.as_mut().update_parent_count(len as i32);
                notify(new_loc, len, cursor.node);
            } else {
                // Case 2 and 3.
                Self::make_space_in_leaf(&mut cursor, 1, &mut notify);
                cursor.node.as_mut().data[cursor.idx] = Entry {
                    loc: new_loc,
                    len: len as i32
                };
                // eprintln!("3 update_parent_count {} {:?}", len, &self);
                cursor.node.as_mut().update_parent_count(len as i32);
                // eprintln!("3 ->date_parent_count {} {:?}", len, &self);
                notify(new_loc, len, cursor.node);
            }
        }

        if cfg!(debug_assertions) {
            self.as_ref().get_ref().check();
        }
    }

    pub fn delete(&mut self, raw_pos: u32) {
        unimplemented!("delete");
    }



    // Returns size.
    fn check_leaf(leaf: &NodeLeaf, expected_parent: ParentPtr) -> usize {
        assert_eq!(leaf.parent, expected_parent);
        
        let mut count: usize = 0;
        let mut done = false;

        for e in &leaf.data {
            if e.is_invalid() {
                done = true;
            } else {
                // Make sure there's no data after an invalid entry
                assert!(done == false);
                count += e.get_text_len() as usize;
            }
        }

        // An empty leaf is only valid if we're the root element.
        if let ParentPtr::Internal(_) = leaf.parent {
            assert!(count > 0);
        }

        count
    }
    
    // Returns size.
    fn check_internal(node: &NodeInternal, expected_parent: ParentPtr) -> usize {
        assert_eq!(node.parent, expected_parent);
        
        let mut count_total: usize = 0;
        let mut done = false;
        let mut child_type = None; // Make sure all the children have the same type.
        let self_parent = ParentPtr::Internal(NonNull::new(node as *const _ as *mut _).unwrap());

        for (child_count_expected, child) in &node.data {
            if let Some(child) = child {
                // Make sure there's no data after an invalid entry
                assert!(done == false);

                let child_ref = child.as_ref().get_ref();

                let actual_type = match child_ref {
                    Node::Internal(n) => 1,
                    Node::Leaf(n) => 2
                };
                // Make sure all children have the same type.
                if child_type.is_none() { child_type = Some(actual_type) }
                else { assert_eq!(child_type, Some(actual_type)); }

                // Recurse
                let count_actual = match child_ref {
                    Node::Leaf(n) => { Self::check_leaf(n, self_parent) },
                    Node::Internal(n) => { Self::check_internal(n, self_parent) },
                };

                // Make sure all the individual counts match.
                assert_eq!(*child_count_expected as usize, count_actual);
                count_total += count_actual;
            } else {
                done = true;
            }
        }

        count_total
    }

    pub fn check(&self) {
        // Check the parent of each node is its correct parent
        // Check the size of each node is correct up and down the tree
        let root = self.root.as_ref().get_ref();
        let expected_parent = ParentPtr::Root(NonNull::new(self as *const _ as *mut Self).unwrap());
        let expected_size = match root {
            Node::Internal(n) => { Self::check_internal(&n, expected_parent) },
            Node::Leaf(n) => { Self::check_leaf(&n, expected_parent) },
        };
        assert_eq!(self.count as usize, expected_size);
    }

    pub unsafe fn lookup_position(loc: CRDTLocation, ptr: NonNull<NodeLeaf>) -> u32 {
        // First make a cursor to the specified item
        let leaf = ptr.as_ref();
        // let mut parent = leaf.parent;
        // enum NodePtr {
        //     Internal(NonNull<NodeInternal>),
        //     Leaf(NonNull<NodeLeaf>),
        // }
        // let mut node = NodePtr::Leaf(ptr);

        // First find the entry
        // let (mut pos, idx) = leaf.find2(loc);
        // idx.expect("Internal consistency violation - could not find leaf");

        // let cursor = Cursor::new(ptr, idx, pos);
        let cursor = leaf.find(loc).expect("Position not in named leaf");

        cursor.get_pos()
    }

    // unsafe fn lookup_position(loc: CRDTLocation, ptr: NonNull<NodeLeaf>) -> usize {
    //     let leaf = ptr.as_ref();
    //     let mut parent = leaf.parent;
    //     // enum NodePtr {
    //     //     Internal(NonNull<NodeInternal>),
    //     //     Leaf(NonNull<NodeLeaf>),
    //     // }
    //     // let mut node = NodePtr::Leaf(ptr);

    //     // First find the entry
    //     let (mut pos, idx) = leaf.find2(loc);
    //     idx.expect("Internal consistency violation - could not find leaf");
        
    //     // Ok now ascend up the tree.
    //     loop {
    //         // let parent = match node {
    //         //     NodePtr::Internal(n) => n.as_ref().parent,
    //         //     NodePtr::Leaf(n) => n.as_ref().parent,
    //         // };

    //         // let parent = match parent {
    //         //     ParentPtr::Internal(ptr) => ptr.as_ref(),
    //         //     ParentPtr::Root(_) => break // Hit the root.
    //         // };
            
    //         // Scan the node to count the length.
    //         for i in 0..MAX_CHILDREN {
    //             let (count, elem) = &parent.data[i];
                
    //             if let Some(elem) = elem {
    //                 if std::ptr::eq(elem.as_ref(), node) {
    //                     // Found the child.
    //                     break;
    //                 } else {
    //                     pos += count;
    //                 }
    //             } else {
    //                 panic!("Could not find child in parent");
    //             }
    //         }

    //         // Scan the internal 

    //         node = parent;
    //     }

    //     pos as usize
    // }
}

impl Node {
    pub unsafe fn new() -> Self {
        Node::Leaf(NodeLeaf::new())
    }
    pub unsafe fn new_with_parent(parent: ParentPtr) -> Self {
        Node::Leaf(NodeLeaf::new_with_parent(parent))
    }

    fn get_parent_mut(&mut self) -> &mut ParentPtr {
        match self {
            Node::Leaf(l) => &mut l.parent,
            Node::Internal(i) => &mut i.parent,
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
            Node::Leaf(l) => l,
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_leaf_mut(&mut self) -> &mut NodeLeaf {
        match self {
            Node::Leaf(l) => l,
            Node::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_internal(&self) -> &NodeInternal {
        match self {
            Node::Internal(n) => n,
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }
    fn unwrap_internal_mut(&mut self) -> &mut NodeInternal {
        match self {
            Node::Internal(n) => n,
            Node::Leaf(_) => panic!("Expected internal node"),
        }
    }

    fn ptr_eq(&self, ptr: NodePtr) -> bool {
        match (self, ptr) {
            (Node::Internal(n), NodePtr::Internal(ptr)) => std::ptr::eq(n, ptr.as_ptr()),
            (Node::Leaf(n), NodePtr::Leaf(ptr)) => std::ptr::eq(n, ptr.as_ptr()),
            _ => panic!("Pointer type does not match")
        }
    }
}

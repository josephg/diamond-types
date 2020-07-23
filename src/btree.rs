// The btree here is used to map character -> document positions. It could also
// be extended to inline a rope, but I haven't done that here.

// The btree implementation here is based off ropey
// (https://github.com/cessen/ropey/) since that has pretty good performance in
// most cases.
#[allow(unused_variables)]

use std::ptr::{NonNull, copy, copy_nonoverlapping};
use std::ops::Range;
use std::marker;
use super::common::*;

const MAX_CHILDREN: usize = 4;
const MIN_CHILDREN: usize = MAX_CHILDREN / 2;

const NUM_ENTRIES: usize = 10;

// More correct to use usize here but this will be fine in practice and faster.
type CharCount = u32;

#[derive(Debug)]
pub struct MarkerTree {
    root: Box<Node>
}

#[derive(Debug)]
pub struct Node {
    parent: Option<NonNull<Node>>, // Null at the root.
    data: NodeData,
}

#[derive(Debug)]
enum NodeData {
    Internal(NodeInternal),
    Leaf(NodeLeaf),
}

#[derive(Debug)]
// Pairs of (count of subtree elements, subtree contents).
// Left packed.
struct NodeInternal([(CharCount, Option<Box<Node>>); MAX_CHILDREN]);

// struct NodeInternal {
//     children: [Box<Node>; MAX_CHILDREN],
// }

#[derive(Debug, Copy, Clone)]
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
}

#[derive(Debug, Copy, Clone)]
pub struct NodeLeaf([Entry; NUM_ENTRIES]);

impl NodeLeaf {
    fn new() -> NodeLeaf {
        Self([INVALID_ENTRY; NUM_ENTRIES])
    }

    pub fn find2(&self, loc: CRDTLocation) -> (ClientSeq, Option<usize>) {
        let mut raw_pos: ClientSeq = 0;

        for i in 0..NUM_ENTRIES {
            let entry = self.0[i];
            if entry.loc.client == CLIENT_INVALID { break; }

            if entry.loc.client == loc.client && entry.get_seq_range().contains(&loc.seq) {
                if entry.len > 0 {
                    raw_pos += loc.seq - entry.loc.seq;
                }
                return (raw_pos, Some(i));
            } else {
                raw_pos += entry.get_text_len()
            }
        }
        (raw_pos, None)
    }

    // Find a given text offset within the node
    // Returns (index, offset within entry)
    pub fn find_offset(&self, mut offset: u32, stick_end: bool) -> Option<(usize, u32)> {
        for i in 0..NUM_ENTRIES {
            if offset == 0 { // Need to specialcase this for the first inserted element.
                return Some((i, 0));
            }

            let entry = self.0[i];
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
        self.0.iter()
        .position(|e| e.loc.client == CLIENT_INVALID)
        .unwrap_or(NUM_ENTRIES)
    }

    fn split_at<F>(&mut self, idx: usize, entries: usize, notify: F) -> NonNull<NodeLeaf>
        where F: FnMut(CRDTLocation, NonNull<NodeLeaf>)
    {
        // I think this might end up copying the new element a bunch :/
        let mut new_node = Self::new();

        unsafe {
            copy_nonoverlapping(&self.0[idx], &mut new_node.0[0], entries - idx);
        }

        // let parent = sel

        // for i in idx..entries {
        //     notify(
        // }

        unimplemented!()
    }
}

const INVALID_ENTRY: Entry = Entry {
    loc: CRDTLocation { client: CLIENT_INVALID, seq: 0 },
    len: 0
};

impl NodeInternal {
    fn get_child(&self, raw_pos: u32) -> Option<(u32, &Node)> {
        let mut offset_remaining = raw_pos;

        self.0.iter().find_map(|(count, elem)| {
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

    fn get_child_mut(&mut self, raw_pos: u32) -> Option<(u32, &mut Node)> {
        let mut offset_remaining = raw_pos;

        self.0.iter_mut().find_map(|(count, elem)| {
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
}

#[derive(Copy, Clone, Debug)]
struct Cursor<'a> {
    node: NonNull<NodeLeaf>,
    idx: usize,
    offset: u32, // usize? ??. This is the offset into the item at idx.
    _marker: marker::PhantomData<&'a Node>,
}

impl<'a> Cursor<'a> {
    fn next_node(&mut self) -> Option<NodeLeaf> {
        unimplemented!();
    }
    
    fn prev_node(&mut self) -> Option<NodeLeaf> {
        unimplemented!();
    }
}

impl MarkerTree {
    pub fn new() -> Self {
        Self {
            root: Box::new(Node::new())
        }
    }

    fn cursor_at_pos<'a>(&'a mut self, raw_pos: u32, stick_end: bool) -> Cursor {
        let mut node: *mut Node = self.root.as_mut();
        let mut offset_remaining = raw_pos;
        unsafe {
            while let NodeData::Internal(data) = &mut (*node).data {
                let (offset, next) = data.get_child_mut(offset_remaining).expect("Internal consistency violation");
                offset_remaining -= offset;
                node = next;
            };

            let node = (*node).unwrap_leaf_mut();
            let (idx, offset_remaining) = node.find_offset(offset_remaining, stick_end)
            .expect("Element does not contain entry");

            Cursor {
                node: NonNull::new_unchecked(node),
                idx,
                offset: offset_remaining,
                _marker: marker::PhantomData
            }
        }
    }

    // Make room at the current cursor location, splitting the current element
    // if necessary (and recursively splitting the btree node if there's no room).
    unsafe fn split_leaf<F>(cursor: &mut Cursor, gap: usize, notify: &mut F)
        where F: FnMut(CRDTLocation, NonNull<NodeLeaf>)
    {
        let node = cursor.node.as_mut();
        
        {
            // let mut entry = &mut node.0[cursor.idx];
            // let seq_len = entry.get_seq_len();
            let seq_len = node.0[cursor.idx].get_seq_len();

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
            // Split the entry in two
            debug_assert!(space_needed <= NUM_ENTRIES/4); // unnecessary but simplifies things.
            // .. actually space_needed should always be 1 or 2.
            
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
                // again) but its ok for now.

                // The other option here would be to use the index as a split
                // point and add padding into the new node to leave space.
                cursor.node = node.split_at(NUM_ENTRIES/2, filled_entries, notify);
                cursor.idx -= NUM_ENTRIES/2;
            }

            unimplemented!("split");
        }

        // There's room in the node itself. We just need to reshuffle.
        let src_idx = cursor.idx;
        let dest_idx = src_idx + space_needed;

        copy(&node.0[src_idx], &mut node.0[dest_idx], filled_entries - src_idx);
        
        // Tidy up the edges
        node.0[dest_idx].keep_end(cursor.offset);
        
        if cursor.offset > 0 {
            node.0[src_idx].keep_start(cursor.offset);
            cursor.idx += 1;
            cursor.offset = 0;
        }
    }

    /**
     * Insert a new CRDT insert / delete at some raw position in the document
     */
    pub fn insert<F>(&mut self, raw_pos: u32, len: ClientSeq, new_loc: CRDTLocation, mut notify: F)
        where F: FnMut(CRDTLocation, NonNull<NodeLeaf>)
    {
        // First walk down the tree to find the location.
        // let mut node = self;

        let mut cursor = self.cursor_at_pos(raw_pos, true);
        unsafe {
            // Insert has 3 cases:
            // - 1. The entry can be extended. We can do this inline.
            // - 2. The inserted text is at the end an entry, but the entry cannot
            //   be extended. We need to add 1 new entry to the leaf.
            // - 3. The inserted text is in the middle of an entry. We need to
            //   split the entry and insert a new entry in the middle. We need
            //   to add 2 new entries.

            let old_entry = &mut cursor.node.as_mut().0[cursor.idx];

            // We also want case 2 if the node is brand new...
            if cursor.idx == 0 && old_entry.loc.client == CLIENT_INVALID {
                *old_entry = Entry {
                    loc: new_loc,
                    len: len as i32,
                };
                notify(new_loc, cursor.node);
            } else if old_entry.len > 0 && old_entry.len as u32 == cursor.offset
                    && old_entry.loc.client == new_loc.client
                    && old_entry.loc.seq + old_entry.len as u32 == new_loc.seq {
                // Case 1 - extend the entry.
                old_entry.len += len as i32;
                notify(new_loc, cursor.node);
            } else {
                // Case 2 and 3.
                Self::split_leaf(&mut cursor, 1, &mut notify);
                cursor.node.as_mut().0[cursor.idx] = Entry {
                    loc: new_loc,
                    len: len as i32
                };
                notify(new_loc, cursor.node);
            }
        }
    }

    pub fn delete(&mut self, raw_pos: u32) {
        unimplemented!("delete");
    }

    pub unsafe fn lookup_position(loc: CRDTLocation, ptr: *const Node) -> usize {
        let mut node = &*ptr;
        let leaf = node.unwrap_leaf();

        // First find the entry
        let (mut pos, idx) = leaf.find2(loc);
        idx.expect("Internal consistency violation - could not find leaf");
        
        // Ok now ascend up the tree.
        loop {
            let parent = match node.parent {
                Some(ptr) => &*ptr.as_ptr(),
                None => break // Hit the root.
            };
            let data = parent.unwrap_internal();
            
            // Scan the node to count the length.
            for i in 0..MAX_CHILDREN {
                let (count, elem) = &data.0[i];
                
                if let Some(elem) = elem {
                    if std::ptr::eq(elem.as_ref(), node) {
                        // Found the child.
                        break;
                    } else {
                        pos += count;
                    }
                } else {
                    panic!("Could not find child in parent");
                }
            }

            // Scan the internal 

            node = parent;
        }

        pos as usize
    }
}

impl Node {
    pub fn new() -> Self {
        Node {
            parent: None,
            data: NodeData::Leaf(NodeLeaf::new())
        }
    }

    fn unwrap_leaf(&self) -> &NodeLeaf {
        match &self.data {
            NodeData::Leaf(l) => l,
            NodeData::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_leaf_mut(&mut self) -> &mut NodeLeaf {
        match &mut self.data {
            NodeData::Leaf(l) => l,
            NodeData::Internal(_) => panic!("Expected leaf - found internal node"),
        }
    }
    fn unwrap_internal(&self) -> &NodeInternal {
        match &self.data {
            NodeData::Internal(n) => n,
            NodeData::Leaf(_) => panic!("Expected internal node"),
        }
    }

}

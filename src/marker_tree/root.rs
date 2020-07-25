use super::*;

use std::ptr;

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
        let num_copied = filled_entries - src_idx;

        if num_copied > 0 {
            ptr::copy(&node.data[src_idx], &mut node.data[dest_idx], num_copied);
        }
        
        // Tidy up the edges
        if cursor.offset > 0 {
            debug_assert!(num_copied > 0);
            node.data[src_idx].keep_start(cursor.offset);
            node.data[dest_idx].keep_end(cursor.offset);
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
        let expected_size = self.count + len;

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

            // And check the total size of the tree has grown by len.
            assert_eq!(expected_size, self.count);
        }
    }

    pub fn delete(&mut self, _raw_pos: u32) {
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
                    Node::Internal(_) => 1,
                    Node::Leaf(_) => 2
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

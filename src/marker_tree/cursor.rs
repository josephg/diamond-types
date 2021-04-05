use super::*;

impl<'a> Cursor<'a> {
    pub(super) fn new(node: NonNull<NodeLeaf>, idx: usize, offset: u32) -> Self {
        // TODO: This is creating a cursor with 'static lifetime, which isn't really what we want.
        Cursor {
            node, idx, offset,
            _marker: marker::PhantomData
        }
    }

    pub(super) unsafe fn get_node_mut(&self) -> &mut NodeLeaf {
        &mut *self.node.as_ptr()
    }

    /// Internal method for prev_entry and next_entry when we need to move laterally.
    fn traverse(&mut self, direction_forward: bool) -> bool {
        // println!("** traverse called {:?} {}", self, traverse_next);
        // idx is 0. Go up as far as we can until we get to an index that has room, or we hit the
        // root.
        let node = unsafe { self.node.as_ref() };

        let mut parent = node.parent;
        let mut node_ptr = NodePtr::Leaf(self.node);
        loop {
            match parent {
                ParentPtr::Root(_) => { return false; },
                ParentPtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    // Time to find ourself up this tree.
                    let idx = node_ref.find_child(node_ptr).unwrap();
                    // println!("found myself at {}", idx);

                    let next_idx: Option<usize> = if direction_forward {
                        let next_idx = idx + 1;
                        // This would be much cleaner if I put a len field in NodeInternal instead.
                        // TODO: Consider using node_ref.count_children() instead of this mess.
                        if (next_idx < MAX_CHILDREN) && node_ref.data[next_idx].1.is_some() {
                            Some(next_idx)
                        } else { None }
                    } else {
                        if idx > 0 {
                            Some(idx - 1)
                        } else { None }
                    };
                    // println!("index {:?}", next_idx);

                    if let Some(next_idx) = next_idx {
                        // Whew - now we can descend down from here.
                        // println!("traversing laterally to {}", next_idx);
                        node_ptr = unsafe { node_ref.data[next_idx].1.as_ref().unwrap().as_ptr() };
                        break;
                    } else {
                        // idx is 0. Keep climbing that ladder!
                        node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                        parent = node_ref.parent;
                    }
                }
            }
        }

        // Now back down. We don't need idx here because we just take the first / last item in each
        // node going down the tree.
        loop {
            // println!("nodeptr {:?}", node_ptr);
            match node_ptr {
                NodePtr::Internal(n) => {
                    let node_ref = unsafe { n.as_ref() };
                    let next_idx = if direction_forward {
                        0
                    } else {
                        let num_children = node_ref.count_children();
                        assert!(num_children > 0);
                        num_children - 1
                    };
                    node_ptr = unsafe { node_ref.data[next_idx].1.as_ref().unwrap().as_ptr() };
                },
                NodePtr::Leaf(n) => {
                    // Finally.
                    let node_ref = unsafe { n.as_ref() };
                    assert!(node_ref.len > 0);
                    // println!("landed in leaf {:#?}", node_ref);
                    self.node = n;
                    if direction_forward {
                        self.idx = 0;
                        self.offset = 0;
                    } else {
                        self.idx = node_ref.len_entries() - 1;
                        self.offset = node_ref.data[self.idx].get_seq_len();
                        // println!("leaf {:?}", self);
                    }
                    return true;
                }
            }
        }
    }

    /// Move back to the previous entry. Returns true if it exists, otherwise
    /// returns false if we're at the start of the doc already.
    fn prev_entry_marker(&mut self, marker: Option<&mut FlushMarker>) -> bool {
        if self.idx > 0 {
            self.idx -= 1;
            self.offset = self.get_entry().get_seq_len();
            // println!("prev_entry get_entry returns {:?}", self.get_entry());
            true
        } else {
            if let Some(marker) = marker {
                marker.flush(unsafe { self.node.as_mut() });
            }
            self.traverse(false)
        }
    }

    fn prev_entry(&mut self) -> bool {
        self.prev_entry_marker(None)
    }

    pub(super) fn next_entry_marker(&mut self, marker: Option<&mut FlushMarker>) -> bool {
        // TODO: Do this without code duplication.
        unsafe {
            if self.idx + 1 < self.node.as_ref().len as usize {
                self.idx += 1;
                self.offset = 0;
                true
            } else {
                if let Some(marker) = marker {
                    marker.flush(self.node.as_mut());
                }
                self.traverse(true)
            }
        }
    }

    pub(super) fn next_entry(&mut self) -> bool {
        self.next_entry_marker(None)
    }

    pub(super) fn get_pos(&self) -> u32 {
        let node = unsafe { self.node.as_ref() };
        
        let mut pos: u32 = 0;
        // First find out where we are in the current node.
        
        // TODO: This is a bit redundant - we could find out the local position
        // when we scan initially to initialize the cursor.
        for e in &node.data[0..self.idx] {
            pos += e.get_content_len();
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

                    // node_ptr = NodePtr::Internal(unsafe { NonNull::new_unchecked(node_ref as *const _ as *mut _) });
                    node_ptr = NodePtr::Internal(n);
                    parent = node_ref.parent;
                }
            }
        }

        pos
    }

    pub(super) fn get_entry(&self) -> &Entry {
        let node = unsafe { self.node.as_ref() };
        // println!("entry {:?}", self);
        &node.data[self.idx]
    }

    pub(super) fn get_entry_mut(&mut self) -> &mut Entry {
        let node = unsafe { self.node.as_mut() };
        debug_assert!(self.idx < node.len_entries());
        &mut node.data[self.idx]
    }
    
    pub fn tell(mut self) -> CRDTLocation {
        while self.idx == 0 || self.get_entry().len < 0 {
            // println!("\nentry {:?}", self);
            let exists = self.prev_entry();
            if !exists { return CRDT_DOC_ROOT; }
            // println!("-> prev {:?} inside {:#?}", self, unsafe { self.node.as_ref() });
            // println!();
        }

        let entry = self.get_entry(); // Shame this is called twice but eh.
        CRDTLocation {
            agent: entry.loc.agent,
            seq: entry.loc.seq + self.offset
        }
    }

    // This is a terrible name. This method modifies a cursor at the end of a
    // span to be a cursor to the start of the next span.
    pub(super) fn roll_to_next(&mut self, stick_end: bool) {
        unsafe {
            let node = self.node.as_ref();
            let seq_len = node.data[self.idx].get_seq_len();

            debug_assert!(self.offset <= seq_len);

            // If we're at the end of the current entry, skip it.
            if self.offset == seq_len {
                self.offset = 0;
                self.idx += 1;
                // entry = &mut node.0[cursor.idx];

                if !stick_end && self.idx >= node.len as usize {
                    self.next_entry();
                }
            }

        }
    }
}
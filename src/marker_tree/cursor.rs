use super::*;

impl<'a> Cursor<'a> {
    pub(super) fn new(node: NonNull<NodeLeaf>, idx: usize, offset: u32) -> Self {
        Cursor {
            node, idx, offset, _marker: marker::PhantomData
        }
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

    pub(super) fn get_pos(&self) -> u32 {
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
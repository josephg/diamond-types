use super::*;

use smallvec::SmallVec;

pub type DeleteResult<E> = SmallVec<[E; 2]>;
pub fn extend_delete<E: EntryTraits>(delete: &mut DeleteResult<E>, entry: E) {
    // println!("extend_delete {:?}", op);
    if let Some(last) = delete.last_mut() {
        if last.can_append(&entry) {
            // Extend!
            last.append(entry);
        } else { delete.push(entry); }
    } else { delete.push(entry); }
}

impl<E: EntryTraits> RangeTree<E> {
    pub fn new() -> Pin<Box<Self>> {
        let mut tree = Box::pin(Self {
            count: 0,
            root: unsafe { Node::new_leaf() },
            last_cursor: Cell::new(None),
            _pin: marker::PhantomPinned,
        });

        // What a mess. I'm sure there's a nicer way to write this, somehow O_o.
        let parent_ref = unsafe { tree.as_ref().get_ref().to_parent_ptr() };
        tree.as_mut().root_ref_mut().set_parent(parent_ref);

        tree
    }

    fn root_ref_mut(self: Pin<&mut Self>) -> &mut Node<E> {
        unsafe {
            &mut self.get_unchecked_mut().root
        }
    }

    pub fn len(&self) -> usize {
        self.count as _
    }

    unsafe fn to_parent_ptr(&self) -> ParentPtr<E> {
        ParentPtr::Root(ref_to_nonnull(self))
    }

    pub fn cursor_at_pos(&self, raw_pos: usize, stick_end: bool) -> Cursor<E> {
        // if let Some((pos, mut cursor)) = self.last_cursor.get() {
        //     if pos == raw_pos {
        //         if cursor.offset == 0 {
        //             cursor.prev_entry();
        //         }
        //         return cursor;
        //     }
        // }

        unsafe {
            let mut node = self.root.as_ptr();
            let mut offset_remaining = raw_pos;
            while let NodePtr::Internal(data) = node {
                let (new_offset_remaining, next) = data.as_ref()
                    .get_child_ptr(offset_remaining, stick_end)
                    .expect("Internal consistency violation");
                offset_remaining = new_offset_remaining as usize;
                node = next;
            };

            let leaf_ptr = node.unwrap_leaf();
            let (idx, offset_remaining) = leaf_ptr.as_ref().find_offset(offset_remaining, stick_end)
            .expect("Element does not contain entry");

            Cursor {
                node: leaf_ptr,
                idx,
                offset: offset_remaining,
                // _marker: marker::PhantomData
            }
        }
    }

    // pub fn clear_cursor_cache(self: &Pin<Box<Self>>) {
    //     self.as_ref().last_cursor.set(None);
    // }
    // pub fn cache_cursor(self: &Pin<Box<Self>>, pos: usize, cursor: Cursor<E>) {
    //     self.as_ref().last_cursor.set(Some((pos, cursor)));
    // }

    pub fn iter(&self) -> Cursor<E> {
        self.cursor_at_pos(0, false)
    }

    pub fn next_entry_or_panic(cursor: &mut Cursor<E>, marker: &mut FlushMarker) {
        if cursor.next_entry_marker(Some(marker)) == false {
            panic!("Local delete past the end of the document");
        }
    }

    // Returns size.
    fn check_leaf(leaf: &NodeLeaf<E>, expected_parent: ParentPtr<E>) -> usize {
        assert_eq!(leaf.parent, expected_parent);
        
        let mut count: usize = 0;
        let mut done = false;
        let mut num: usize = 0;

        for e in &leaf.data[..] {
            if e.is_valid() {
                // Make sure there's no data after an invalid entry
                assert_eq!(done, false, "Leaf contains gaps");
                assert_ne!(e.len(), 0, "Invalid leaf - 0 length");
                count += e.content_len() as usize;
                num += 1;
            } else {
                done = true;
            }
        }

        // An empty leaf is only valid if we're the root element.
        if let ParentPtr::Internal(_) = leaf.parent {
            assert!(num > 0, "Non-root leaf is empty");
        }

        assert_eq!(num, leaf.num_entries as usize, "Cached leaf len does not match");

        count
    }
    
    // Returns size.
    fn check_internal(node: &NodeInternal<E>, expected_parent: ParentPtr<E>) -> usize {
        assert_eq!(node.parent, expected_parent);
        
        let mut count_total: usize = 0;
        let mut done = false;
        let mut child_type = None; // Make sure all the children have the same type.
        // let self_parent = ParentPtr::Internal(NonNull::new(node as *const _ as *mut _).unwrap());
        let self_parent = unsafe { node.to_parent_ptr() };

        for (child_count_expected, child) in &node.data[..] {
            if let Some(child) = child {
                // Make sure there's no data after an invalid entry
                assert_eq!(done, false);

                let child_ref = child;

                let actual_type = match child_ref {
                    Node::Internal(_) => 1,
                    Node::Leaf(_) => 2,
                };
                // Make sure all children have the same type.
                if child_type.is_none() { child_type = Some(actual_type) }
                else { assert_eq!(child_type, Some(actual_type)); }

                // Recurse
                let count_actual = match child_ref {
                    Node::Leaf(ref n) => { Self::check_leaf(n.as_ref().get_ref(), self_parent) },
                    Node::Internal(ref n) => { Self::check_internal(n.as_ref().get_ref(), self_parent) },
                };

                // Make sure all the individual counts match.
                // if *child_count_expected as usize != count_actual {
                //     eprintln!("xxx {:#?}", node);
                // }
                assert_eq!(*child_count_expected as usize, count_actual, "Child node count does not match");
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
        // println!("check tree {:#?}", self);
        let root = &self.root;
        let expected_parent = ParentPtr::Root(unsafe { ref_to_nonnull(self) });
        let expected_size = match root {
            Node::Internal(n) => { Self::check_internal(&n, expected_parent) },
            Node::Leaf(n) => { Self::check_leaf(&n, expected_parent) },
        };
        assert_eq!(self.count as usize, expected_size, "tree.count is incorrect");
    }

    fn print_node_tree(node: &Node<E>, depth: usize) {
        for _ in 0..depth { eprint!("  "); }
        match node {
            Node::Internal(n) => {
                let n = n.as_ref().get_ref();
                eprintln!("Internal {:?} (parent: {:?})", n as *const _, n.parent);
                let mut unused = 0;
                for (_, e) in &n.data[..] {
                    if let Some(e) = e {
                        Self::print_node_tree(e, depth + 1);
                    } else { unused += 1; }
                }

                if unused > 0 {
                    for _ in 0..=depth { eprint!("  "); }
                    eprintln!("({} empty places)", unused);
                }
            },
            Node::Leaf(n) => {
                eprintln!("Leaf {:?} (parent: {:?}) - {} filled", n as *const _, n.parent, n.len_entries());
            }
        }
    }

    #[allow(unused)]
    pub fn print_ptr_tree(&self) {
        eprintln!("Tree count {} ptr {:?}", self.count, self as *const _);
        Self::print_node_tree(&self.root, 1);
    }

    pub unsafe fn cursor_at_marker(loc: E::Item, ptr: NonNull<NodeLeaf<E>>) -> Cursor<E> {
        // First make a cursor to the specified item
        let leaf = ptr.as_ref();
        let cursor = leaf.find(loc).expect("Position not in named leaf");
        // cursor.count_pos() as _
        cursor
    }

    #[allow(unused)]
    pub fn print_stats(&self) {
        // We'll get the distribution of entry sizes
        let mut size_counts = vec!();

        for entry in self.iter() {
            // println!("entry {:?}", entry);
            let bucket = entry.len() as usize;
            if bucket >= size_counts.len() {
                size_counts.resize(bucket + 1, 0);
            }
            size_counts[bucket] += 1;
        }

        println!("Entry distribution {:?}", size_counts);
    }

    #[allow(unused)]
    pub(crate) fn count_entries(&self) -> usize {
        self.iter().fold(0, |a, _| a + 1)
    }
}

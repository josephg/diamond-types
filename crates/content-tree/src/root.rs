use std::mem::size_of;

use humansize::{file_size_opts, FileSize};
use smallvec::SmallVec;
use rle::{Searchable, merge_items};
use super::*;

pub type DeleteResult<E> = SmallVec<[E; 8]>;

impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn new() -> Pin<Box<Self>> {
        let mut tree = Box::pin(Self {
            count: I::IndexValue::default(),
            root: unsafe { Node::Leaf(Box::pin(NodeLeaf::new(None))) },
            // last_cursor: Cell::new(None),
            _pin: marker::PhantomPinned,
        });

        // What a mess. I'm sure there's a nicer way to write this, somehow O_o.
        let parent_ref = unsafe { tree.as_ref().get_ref().to_parent_ptr() };
        tree.as_mut().root_ref_mut().set_parent(parent_ref);

        tree
    }

    fn root_ref_mut(self: Pin<&mut Self>) -> &mut Node<E, I, IE, LE> {
        unsafe {
            &mut self.get_unchecked_mut().root
        }
    }

    pub fn len(&self) -> I::IndexValue {
        self.count
    }

    // pub fn get(&self, pos: usize) -> Option<E::Item> {
    //     let cursor = self.cursor_at_pos(pos, false);
    //     cursor.get_item()
    // }

    pub(crate) unsafe fn to_parent_ptr(&self) -> ParentPtr<E, I, IE, LE> {
        ParentPtr::Root(ref_to_nonnull(self))
    }

    pub fn unsafe_cursor_at_query<F, G>(&self, raw_pos: usize, stick_end: bool, offset_to_num: F, entry_to_num: G) -> UnsafeCursor<E, I, IE, LE>
            where F: Fn(I::IndexValue) -> usize, G: Fn(E) -> usize {
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
                    .find_child_at_offset(offset_remaining, stick_end, &offset_to_num)
                    .expect("Internal consistency violation");
                offset_remaining = new_offset_remaining;
                node = next;
            };

            let leaf_ptr = node.unwrap_leaf();
            let node = leaf_ptr.as_ref();

            let (idx, offset_remaining) = if node.num_entries == 0 {
                (0, usize::MAX)
            } else {
                node.find_offset(offset_remaining, stick_end, entry_to_num)
                    .expect("Element does not contain entry")
            };

            UnsafeCursor {
                node: leaf_ptr,
                idx,
                offset: offset_remaining,
                // _marker: marker::PhantomData
            }
        }
    }

    pub(crate) fn leaf_at_start(&self) -> &NodeLeaf<E, I, IE, LE> {
        // There is always at least one leaf, so this is safe!
        unsafe {
            let mut node = self.root.as_ptr();
            while let NodePtr::Internal(data) = node {
                node = data.as_ref().children[0].as_ref().unwrap().as_ptr()
            };

            node.unwrap_leaf().as_ref()
        }
    }

    pub fn unsafe_cursor_at_start(&self) -> UnsafeCursor<E, I, IE, LE> {
        // TODO: Consider moving this into unsafe_cursor
        unsafe {
            let leaf_ref = self.leaf_at_start();
            UnsafeCursor {
                node: NonNull::new_unchecked(leaf_ref as *const _ as *mut _),
                idx: 0,
                offset: if leaf_ref.num_entries == 0 { usize::MAX } else { 0 },
                // _marker: marker::PhantomData
            }
        }
    }

    pub fn unsafe_cursor_at_end(&self) -> UnsafeCursor<E, I, IE, LE> {
        // There's ways to write this to be faster, but this method is called rarely enough that it
        // should be fine.
        // let cursor = self.cursor_at_query(offset_to_num(self.count), true, offset_to_num, entry_to_num);

        let cursor = unsafe {
            let mut node = self.root.as_ptr();
            while let NodePtr::Internal(ptr) = node {
                node = ptr.as_ref().last_child();
            };

            // Now scan to the end of the leaf
            let leaf_ptr = node.unwrap_leaf();
            let leaf = leaf_ptr.as_ref();
            let (idx, offset) = if leaf.len_entries() == 0 {
                // We're creating a cursor into an empty range tree.
                (0, usize::MAX)
            } else {
                let idx = leaf.len_entries() - 1;
                let offset = leaf.data[idx].len();
                (idx, offset)
            };
            UnsafeCursor {
                node: leaf_ptr,
                idx,
                offset
            }
        };

        if cfg!(debug_assertions) {
            // Make sure nothing went wrong while we're here.
            let mut cursor = cursor.clone();
            let node = unsafe { cursor.node.as_ref() };
            if let Some(entry) = cursor.try_get_raw_entry() {
                assert_eq!(entry.len(), cursor.offset);
            }
            if node.len_entries() > 0 {
                assert_eq!(cursor.idx, node.len_entries() - 1);
            }
            assert!(!cursor.next_entry());
        }

        cursor
    }

    // pub fn clear_cursor_cache(self: &Pin<Box<Self>>) {
    //     self.as_ref().last_cursor.set(None);
    // }
    // pub fn cache_cursor(self: &Pin<Box<Self>>, pos: usize, cursor: Cursor<E>) {
    //     self.as_ref().last_cursor.set(Some((pos, cursor)));
    // }

    pub fn next_entry_or_panic(cursor: &mut UnsafeCursor<E, I, IE, LE>, marker: &mut I::IndexUpdate) {
        if !cursor.next_entry_marker(Some(marker)) {
            panic!("Local delete past the end of the document");
        }
    }

    // Returns size.
    fn check_leaf(leaf: &NodeLeaf<E, I, IE, LE>, expected_parent: ParentPtr<E, I, IE, LE>) -> I::IndexValue {
        assert_eq!(leaf.parent, expected_parent);
        
        // let mut count: usize = 0;
        let mut count = I::IndexValue::default();

        for e in &leaf.data[..leaf.num_entries as usize] {
            // assert!(e.is_valid());

            // Make sure there's no data after an invalid entry
            assert_ne!(e.len(), 0, "Invalid leaf - 0 length");
            // count += e.content_len() as usize;
            I::increment_offset(&mut count, e);
        }

        // An empty leaf is only valid if we're the root element.
        if let ParentPtr::Internal(_) = leaf.parent {
            assert_ne!(leaf.num_entries, 0, "Non-root leaf is empty");
        }

        // Check the next pointer makes sense.
        // Note we're using adjacent_leaf_by_traversal, which forces the full traversal.
        let next = leaf.adjacent_leaf_by_traversal(true);
        assert_eq!(next, leaf.next);

        count
    }
    
    // Returns size.
    fn check_internal(node: &NodeInternal<E, I, IE, LE>, expected_parent: ParentPtr<E, I, IE, LE>) -> I::IndexValue {
        assert_eq!(node.parent, expected_parent);
        
        // let mut count_total: usize = 0;
        let mut count_total = I::IndexValue::default();
        let mut done = false;
        let mut child_type = None; // Make sure all the children have the same type.
        // let self_parent = ParentPtr::Internal(NonNull::new(node as *const _ as *mut _).unwrap());
        let self_parent = unsafe { node.to_parent_ptr() };

        for idx in 0..node.index.len() {
            let child_count_expected = node.index[idx];
            let child = &node.children[idx];

            if let Some(child) = child {
                // Make sure there's no data after an invalid entry
                assert!(!done);

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
                assert_eq!(child_count_expected, count_actual, "Child node count does not match");
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
            Node::Internal(n) => { Self::check_internal(n, expected_parent) },
            Node::Leaf(n) => { Self::check_leaf(n, expected_parent) },
        };
        assert_eq!(self.count, expected_size, "tree.count is incorrect");
    }

    fn print_node_tree(node: &Node<E, I, IE, LE>, depth: usize) {
        for _ in 0..depth { eprint!("  "); }
        match node {
            Node::Internal(n) => {
                let n = n.as_ref().get_ref();
                eprintln!("Internal {:?} (parent: {:?})", n as *const _, n.parent);
                let mut unused = 0;
                for e in &n.children[..] {
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
        eprintln!("Tree count {:?} ptr {:?}", self.count, self as *const _);
        Self::print_node_tree(&self.root, 1);
    }

    #[allow(unused)]
    pub fn print_stats(&self, name: &str, detailed: bool) {
        // We'll get the distribution of entry sizes
        let mut size_counts = vec!();

        for entry in self.raw_iter() {
            // println!("entry {:?}", entry);
            let bucket = entry.len() as usize;
            if bucket >= size_counts.len() {
                size_counts.resize(bucket + 1, 0);
            }
            size_counts[bucket] += 1;
        }

        let (num_internal_nodes, num_leaf_nodes) = self.count_nodes();
        let leaf_node_size = num_leaf_nodes * size_of::<NodeLeaf<E, I, IE, LE>>();
        let internal_node_size = num_internal_nodes * size_of::<NodeInternal<E, I, IE, LE>>();
        let num_entries = self.count_entries();

        println!("-------- Range tree {} stats --------", name);
        println!("Number of {} byte entries: {} ({} bytes of entries)",
             size_of::<E>(),
             num_entries,
             (num_entries * size_of::<E>()).file_size(file_size_opts::CONVENTIONAL).unwrap()
        );
        println!("Number of {} byte internal nodes {} ({})",
             size_of::<NodeInternal<E, I, IE, LE>>(),
             num_internal_nodes,
             internal_node_size.file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("Number of {} byte leaf nodes {} ({}) (space for {} entries)",
             size_of::<NodeLeaf<E, I, IE, LE>>(),
             num_leaf_nodes,
             leaf_node_size.file_size(file_size_opts::CONVENTIONAL).unwrap(),
             num_leaf_nodes * LE
        );

        println!("Depth {}", self.get_depth());
        println!("Total range tree memory usage {}",
             self.count_total_memory().file_size(file_size_opts::CONVENTIONAL).unwrap());

        let compacted_entries = merge_items(self.raw_iter()).count();
        // println!("(efficient size: {})", (self.count_entries() * size_of::<E>()).file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("Compacts to {} entries / {} bytes",
             compacted_entries,
             (compacted_entries * size_of::<E>()).file_size(file_size_opts::CONVENTIONAL).unwrap()
        );

        // This prints the first 100 items of the real entries, and maximally compacted entries:
        // for e in self.iter().take(100) {
        //     println!("{:?}", e);
        // }
        // println!("\n\n");
        // for e in compacted.iter().take(100) {
        //     println!("{:?}", e);
        // }

        if detailed {
            println!("Entry distribution {:?}", size_counts);
            println!("Internal node size {}", std::mem::size_of::<NodeInternal<E, I, IE, LE>>());
            println!("Node entry size {} alignment {}",
                     std::mem::size_of::<Option<Node<E, I, IE, LE>>>(),
                     std::mem::align_of::<Option<Node<E, I, IE, LE>>>());
            println!("Leaf size {}", std::mem::size_of::<NodeLeaf<E, I, IE, LE>>());
        }
    }

    fn get_depth(&self) -> usize {
        unsafe {
            let mut depth = 0;
            let mut node = self.root.as_ptr();
            while let NodePtr::Internal(data) = node {
                depth += 1;
                node = data.as_ref().children[0].as_ref().unwrap().as_ptr()
            };
            depth
        }
    }

    #[allow(unused)]
    pub fn count_entries(&self) -> usize {
        self.raw_iter().fold(0, |a, _| a + 1)
    }

    // Passing (num internal nodes, num leaf nodes).
    fn count_nodes_internal(node: &Node<E, I, IE, LE>, num: &mut (usize, usize)) {
        if let Node::Internal(n) = node {
            num.0 += 1;

            for e in n.children[..].iter().flatten() {
                Self::count_nodes_internal(e, num);
            }
        } else { num.1 += 1; }
    }

    #[allow(unused)]
    pub fn count_nodes(&self) -> (usize, usize) {
        let mut num = (0, 0);
        Self::count_nodes_internal(&self.root, &mut num);
        num
    }

    fn count_memory_internal(node: &Node<E, I, IE, LE>, size: &mut usize) {
        match node {
            Node::Internal(n) => {
                *size += size_of::<NodeInternal<E, I, IE, LE>>();

                for e in n.children[..].iter().flatten() {
                    Self::count_memory_internal(e, size);
                }
            }
            Node::Leaf(_) => {
                *size += std::mem::size_of::<NodeLeaf<E, I, IE, LE>>();
            }
        }
    }

    #[allow(unused)]
    pub fn count_total_memory(&self) -> usize {
        let mut size = size_of::<ContentTreeRaw<E, I, IE, LE>>();
        Self::count_memory_internal(&self.root, &mut size);
        size
    }
}

impl<E: ContentTraits + Searchable, I: TreeIndex<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    /// Returns a cursor right before the named location, referenced by the pointer.
    pub unsafe fn cursor_before_item(loc: E::Item, ptr: NonNull<NodeLeaf<E, I, IE, LE>>) -> UnsafeCursor<E, I, IE, LE> {
        // First make a cursor to the specified item
        let leaf = ptr.as_ref();
        leaf.find(loc).expect("Position not in named leaf")
    }
}

impl<E: ContentTraits + ContentLength, I: FindContent<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn content_len(&self) -> usize {
        I::index_to_content(self.count)
    }
    
    pub fn unsafe_cursor_at_content_pos(&self, pos: usize, stick_end: bool) -> UnsafeCursor<E, I, IE, LE> {
        self.unsafe_cursor_at_query(pos, stick_end, I::index_to_content, |e| e.content_len())
    }

    pub fn cursor_at_content_pos(&self, pos: usize, stick_end: bool) -> Cursor<E, I, IE, LE> {
        self.cursor_at_query(pos, stick_end, I::index_to_content, |e| e.content_len())
    }

    pub fn mut_cursor_at_content_pos<'a>(self: &'a mut Pin<Box<Self>>, pos: usize, stick_end: bool) -> MutCursor<'a, E, I, IE, LE> {
        self.mut_cursor_at_query(pos, stick_end, I::index_to_content, |e| e.content_len())
    }
}

impl<E: ContentTraits, I: FindOffset<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn offset_len(&self) -> usize {
        I::index_to_offset(self.count)
    }

    pub fn unsafe_cursor_at_offset_pos(&self, pos: usize, stick_end: bool) -> UnsafeCursor<E, I, IE, LE> {
        self.unsafe_cursor_at_query(pos, stick_end, I::index_to_offset, |e| e.len())
    }

    pub fn cursor_at_offset_pos(&self, pos: usize, stick_end: bool) -> Cursor<E, I, IE, LE> {
        self.cursor_at_query(pos, stick_end, I::index_to_offset, |e| e.len())
    }

    pub fn mut_cursor_at_offset_pos<'a>(self: &'a mut Pin<Box<Self>>, pos: usize, stick_end: bool) -> MutCursor<'a, E, I, IE, LE> {
        self.mut_cursor_at_query(pos, stick_end, I::index_to_offset, |e| e.len())
    }
}
    
impl<E: ContentTraits + Searchable, I: FindOffset<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn at_offset(&self, pos: usize) -> Option<E::Item> {
        let cursor = self.unsafe_cursor_at_offset_pos(pos, false);
        unsafe { cursor.get_item() }
    }
}

impl<E: ContentTraits + ContentLength + Searchable, I: FindContent<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    pub fn at_content(&self, pos: usize) -> Option<E::Item> {
        let cursor = self.unsafe_cursor_at_content_pos(pos, false);
        unsafe { cursor.get_item() }
    }
}

impl<E: ContentTraits + PartialEq, I: TreeIndex<E>, const IE: usize, const LE: usize> PartialEq for ContentTreeRaw<E, I, IE, LE> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<E: ContentTraits + PartialEq, I: TreeIndex<E>, const IE: usize, const LE: usize> Eq for ContentTreeRaw<E, I, IE, LE> {}

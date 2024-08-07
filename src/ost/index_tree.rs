//! This file contains an implementation of [`IndexTree`] - which is a run-length encoded, in-memory
//! BTree mapping from integers to some value type.
//!
//! The merging algorithm uses this type to find the item which stores a specific local version
//! value.

use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::ops::{Index, IndexMut, Range};
use rle::{HasLength, RleDRun};
use crate::{DTRange, LV};
use crate::ost::{LEAF_CHILDREN, LeafIdx, NODE_CHILDREN, NodeIdx, remove_from_array, remove_from_array_fill};

#[derive(Debug, Clone)]
pub(crate) struct IndexTree<V: Copy> {
    leaves: Vec<IndexLeaf<V>>,
    nodes: Vec<IndexNode>,

    height: usize,
    root: usize,
    cursor: Cell<(LV, IndexCursor)>,

    // Linked lists.
    // free_leaf_pool_head: LeafIdx,
    // free_node_pool_head: NodeIdx,
}

#[derive(Debug, Clone, Copy)]
struct IndexCursor {
    // The item pointed to by the cursor should still be in the CPU's L1 cache. I could cache some
    // properties of the cursor's leaf item here, but I think it wouldn't improve performance -
    // since we wouldn't be saving any memory loads anyway.
    leaf_idx: LeafIdx,
    elem_idx: usize,
}

// Wouldn't need this impl if LeafIdx defaulted to 0...
impl Default for IndexCursor {
    fn default() -> Self {
        IndexCursor {
            leaf_idx: LeafIdx(0),
            elem_idx: 0,
        }
    }
}

const NODE_SPLIT_POINT: usize = NODE_CHILDREN / 2;
const LEAF_SPLIT_POINT: usize = LEAF_CHILDREN / 2;

#[derive(Debug, Clone)]
pub struct IndexLeaf<V> {
    /// The bounds is usize::MAX for unused items. The last item has an upper bound equal to the
    /// start bound of the first item in the next leaf. This is also cached in upper_bound.
    bounds: [LV; LEAF_CHILDREN],
    children: [V; LEAF_CHILDREN],

    next_leaf: LeafIdx,
    parent: NodeIdx,
}

/// A node child specifies the LV of the (recursive) first element and an index in the data
/// structure. The index is either an index into the internal nodes or leaf nodes depending on the
/// height.
type NodeChild = (LV, usize);

#[derive(Debug, Clone)]
pub struct IndexNode {
    /// Child entries point to either another node or a leaf. We disambiguate using the height.
    /// The named LV is the first LV of the child data.
    ///
    /// Children are (usize::MAX, usize::MAX) if they are unset.
    children: [NodeChild; NODE_CHILDREN],
    parent: NodeIdx,
}

fn initial_root_leaf<V: Default + Copy>() -> IndexLeaf<V> {
    // The tree is initialized with V::Default covering the entire range. This means we don't need
    // to have any special handling for the size of the tree. Set operations "carve out" their
    // specified value.
    let mut bounds = [usize::MAX; LEAF_CHILDREN];
    bounds[0] = 0;

    IndexLeaf {
        bounds,
        children: [V::default(); LEAF_CHILDREN],
        // upper_bound: usize::MAX, // The bounds of the last item is (functionally) infinity.
        next_leaf: LeafIdx(usize::MAX),
        parent: NodeIdx(usize::MAX), // This node won't exist yet - but thats ok.
    }
}

const EMPTY_NODE_CHILD: NodeChild = (usize::MAX, usize::MAX);

impl<V: Copy> IndexLeaf<V> {
    #[inline(always)]
    fn has_space(&self, space_wanted: usize) -> bool {
        if space_wanted == 0 { return true; }
        debug_assert!(space_wanted < LEAF_CHILDREN);
        self.bounds[LEAF_CHILDREN - space_wanted] == usize::MAX

        // We could alternately write this:
        // self.bounds[LEAF_CHILDREN.wrapping_sub(space_wanted) % LEAF_CHILDREN] == usize::MAX
        // ... But since has_space is always inlined, the compiler knows it can never panic anyway.
    }

    fn is_last(&self) -> bool { !self.next_leaf.exists() }

    fn remove_children(&mut self, del_range: Range<usize>) {
        remove_from_array_fill(&mut self.bounds, del_range.clone(), usize::MAX);
        remove_from_array(&mut self.children, del_range.clone());
    }
}

impl IndexNode {
    fn is_full(&self) -> bool {
        self.children.last().unwrap().1 != usize::MAX
    }

    fn remove_children(&mut self, del_range: Range<usize>) {
        remove_from_array_fill(&mut self.children, del_range.clone(), EMPTY_NODE_CHILD);
    }
}

impl<V: Default + IndexContent> Default for IndexTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Copy> Index<LeafIdx> for IndexTree<V> {
    type Output = IndexLeaf<V>;

    fn index(&self, index: LeafIdx) -> &Self::Output {
        &self.leaves[index.0]
    }
}
impl<V: Copy> IndexMut<LeafIdx> for IndexTree<V> {
    fn index_mut(&mut self, index: LeafIdx) -> &mut Self::Output {
        &mut self.leaves[index.0]
    }
}
impl<V: Copy> Index<NodeIdx> for IndexTree<V> {
    type Output = IndexNode;

    fn index(&self, index: NodeIdx) -> &Self::Output {
        &self.nodes[index.0]
    }
}
impl<V: Copy> IndexMut<NodeIdx> for IndexTree<V> {
    fn index_mut(&mut self, index: NodeIdx) -> &mut Self::Output {
        &mut self.nodes[index.0]
    }
}


/// I'm not sure if this is a good idea. The index stores its base positions separate from the
/// content.
///
/// Essentially index content must splitable & mergable be such that .truncate() / .append() are
/// no-ops. .can_append will also need the base & offset.
// pub trait IndexContent: Debug + Copy + Eq {
pub trait IndexContent: Debug + Copy {
    /// Try to append other to self. If possible, self is modified (if necessary) and true is
    /// returned.
    fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool;

    fn at_offset(&self, offset: usize) -> Self;

    fn eq(&self, other: &Self, upto_len: usize) -> bool;
}

fn split_rle<V: IndexContent>(val: RleDRun<V>, offset: usize) -> (RleDRun<V>, RleDRun<V>) {
    debug_assert!(offset > 0);
    debug_assert!(offset < (val.end - val.start));

    (RleDRun {
        start: val.start,
        end: val.start + offset,
        val: val.val,
    }, RleDRun {
        start: val.start + offset,
        end: val.end,
        val: val.val.at_offset(offset),
    })
}

impl<V: Default + IndexContent> IndexTree<V> {
    pub fn new() -> Self {
        Self {
            leaves: vec![initial_root_leaf()],
            nodes: vec![],
            height: 0,
            root: 0,
            cursor: Default::default(),
            // free_leaf_pool_head: LeafIdx(usize::MAX),
            // free_node_pool_head: NodeIdx(usize::MAX),
        }
    }

    pub fn clear(&mut self) {
        self.leaves.clear();
        self.nodes.clear();
        self.height = 0;
        self.root = 0;
        self.cursor = Default::default();
        // self.free_leaf_pool_head = LeafIdx(usize::MAX);
        // self.free_node_pool_head = NodeIdx(usize::MAX);

        self.leaves.push(initial_root_leaf());
    }

    fn create_new_root_node(&mut self, lower_bound: usize, child_a: usize, split_point: LV, child_b: usize) -> NodeIdx {
        self.height += 1;
        let mut new_root = IndexNode {
            children: [EMPTY_NODE_CHILD; NODE_CHILDREN],
            parent: Default::default(),
        };
        new_root.children[0] = (lower_bound, child_a);
        new_root.children[1] = (split_point, child_b);

        let new_idx = self.nodes.len();
        // println!("Setting root to {new_idx}");
        self.root = new_idx;
        self.nodes.push(new_root);
        NodeIdx(new_idx)
    }

    /// This method always splits a node in the middle. This isn't always optimal, but its simpler.
    fn split_node(&mut self, old_idx: NodeIdx, children_are_leaves: bool) -> NodeIdx {
        // Split a full internal node into 2 nodes.
        let new_node_idx = self.nodes.len();
        // println!("split node -> {new_node_idx}");
        let old_node = &mut self.nodes[old_idx.0];
        let split_lv = old_node.children[NODE_SPLIT_POINT].0;

        // The old leaf must be full before we split it.
        debug_assert!(old_node.is_full());

        // eprintln!("split node {:?} -> {:?} + {:?} (leaves: {children_are_leaves})", old_idx, old_idx, new_node_idx);
        // eprintln!("split start {:?} / {:?}", &old_node.children[..NODE_SPLIT_POINT], &old_node.children[NODE_SPLIT_POINT..]);

        let mut new_node = IndexNode {
            children: [EMPTY_NODE_CHILD; NODE_CHILDREN],
            parent: NodeIdx(usize::MAX), // Overwritten below.
        };

        new_node.children[0..NODE_SPLIT_POINT].copy_from_slice(&old_node.children[NODE_SPLIT_POINT..]);
        old_node.children[NODE_SPLIT_POINT..].fill(EMPTY_NODE_CHILD);

        if children_are_leaves {
            for (_, idx) in &new_node.children[..NODE_SPLIT_POINT] {
                self.leaves[*idx].parent = NodeIdx(new_node_idx);
            }
        } else {
            for (_, idx) in &new_node.children[..NODE_SPLIT_POINT] {
                self.nodes[*idx].parent = NodeIdx(new_node_idx);
            }
        }

        debug_assert_eq!(new_node_idx, self.nodes.len());
        // let split_point_lv = new_node.children[0].0;
        self.nodes.push(new_node);

        // It would be much nicer to do this above earlier - and in earlier versions I did.
        // The problem is that both create_new_root_node and insert_into_node can insert new items
        // into self.nodes. If that happens, the new node index we're expecting to use is used by
        // another node. Hence, we need to call self.nodes.push() before calling any other function
        // which modifies the node list.
        let old_node = &self.nodes[old_idx.0];
        if old_idx.0 == self.root {
            let lower_bound = old_node.children[0].0;
            // We'll make a new root.
            let parent = self.create_new_root_node(
                                                    lower_bound, old_idx.0,
                                                    split_lv, new_node_idx);
            self.nodes[old_idx.0].parent = parent;
            self.nodes[new_node_idx].parent = parent
        } else {
            let parent = old_node.parent;
            self.nodes[new_node_idx].parent = self.insert_into_node(parent, (split_lv, new_node_idx), old_idx.0, false);
        }

        NodeIdx(new_node_idx)
    }

    #[must_use]
    fn insert_into_node(&mut self, mut node_idx: NodeIdx, new_child: NodeChild, after_child: usize, children_are_leaves: bool) -> NodeIdx {
        let mut node = &mut self[node_idx];

        // Where will the child go? I wonder if the compiler can do anything smart with this...
        let mut insert_pos = node.children
            .iter()
            .position(|(_, idx)| { *idx == after_child })
            .unwrap() + 1;

        // dbg!(&node);
        // println!("insert_into_node n={:?} after_child {after_child} pos {insert_pos}, new_child {:?}", node_idx, new_child);

        if node.is_full() {
            let new_node = self.split_node(node_idx, children_are_leaves);

            if insert_pos >= NODE_SPLIT_POINT {
                // Actually we're inserting into the new node.
                insert_pos -= NODE_SPLIT_POINT;
                node_idx = new_node;
            }
            // Technically this only needs to be reassigned in the if() above, but reassigning it
            // in all cases is necessary for the borrowck.
            node = &mut self[node_idx];
        }

        // Could scan to find the actual length of the children, then only memcpy that many. But
        // memcpy is cheap.
        node.children.copy_within(insert_pos..NODE_CHILDREN - 1, insert_pos + 1);
        node.children[insert_pos] = new_child;

        if insert_pos == 0 {
            let parent = node.parent;
            Self::recursively_update_nodes(&mut self.nodes, parent, node_idx.0, new_child.0);
        }

        node_idx
    }

    fn split_leaf(&mut self, old_idx: LeafIdx) -> LeafIdx {
        // This function splits a full leaf node in the middle, into 2 new nodes.
        // The result is two nodes - old_leaf with items 0..N/2 and new_leaf with items N/2..N.

        let old_height = self.height;
        // TODO: This doesn't currently use the pool of leaves that we have so carefully prepared.
        // It would be good to fix this, but it currently never actually happens in any of the
        // benchmarking data.
        let new_leaf_idx = self.leaves.len(); // Weird instruction order for borrowck.
        let mut old_leaf = &mut self.leaves[old_idx.0];
        // debug_assert!(old_leaf.is_full());
        debug_assert!(!old_leaf.has_space(2));

        // let parent = old_leaf.parent;
        let split_lv = old_leaf.bounds[LEAF_SPLIT_POINT];

        let parent = if old_height == 0 {
            // Insert this leaf into a new root node. This has to be the first node.
            let lower_bound = old_leaf.bounds[0];
            let parent = self.create_new_root_node(
                                                    lower_bound, old_idx.0,
                                                    split_lv, new_leaf_idx);
            old_leaf = &mut self.leaves[old_idx.0];
            debug_assert_eq!(parent, NodeIdx(0));
            // let parent = NodeIdx(self.nodes.len());
            old_leaf.parent = NodeIdx(0);
            // debug_assert_eq!(old_leaf.parent, NodeIdx(0)); // Ok because its the default.
            // old_leaf.parent = NodeIdx(0); // Could just default nodes to have a parent of 0.

            NodeIdx(0)
        } else {
            let mut parent = old_leaf.parent;
            // The parent may change by calling insert_into_node - since the node we're inserting
            // into may split off.

            parent = self.insert_into_node(parent, (split_lv, new_leaf_idx), old_idx.0, true);
            old_leaf = &mut self.leaves[old_idx.0]; // borrowck.
            parent
        };

        // The old leaf must be full before we split it.
        // debug_assert!(old_leaf.data.last().unwrap().is_some());

        let mut new_leaf = IndexLeaf {
            bounds: [usize::MAX; LEAF_CHILDREN],
            children: [V::default(); LEAF_CHILDREN],
            // upper_bound: old_leaf.upper_bound,
            next_leaf: old_leaf.next_leaf,
            parent,
        };

        // We'll steal the second half of the items in OLD_LEAF.
        // Could use ptr::copy_nonoverlapping but this is safe, and they compile to the same code.
        new_leaf.children[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.children[LEAF_SPLIT_POINT..]);
        new_leaf.bounds[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.bounds[LEAF_SPLIT_POINT..]);

        // The old leaf's new bound is the first copied item's position.
        // old_leaf.upper_bound = split_lv;
        old_leaf.bounds[LEAF_SPLIT_POINT..].fill(usize::MAX);

        // Ignore any danging children in release mode. They don't matter.
        if cfg!(debug_assertions) {
            // This behaviour shouldn't be depended on... its nice while debugging though.
            old_leaf.children[LEAF_SPLIT_POINT..].fill(V::default());
        }

        // old_leaf.upper_bound = split_lv;
        old_leaf.next_leaf = LeafIdx(new_leaf_idx);

        self.leaves.push(new_leaf);

        LeafIdx(new_leaf_idx)
    }

    fn make_space_in_leaf_for<const SIZE: usize>(&mut self, mut leaf_idx: LeafIdx, mut elem_idx: usize) -> (LeafIdx, usize) {
        assert!(SIZE == 1 || SIZE == 2);

        if !self.leaves[leaf_idx.0].has_space(SIZE) {
            let new_node = self.split_leaf(leaf_idx);

            if elem_idx >= LEAF_SPLIT_POINT {
                // We're inserting into the newly created node.
                leaf_idx = new_node;
                elem_idx -= LEAF_SPLIT_POINT;
            }
        }

        let leaf = &mut self.leaves[leaf_idx.0];

        // Could scan to find the actual length of the children, then only memcpy that many. But
        // memcpy is cheap.
        // Could also memcpy fewer items if we split it - since we know then the max will be
        // LEAF_SPLIT_POINT. But I don't think that'll make any difference.
        leaf.bounds.copy_within(elem_idx..LEAF_CHILDREN - SIZE, elem_idx + SIZE);
        leaf.children.copy_within(elem_idx..LEAF_CHILDREN - SIZE, elem_idx + SIZE);

        (leaf_idx, elem_idx)
    }

    /// This function blindly assumes the item is definitely in the recursive children.
    fn find_lv_in_node(node: &IndexNode, needle: LV) -> usize {
        // TODO: Speed up using SIMD.
        node.children[1..].iter()
            // Looking for the first child which contains the needle.
            .position(|(lv, _)| { needle < *lv })
            .unwrap_or(NODE_CHILDREN - 1)
            // .expect("Invalid search in index node")
    }

    fn find_child_idx_in_node(node: &IndexNode, child: usize) -> usize {
        // TODO: Speed up using SIMD.
        node.children.iter()
            .position(|(_, idx)| { child == *idx })
            .expect("Invalid search in index node")
    }

    fn find_in_leaf(leaf: &IndexLeaf<V>, needle: LV) -> usize {
        // Find the index of the first item where the needle is *not* in the range, and then return
        // the previous item.

        // debug_assert!(leaf.is_last() || needle < leaf.upper_bound, "leaf: {:?} / needle {needle}", leaf);

        // There are much faster ways to write this using SIMD.
        leaf.bounds[1..].iter()
            // We're looking for the first item past the needle.
            .position(|bound| *bound == usize::MAX || needle < *bound)
            .unwrap_or(LEAF_CHILDREN - 1)
    }


    #[inline]
    fn leaf_upper_bound(&self, leaf: &IndexLeaf<V>) -> LV {
        Self::leaf_upper_bound_2(&self.leaves, leaf)
    }

    #[inline]
    fn leaf_upper_bound_2(leaves: &Vec<IndexLeaf<V>>, leaf: &IndexLeaf<V>) -> LV {
        if leaf.is_last() {
            usize::MAX
        } else {
            leaves[leaf.next_leaf.0].bounds[0]
        }
    }

    fn check_cursor_at(&self, cursor: IndexCursor, lv: LV, at_end: bool) {
        assert!(cfg!(debug_assertions));
        let leaf = &self.leaves[cursor.leaf_idx.0];
        let lower_bound = leaf.bounds[cursor.elem_idx];

        let next = cursor.elem_idx + 1;
        let upper_bound = if next < LEAF_CHILDREN && leaf.bounds[next] != usize::MAX {
            leaf.bounds[next]
        } else {
            self.leaf_upper_bound(leaf)
        };
        assert!(lv >= lower_bound);

        if at_end {
            assert_eq!(lv, upper_bound);
        } else {
            assert!(lv < upper_bound, "Cursor is not within expected bound. Expect {lv} / upper_bound {upper_bound}");
        }
    }

    fn cursor_to_next(&self, cursor: &mut IndexCursor) {
        let leaf = &self.leaves[cursor.leaf_idx.0];
        let next_idx = cursor.elem_idx + 1;
        if next_idx >= LEAF_CHILDREN || leaf.bounds[next_idx] == usize::MAX {
            cursor.elem_idx = 0;
            cursor.leaf_idx = leaf.next_leaf;
        } else {
            cursor.elem_idx += 1;
        }
    }

    /// Generate a cursor which points at the specified LV.
    fn cursor_at(&self, lv: LV) -> IndexCursor {
        debug_assert!(lv < usize::MAX);

        let (cursor_lv, cursor) = self.cursor.get();
        if cursor_lv == lv {
            // println!("1");
            // println!("HIT");
            if cfg!(debug_assertions) {
                self.check_cursor_at(cursor, lv, false);
            }

            return cursor;
        }

        let leaf = &self[cursor.leaf_idx];
        // TODO: Consider caching the upper bound of the subsequent element in the cursor.

        // This is correct, but doesn't improve performance.
        // if lv >= leaf.bounds[cursor.elem_idx] {
        //     let next_elem = cursor.elem_idx + 1;
        //     let upper_bound = if next_elem >= LEAF_CHILDREN || leaf.bounds[next_elem] == usize::MAX {
        //         self.leaf_upper_bound(leaf)
        //     } else {
        //         leaf.bounds[next_elem]
        //     };
        //     if lv < upper_bound {
        //         return cursor;
        //     }
        // }

        if lv >= leaf.bounds[0] {
            // There are 3 cases:
            // - The lv is less than the bound (or this is the last node)
            // - The lv is exactly the same as the upper bound. Use the start of the next leaf
            // - Or the LV is something else. Scan normally.

            // TODO: Take advantage of elem_idx in the cursor.
            let upper_bound = self.leaf_upper_bound(leaf);
            // let rel = self.upper_bound(leaf).map(|bound| lv.cmp(&bound)).unwrap_or(Ordering::Less);

            if lv < upper_bound { // || end_ok && lv == upper_bound
                // println!("2");
                // println!("...");
                return IndexCursor {
                    leaf_idx: cursor.leaf_idx,
                    elem_idx: Self::find_in_leaf(leaf, lv),
                };
            } else if lv == upper_bound {
                // println!("3");
                // println!("...");
                return IndexCursor {
                    leaf_idx: leaf.next_leaf,
                    elem_idx: 0, // Has to be.
                };
            }
        }

        // println!("MISS");

        // Make a cursor by descending from the root.
        let mut idx = self.root;
        for _h in 0..self.height {
            let n = &self.nodes[idx];
            let slot = Self::find_lv_in_node(n, lv);
            idx = n.children[slot].1;
        }

        // dbg!(&self, lv, idx);

        // Now idx will point to the leaf node. Search there.
        // println!("4");
        IndexCursor {
            leaf_idx: LeafIdx(idx),
            elem_idx: Self::find_in_leaf(&self.leaves[idx], lv),
        }
    }

    /// Get the entry at the specified offset. This will return the largest run of values which
    /// contains the specified index.
    pub fn get_entry(&self, lv: LV) -> RleDRun<V> {
        let cursor = self.cursor_at(lv);

        if cfg!(debug_assertions) {
            self.check_cursor_at(cursor, lv, false);
        }

        self.cursor.set((lv, cursor));

        let leaf = &self.leaves[cursor.leaf_idx.0];
        let val = leaf.children[cursor.elem_idx];
        let lower_bound = leaf.bounds[cursor.elem_idx];

        let next_elem = cursor.elem_idx + 1;
        let upper_bound = if next_elem >= LEAF_CHILDREN || leaf.bounds[next_elem] == usize::MAX {
            self.leaf_upper_bound(leaf)
        } else {
            leaf.bounds[next_elem]
        };
        debug_assert!(lv >= lower_bound && lv < upper_bound);

        RleDRun {
            start: lower_bound,
            end: upper_bound,
            val
        }
    }

    /// After the first item in a leaf has been modified, we need to walk up the node tree to update
    /// the start LV values.
    fn recursively_update_nodes(nodes: &mut Vec<IndexNode>, mut node_idx: NodeIdx, mut child: usize, new_start: LV) {
        while node_idx.0 != usize::MAX {
            let node = &mut nodes[node_idx.0];
            let child_idx = Self::find_child_idx_in_node(node, child);
            node.children[child_idx].0 = new_start;
            if child_idx != 0 {
                // We're done here. This is the most likely case.
                break;
            }

            // Otherwise continue up the tree until we hit the root.
            child = node_idx.0;
            node_idx = node.parent;
        }
    }

    #[inline]
    fn get_leaf_and_bound(&mut self, idx: LeafIdx) -> (&mut IndexLeaf<V>, LV) {
        Self::get_leaf_and_bound_2(&mut self.leaves, idx)
    }

    fn get_leaf_and_bound_2(leaves: &mut Vec<IndexLeaf<V>>, idx: LeafIdx) -> (&mut IndexLeaf<V>, LV) {
        let leaf = &leaves[idx.0];
        let upper_bound = Self::leaf_upper_bound_2(leaves, leaf);
        (&mut leaves[idx.0], upper_bound)
    }


    /// Returns true if we need to keep trimming stuff after this leaf.
    fn trim_leaf_end(&mut self, leaf_idx: LeafIdx, elem_idx: usize, end: LV) -> bool {
        debug_assert!(elem_idx >= 1);
        // debug_assert!(elem_idx < LEAF_CHILDREN);
        // let leaf = &mut self.leaves[leaf_idx.0];
        let (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
        // dbg!(leaf_idx, elem_idx, end, leaf_upper_bound, &leaf);
        // debug_assert!(end > leaf.bounds[elem_idx]); // This element will not be removed.

        if cfg!(debug_assertions) {
            // Check the bounds
            let mut prev = leaf.bounds[0];
            for &b in &leaf.bounds[1..elem_idx] {
                if b != usize::MAX {
                    assert!(b > prev, "Bounds does not monotonically increase b={:?}", &leaf.bounds);
                }
                prev = b;
            }
        }

        if elem_idx >= LEAF_CHILDREN || leaf.bounds[elem_idx] == usize::MAX {
            // The cat is already out of the bag. Continue trimming after this leaf.
            return end > leaf_upper_bound;
        }

        // This function wouldn't be called if we had nothing to do. (Though if this were the
        // case, we could return immediately).
        // debug_assert!(leaf.bounds[elem_idx] < end);

        let mut del_to = elem_idx;
        // let mut last_idx = i;

        // let mut stop_here = false;

        loop {
            // The bounds of element i.
            let next = del_to + 1;
            let mut b = if next > LEAF_CHILDREN {
                break;
            } else if next == LEAF_CHILDREN {
                leaf_upper_bound
            } else {
                leaf.bounds[next]
            };
            // Which may be usize::MAX.

            // Ugh this is so gross. If we hit the last in-use item, the bound is
            // leaf_upper_bound and stop after this one.
            if b == usize::MAX {
                b = leaf_upper_bound;
            }

            // if b == usize::MAX { del_to = LEAF_CHILDREN; break; }

            match end.cmp(&b) {
                Ordering::Less => {
                    // println!("Trim {del_to} to {end}");
                    // Trim the current item and stop here.
                    // let b = b.min(leaf_upper_bound);
                    debug_assert!(leaf.bounds[del_to] < end);
                    leaf.children[del_to] = leaf.children[del_to].at_offset(end - leaf.bounds[del_to]);
                    leaf.bounds[del_to] = end;
                    // stop_here = true;
                    break;
                }
                Ordering::Equal => {
                    // The current item is the last item to delete.
                    del_to += 1;
                    break;
                }
                Ordering::Greater => {
                    // Keep scanning.
                    del_to += 1;
                }
            }

            // Bleh!
            if next < LEAF_CHILDREN && leaf.bounds[next] == usize::MAX { break; }
        }

        if del_to >= LEAF_CHILDREN || leaf.bounds[del_to] == usize::MAX {
            // Delete the rest of this leaf and bubble up.
            leaf.bounds[elem_idx..].fill(usize::MAX);
            end > leaf_upper_bound
        } else {
            let trimmed_items = del_to - elem_idx;

            if trimmed_items >= 1 {
                // println!("trim {elem_idx} <- {del_to}..");

                // Hold onto your hats, its time to delete some items.
                leaf.remove_children(elem_idx..del_to);
            }
            false
        }
    }

    fn upper_bound_scan(&self, mut idx: usize, mut height: usize) -> usize {
        while height > 0 {
            // Descend to the last child of this item.
            let node = &self.nodes[idx];

            debug_assert!(node.children[0].1 != usize::MAX, "Node is empty. idx: {idx}");

            let last_child_idx = node.children.iter()
                .rfind(|(_, idx)| *idx != usize::MAX)
                .expect("Invalid state: Node is empty")
                .1;

            height -= 1;
            idx = last_child_idx;
        }

        // idx is now pointing to a leaf.
        self.leaf_upper_bound(&self.leaves[idx])
    }

    // fn discard_leaf_internal(leaves: &mut Vec<IndexLeaf<V>>, leaf_pool_head: &mut LeafIdx, leaf_idx: LeafIdx) {
    //     let leaf = &mut leaves[leaf_idx.0];
    //     leaf.next_leaf = *leaf_pool_head;
    //     *leaf_pool_head = leaf_idx;
    // }

    // fn discard_leaf(&mut self, leaf_idx: LeafIdx) {
    //     // println!("Discard leaf {:?}", leaf_idx);
    //
    //     // Self::discard_leaf_internal(&mut self.leaves, &mut self.free_leaf_pool_head, leaf_idx);
    //     let leaf = &mut self.leaves[leaf_idx.0];
    //     leaf.next_leaf = self.free_leaf_pool_head;
    //     self.free_leaf_pool_head = leaf_idx;
    //
    //     if cfg!(debug_assertions) {
    //         // Make sure discarded leaves aren't added multiple times to the discard queue.
    //         assert_ne!(leaf.parent, NodeIdx(0xfefe));
    //         leaf.parent = NodeIdx(0xfefe);
    //     }
    // }

    // fn discard_node(&mut self, idx: usize, height: usize) {
    //     if height == 0 {
    //         self.discard_leaf(LeafIdx(idx));
    //     } else {
    //         // println!("DISCARD NODE {idx}");
    //         // Move it to the free list.
    //         let node = &mut self.nodes[idx];
    //         node.parent = self.free_node_pool_head;
    //         self.free_node_pool_head = NodeIdx(idx);
    //
    //         let old_children = mem::replace(&mut node.children, [EMPTY_NODE_CHILD; NODE_CHILDREN]);
    //
    //         for (_, child_idx) in old_children {
    //             if child_idx == usize::MAX { break; }
    //             self.discard_node(child_idx, height - 1);
    //         }
    //     }
    // }
    //
    // fn remove_and_queue_node_children(&mut self, node_idx: NodeIdx, child_range: Range<usize>, _height: usize) {
    //     // This is horrible.
    //     // for i in child_range.clone() {
    //     //     // TODO: Benchmark this against just copying out the children we care about.
    //     //     let child_idx = self.nodes[node_idx.0].children[i].1; // boooo.
    //     //     self.discard_node(child_idx, height - 1);
    //     // }
    //
    //     // Bleh. I want to do this but the borrowck suuucks.
    //     // for (_, idx) in &node.children[..keep_child_idx] {
    //     //     self.discard_node(*idx, height - 1);
    //     // }
    //
    //     self.nodes[node_idx.0].remove_children(child_range);
    // }

    fn trim_node_start(&mut self, mut idx: usize, end: LV, mut height: usize) -> LeafIdx {
        while height > 0 {
            let mut node = &mut self.nodes[idx];

            if end > node.children[0].0 {
                let keep_child_idx = Self::find_lv_in_node(node, end);

                if cfg!(debug_assertions) {
                    let i = node.children[keep_child_idx].1;
                    debug_assert!(self.upper_bound_scan(i, height - 1) > end);
                    node = &mut self.nodes[idx];
                }

                if keep_child_idx >= 1 {
                    // Remove children and move the rest to the start of the array.
                    node.remove_children(0..keep_child_idx);
                }

                node.children[0].0 = end;
                // dbg!(height, end, &node.children, keep_child_idx, node.children[keep_child_idx].1);
            }
            idx = node.children[0].1;

            height -= 1;
        }

        // Ok, now drop the first however many items from the leaf.
        let leaf = &mut self.leaves[idx];
        let keep_elem_idx = Self::find_in_leaf(leaf, end);
        if keep_elem_idx >= 1 {
            leaf.remove_children(0..keep_elem_idx);
        }
        leaf.children[0] = leaf.children[0].at_offset(end - leaf.bounds[0]);
        leaf.bounds[0] = end;

        if cfg!(debug_assertions) {
            let leaf = &self.leaves[idx];
            let leaf_upper_bound = self.leaf_upper_bound(leaf);
            assert!(leaf_upper_bound >= end);
        }

        LeafIdx(idx)
    }

    /// Change the upper bound of the child of this node to end.
    fn trim_node_end_after_child(&mut self, node_idx: NodeIdx, child: usize, end: LV, height: usize) -> LeafIdx {
        debug_assert!(height >= 1);

        // We're going to keep at least 1 child, so this node (and its recursive parents) won't be
        // deleted.
        let mut node = &mut self.nodes[node_idx.0];
        let idx = Self::find_child_idx_in_node(node, child);

        let del_start = idx + 1;

        if cfg!(debug_assertions) {
            let child_idx = node.children[idx].1;
            let up = self.upper_bound_scan(child_idx, height - 1);
            assert!(end > up);
            node = &mut self.nodes[node_idx.0];
            if del_start < NODE_CHILDREN && node.children[del_start].1 != usize::MAX {
                // assert_eq!(node.children[del_start].0, up);
                assert!(end > up);
            }
        }
        // debug_assert!(del_start >= NODE_CHILDREN || end >= node.children[del_start].0,
        //     "del_start: {del_start} / end: {end}"
        // );

        for i in del_start..NODE_CHILDREN {
            let (_lower_bound, child_idx) = node.children[i];

            // if idx == usize::MAX { i = NODE_CHILDREN; break; }
            if child_idx == usize::MAX { break; }

            // This is a little bit inefficient. It might be better to search from the end, or
            // binary search or something. But given how rarely this will all run, I think its ok.
            let upper_bound = if i + 1 < NODE_CHILDREN && node.children[i + 1].1 != usize::MAX {
                // This is a shortcut.

                if cfg!(debug_assertions) {
                    let n = &self.nodes[node_idx.0];
                    debug_assert_eq!(n.children[i + 1].0, self.upper_bound_scan(child_idx, height - 1));
                    node = &mut self.nodes[node_idx.0]; // borrowck.
                }

                node.children[i + 1].0
            } else {
                self.upper_bound_scan(child_idx, height - 1)
            };

            if end < upper_bound {
                // end < upper_bound. Trim the start of this child.
                node = &mut self.nodes[node_idx.0]; // borrowck.
                node.children[i].0 = end; // Update the lower bound of this child.

                let del_end = i;
                if del_end > del_start {
                    // Delete skipped over elements.
                    // self.remove_and_queue_node_children(node_idx, del_start..del_end, height);
                    // node = &mut self.nodes[node_idx.0]; // borrowck.

                    // These items will already have been discarded by discard_node, below.
                    node.remove_children(del_start..del_end);
                }

                return self.trim_node_start(child_idx, end, height - 1);
            } else {
                // Remove this child.
                // self.discard_node(child_idx, height - 1);
            }

            // Borrowck.
            node = &mut self.nodes[node_idx.0];
        }

        node.children[del_start..].fill(EMPTY_NODE_CHILD);

        // Recurse up.
        debug_assert!(node.parent.0 != usize::MAX, "Invalid bounds");
        let parent = node.parent;
        self.trim_node_end_after_child(parent, node_idx.0, end, height + 1)

        // if i == NODE_CHILDREN {
        //     debug_assert!(node.parent.0 != usize::MAX, "Invalid bounds");
        //     self.trim_node_end_after_child(node.parent, node_idx.0, end, height + 1)
        // }
    }

    /// This method clears everything out of the way for the specified element, to set its
    /// upper bound correctly.
    fn extend_upper_range(&mut self, leaf_idx: LeafIdx, elem_idx: usize, end: LV) {
        // This may need to do a lot of work:
        // - The leaf we're currently inside of needs to be trimmed, from elem_idx onwards
        // - If we continue, the parent leaf needs to be trimmed, and its parent and so on. This may
        //   cause some leaves and nodes to be discarded entirely.
        // - Then some nodes and a leaf may need the first few elements removed.

        // We'll always call this with the "next" elem_idx. So the leaf thats being trimmed will
        // never itself be removed.
        debug_assert!(elem_idx >= 1);

        // First, trim the end of this leaf if we can.
        if !self.trim_leaf_end(leaf_idx, elem_idx, end) || self.height == 0 { return; }

        let parent = self.leaves[leaf_idx.0].parent;
        debug_assert!(parent.0 != usize::MAX);

        let new_next_leaf = self.trim_node_end_after_child(parent, leaf_idx.0, end, 1);
        self.leaves[leaf_idx.0].next_leaf = new_next_leaf;
    }

    pub fn set_range(&mut self, range: DTRange, data: V) {
        // println!("    SET RANGE {:?} = {:?}", range, data);
        if range.is_empty() { return; }
        let cursor = self.cursor_at(range.start);
        if cfg!(debug_assertions) {
            self.check_cursor_at(cursor, range.start, false);
        }

        // self.cursor.set((range.start, cursor));
        // The cursor may move.
        let (mut cursor, at_end) = self.set_range_internal(cursor, range, data);

        if cfg!(debug_assertions) {
            // println!("check cursor {:?} {}, {}", cursor, range.end, at_end);
            self.check_cursor_at(cursor, range.end, at_end);
        }

        // if hint_fwd {
        if at_end {
            self.cursor_to_next(&mut cursor);
            if cfg!(debug_assertions) {
                self.check_cursor_at(cursor, range.end, false);
            }
        }
        self.cursor.set((range.end, cursor));
    }

    // returns resulting cursor, whether its at the end of the element.
    fn set_range_internal(&mut self, cursor: IndexCursor, range: DTRange, mut data: V) -> (IndexCursor, bool) {
        // Setting a range can involve deleting some number of data items, and inserting an item.
        //
        // For now, I'm never going to leave a leaf empty just so I can avoid needing to deal with
        // ever deleting nodes.

        let IndexCursor { mut leaf_idx, mut elem_idx } = cursor;
        let DTRange { mut start, mut end } = range;
        // let range = ();
        // let cursor = ();

        // let dbg_upper_bound = self.upper_bound(&self.leaves[leaf_idx.0]);
        // let mut leaf = &mut self.leaves[leaf_idx.0];
        let (mut leaf, mut leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);

        debug_assert!(leaf.bounds[elem_idx] != usize::MAX);
        debug_assert!(start >= leaf.bounds[0] || leaf_idx.0 == 0);
        debug_assert!(start < leaf_upper_bound);
        // debug_assert!(elem_idx == LEAF_CHILDREN - 1 || start < leaf.bounds[elem_idx + 1]);
        // And the range should be < the upper bound.

        // debug_assert!(leaf.is_last() || start < leaf.upper_bound);

        assert!(elem_idx < LEAF_CHILDREN);

        let mut cur_start = leaf.bounds[elem_idx];

        if cur_start == start && elem_idx > 0 {
            // Try and append it to the previous item. This is unnecessary, but should help with
            // perf.
            let prev_idx = elem_idx - 1;
            let prev_start = leaf.bounds[prev_idx];
            if leaf.children[prev_idx].try_append(cur_start - prev_start, &data, end - start) {
                // Ok!
                self.extend_upper_range(leaf_idx, elem_idx, end);

                // Note extend_upper_range might have nuked the current element. Since the stored
                // cursor always points to the *next* element, we'll roll the cursor forward in this
                // case here.
                let leaf = &self.leaves[leaf_idx.0];
                if leaf.bounds[elem_idx] == usize::MAX {
                    // println!("A1");
                    return (IndexCursor { leaf_idx: leaf.next_leaf, elem_idx: 0}, false);
                } else {
                    // println!("A2");
                    // self.check_cursor_at(cursor, range.end, false);
                    return (cursor, false);
                }
            }
        }

        // TODO: Probably worth a short-circuit check here to see if the value even changed.

        let mut cur_end = if elem_idx >= LEAF_CHILDREN - 1 {
            leaf_upper_bound
        } else {
            // This is pretty gnarly.
            let b = leaf.bounds[elem_idx + 1];
            if b == usize::MAX { leaf_upper_bound } else { b }
        };

        // If we can append the item to the current item, do that.
        if cur_start < start {
            let mut d = leaf.children[elem_idx];
            if d.try_append(start - cur_start, &data, end - start) {
                data = d;
                start = cur_start;
            }
        }

        let mut end_is_end = true;

        if end < cur_end {
            // Try to append the end of the current element.
            if data.try_append(end - start, &leaf.children[elem_idx].at_offset(end - cur_start), cur_end - end) {
                // Nice. We'll handle this in the special case below.
                end = cur_end;
                end_is_end = false;
            } else {
                // In this case, the item is replacing a prefix of the target slot. We'll just hardcode
                // these cases, since otherwise we need to deal with remainders below and thats a pain.
                if cur_start < start {
                    // We need to "splice in" this item. Eg, x -> xyx. This will result in 2
                    // inserted items.

                    // The resulting behaviour should be that:
                    // b1 (x) b2  ---->  b1 (x) start (y) range.end (x) b2

                    // The item at elem_idx is the start of the item we're splitting. Leave it
                    // alone. We'll replace elem_idx + 1 with data and elem_idx + 2 with remainder.

                    (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<2>(leaf_idx, elem_idx);
                    let leaf = &mut self.leaves[leaf_idx.0];

                    assert!(elem_idx + 2 < LEAF_CHILDREN);
                    leaf.bounds[elem_idx + 1] = start;
                    leaf.children[elem_idx + 1] = data;
                    leaf.bounds[elem_idx + 2] = end;
                    // This will be a no-op for many types of data because of the memcpy.
                    leaf.children[elem_idx + 2] = leaf.children[elem_idx].at_offset(end - cur_start);

                    // We modified elem_idx +1 and +2, so we can't have modified index 0. No parent update.
                    // println!("b");
                    // self.check_cursor_at(IndexCursor { leaf_idx, elem_idx: elem_idx + 1 }, range.end, true);
                    return (IndexCursor { leaf_idx, elem_idx: elem_idx + 1 }, true);
                } else {
                    // Preserve the end of this item. Eg, x -> yx.
                    debug_assert!(cur_start == start);
                    debug_assert!(end < cur_end);

                    (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                    let leaf = &mut self.leaves[leaf_idx.0];

                    // This should be true, but V doesn't impl Eq.
                    // debug_assert_eq!(leaf.children[elem_idx + 1], leaf.children[elem_idx]);

                    debug_assert_eq!(leaf.bounds[elem_idx], start);
                    assert!(elem_idx + 1 < LEAF_CHILDREN);
                    leaf.children[elem_idx] = data;
                    leaf.bounds[elem_idx + 1] = end;
                    leaf.children[elem_idx + 1] = leaf.children[elem_idx + 1].at_offset(end - start);

                    // Since start == lower bound, the parents won't need updating.
                    // println!("c");
                    return (IndexCursor { leaf_idx, elem_idx }, true);
                }
            }
        }

        if end == cur_end {
            // Special case. Might not be worth it.
            if start == cur_start {
                // Nuke the existing item.
                leaf.children[elem_idx] = data;

                // Since start == lower bound, the parents don't need updating.
            } else {
                // Preserve the start of the item. x -> xy.
                debug_assert!(start > cur_start);

                (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                let leaf = &mut self.leaves[leaf_idx.0];

                elem_idx += 1;
                assert!(elem_idx < LEAF_CHILDREN);
                leaf.children[elem_idx] = data;
                leaf.bounds[elem_idx] = start;
                // We didn't modify [0], so no parent update.
            }
            // println!("d");
            return (IndexCursor { leaf_idx, elem_idx }, end_is_end);
        }

        // This element overlaps with some other elements.
        debug_assert!(end > cur_end);
        debug_assert!(start < cur_end);

        if cur_start < start {
            // Trim the current item alone and modify the next item.
            // If we get here then: cur_start < start < cur_end < end.
            debug_assert!(cur_start < start && start < cur_end && cur_end < end);

            elem_idx += 1;

            // Alternately, we could just use make_space_in_leaf here - though it would need to be
            // adjusted to allow the elem_idx to be = LEAF_CHILDREN.
            if elem_idx >= LEAF_CHILDREN || leaf.bounds[elem_idx] == usize::MAX {
                // This is the end of the leaf node.
                // leaf.upper_bound = start;

                // if elem_idx < LEAF_CHILDREN {
                //     // Just insert the new item here, at the end of the current leaf.
                //     leaf.children[elem_idx] = data;
                //     leaf.bounds[elem_idx] = start;
                //     self.extend_upper_range(leaf_idx, elem_idx + 1, end);
                //     return (IndexCursor { leaf_idx, elem_idx }, end_is_end)
                // } else if leaf.is_last() {
                if leaf.is_last() {
                    // This should never happen because we pre-fill the entire usize range with
                    // a default value.
                    unreachable!("Unable to insert past the end of the tree");
                } else {

                    // We've trimmed this leaf node. Roll the cursor to the next item.
                    leaf_idx = leaf.next_leaf;
                    (leaf, leaf_upper_bound) = Self::get_leaf_and_bound_2(&mut self.leaves, leaf_idx);
                    elem_idx = 0;

                    // We're going to replace the leaf's starting item.
                    let parent = leaf.parent;
                    Self::recursively_update_nodes(&mut self.nodes, parent, leaf_idx.0, start);
                }
            }

            debug_assert_eq!(leaf.bounds[elem_idx], cur_end);
            debug_assert!(start < leaf.bounds[elem_idx]);

            // debug_assert!(start < leaf.bounds[elem_idx]);

            // Right now leaf.children[elem_idx] contains an item from cur_end > start.

            // We've moved forward. Try and append the existing item to data.
            cur_start = cur_end;
            cur_end = if elem_idx >= LEAF_CHILDREN - 1 {
                leaf_upper_bound
            } else {
                let b = leaf.bounds[elem_idx + 1];
                if b == usize::MAX { leaf_upper_bound } else { b }
            };

            leaf.bounds[elem_idx] = start;

            // debug_assert!(cur_start < end);

            // Current constraints here:
            // start < cur_start < cur_end
            //         cur_start < end
            debug_assert!(start < cur_start && cur_start < cur_end);
            debug_assert!(cur_start < end);

            if end < cur_end {
                // Try to prepend the new item to the start of the existing item.
                if data.try_append(cur_start - start, &leaf.children[elem_idx], cur_end - cur_start) {
                    // Ok!
                    leaf.children[elem_idx] = data;
                    // println!("e");
                    return (IndexCursor { leaf_idx, elem_idx }, false);
                } else {
                    (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                    leaf = &mut self.leaves[leaf_idx.0];
                    leaf.children[elem_idx] = data;
                    leaf.bounds[elem_idx + 1] = end;
                    leaf.children[elem_idx + 1] = leaf.children[elem_idx + 1].at_offset(end - cur_start);
                    // println!("f");
                    return (IndexCursor { leaf_idx, elem_idx }, end_is_end);
                }
            } else if end == cur_end {
                // This item fits perfectly.
                leaf.children[elem_idx] = data;
                // println!("g");
                return (IndexCursor { leaf_idx, elem_idx }, end_is_end);
            }

            cur_start = start; // Since we've pushed down the item bounds.
        }

        debug_assert!(end > cur_end);
        debug_assert_eq!(cur_start, start);

        // We don't care about the current element at all. Just overwrite it and extend
        // the bounds.
        leaf.children[elem_idx] = data;
        self.extend_upper_range(leaf_idx, elem_idx + 1, end);

        // println!("h");
        (IndexCursor { leaf_idx, elem_idx }, end_is_end)
    }

    fn first_leaf(&self) -> LeafIdx {
        if cfg!(debug_assertions) {
            // dbg!(&self);
            let mut idx = self.root;
            for _ in 0..self.height {
                idx = self.nodes[idx].children[0].1;
            }
            debug_assert_eq!(idx, 0);
        }
        LeafIdx(0)
    }

    pub fn count_items(&self) -> usize {
        let mut count = 0;
        let mut leaf = &self[self.first_leaf()];
        loop {
            // SIMD should make this fast.
            count += leaf.bounds.iter().filter(|b| **b != usize::MAX).count();

            // There is always at least one leaf.
            if leaf.is_last() { break; }
            else {
                leaf = &self[leaf.next_leaf];
            }
        }

        count
    }

    // /// returns number of internal nodes, leaves.
    // pub fn count_obj_pool(&self) -> (usize, usize) {
    //     let mut nodes = 0;
    //     let mut leaves = 0;
    //
    //     let mut idx = self.free_node_pool_head;
    //     while idx.0 != usize::MAX {
    //         nodes += 1;
    //         idx = self.nodes[idx.0].parent;
    //     }
    //     let mut idx = self.free_leaf_pool_head;
    //     while idx.0 != usize::MAX {
    //         leaves += 1;
    //         idx = self.leaves[idx.0].next_leaf;
    //     }
    //
    //     (nodes, leaves)
    // }

    /// Iterate over the contents of the index. Note the index tree may contain extra entries
    /// for items within the range, with a value of V::default.
    pub fn iter(&self) -> IndexTreeIter<V> {
        IndexTreeIter {
            tree: self,
            leaf_idx: self.first_leaf(),
            // leaf: &self.leaves[self.first_leaf()],
            elem_idx: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<RleDRun<V>> {
        self.iter().collect::<Vec<_>>()
    }

    // Returns the next leaf pointer.
    fn dbg_check_walk(&self, idx: usize, height: usize, expect_start: Option<LV>, expect_parent: NodeIdx, mut expect_next_leaf: LeafIdx) -> LeafIdx {
        if height != 0 {
            // Visiting a node.
            assert!(idx < self.nodes.len());
            let node = &self.nodes[idx];

            // dbg!(&self.nodes, self.root, self.height, expect_parent);
            assert_eq!(node.parent, expect_parent);

            // The first child must be in use.
            assert_ne!(node.children[0].1, usize::MAX);
            // The first child must start at expect_start.
            if let Some(expect_start) = expect_start {
                // dbg!(&self.nodes, self.root, self.height);
                assert_eq!(node.children[0].0, expect_start);
            }

            let mut finished = false;
            let mut prev_start = usize::MAX;
            for &(start, child_idx) in &node.children {
                if child_idx == usize::MAX { finished = true; }
                else {
                    assert!(prev_start == usize::MAX || prev_start < start, "prev_start {prev_start} / start {start}");
                    prev_start = start;

                    assert_eq!(finished, false);
                    expect_next_leaf = self.dbg_check_walk(child_idx, height - 1, Some(start), NodeIdx(idx), expect_next_leaf);
                }
            }
            expect_next_leaf
        } else {
            // Visiting a leaf.
            assert_eq!(idx, expect_next_leaf.0);
            assert!(idx < self.leaves.len());
            let leaf = &self.leaves[idx];

            // dbg!(&self, idx);
            assert_eq!(leaf.parent, expect_parent);

            // We check that the first child is in use below.
            if leaf.bounds[0] != usize::MAX {
                if let Some(expect_start) = expect_start {
                    assert_eq!(leaf.bounds[0], expect_start);
                }
            }

            leaf.next_leaf
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_check(&self) {
        // Invariants:
        // - All index markers point to the node which contains the specified item.
        // - Except for the root item, all leaves must have at least 1 data entry.
        // - The "left edge" of items should all have a lower bound of 0
        // - The last leaf node should have an upper bound and node_next of usize::MAX.

        // This code does 2 traversals of the data structure:
        // 1. We walk the leaves by following next_leaf pointers in each leaf node
        // 2. We recursively walk the tree

        // Walk the leaves.
        let mut leaves_visited = 0;
        let mut leaf_idx = self.first_leaf();
        loop {
            let leaf = &self[leaf_idx];
            leaves_visited += 1;

            if leaf_idx == self.first_leaf() {
                // First leaf. This can be empty - but only if the whole data structure is empty.
                if leaf.bounds[0] == usize::MAX {
                    assert!(!leaf.next_leaf.exists());
                }
            } else {
                assert_ne!(leaf.bounds[0], usize::MAX, "Only the first leaf can be empty");
            }

            // Make sure the bounds are all sorted.
            let mut prev = leaf.bounds[0];
            let mut finished = false;
            for &b in &leaf.bounds[1..] {
                if b == usize::MAX {
                    finished = true;
                } else {
                    assert!(b > prev, "Bounds does not monotonically increase b={:?}", &leaf.bounds);
                    // assert!(b < leaf.upper_bound);
                    // assert!(b < self.upper_bound);
                    prev = b;
                    assert!(!finished, "All in-use children must come before all null children");
                }
            }

            if leaf.is_last() { break; }
            else {
                let next_leaf = &self[leaf.next_leaf];
                assert!(next_leaf.bounds[0] > prev);
                // assert_eq!(leaf.upper_bound, next_leaf.bounds[0]);
            }
            leaf_idx = leaf.next_leaf;
        }

        // let mut leaf_pool_size = 0;
        // let mut i = self.free_leaf_pool_head;
        // while i.0 != usize::MAX {
        //     leaf_pool_size += 1;
        //     i = self.leaves[i.0].next_leaf;
        // }
        // assert_eq!(leaves_visited + leaf_pool_size, self.leaves.len());

        if self.height == 0 {
            assert!(self.root < self.leaves.len());
        } else {
            assert!(self.root < self.nodes.len());
        }

        // And walk the tree structure in the nodes
        let last_next = self.dbg_check_walk(self.root, self.height, None, NodeIdx(usize::MAX), self.first_leaf());
        assert!(!last_next.exists());

        let (lv, cursor) = self.cursor.get();
        // self.check_cursor_at(cursor, lv, false);
    }

    #[allow(unused)]
    pub(crate) fn dbg_check_eq_2(&self, other: impl IntoIterator<Item = RleDRun<V>>) {
        self.dbg_check();

        let mut tree_iter = self.iter();
        // let mut expect_iter = expect.into_iter();

        // while let Some(expect_val) = expect_iter.next() {
        let mut actual_remainder = None;
        for mut expect in other.into_iter() {
            loop {
                let mut actual = actual_remainder.take().unwrap_or_else(|| {
                    tree_iter.next().expect("Tree missing item")
                });

                // Skip anything before start.
                if actual.end <= expect.start {
                    continue;
                }

                // Trim the start of actual_next
                if actual.start < expect.start {
                    (_, actual) = split_rle(actual, expect.start - actual.start);
                } else if expect.start < actual.start {
                    panic!("Missing element");
                }

                assert_eq!(actual.start, expect.start);
                let r = DTRange { start: actual.start, end: actual.start + usize::min(actual.len(), expect.len()) };
                assert!(expect.val.eq(&actual.val, usize::min(actual.len(), expect.len())),
                        "at {:?}: expect {:?} != actual {:?} (len={})", r, &expect.val, &actual.val, usize::min(actual.len(), expect.len()));
                // assert_eq!(expect.val, actual.val, "{:?}", &tree_iter);

                if actual.end > expect.end {
                    // We don't need to split it here because that'll happen on the next iteration anyway.
                    actual_remainder = Some(actual);
                    // actual_remainder = Some(split_rle(actual, expect.end - actual.start).1);
                    break;
                } else if actual.end >= expect.end {
                    break;
                } else {
                    // actual.end < expect.end
                    // Keep the rest of expect for the next iteration.
                    (_, expect) = split_rle(expect, actual.end - expect.start);
                    debug_assert_eq!(expect.start, actual.end);
                    // And continue with this expected item.
                }
            }
        }
    }

    #[allow(unused)]
    pub(crate) fn dbg_check_eq<'a>(&self, vals: impl IntoIterator<Item = &'a RleDRun<V>>) where V: 'a {
        self.dbg_check_eq_2(vals.into_iter().copied());
    }

}

#[derive(Debug)]
pub struct IndexTreeIter<'a, V: Copy> {
    tree: &'a IndexTree<V>,
    leaf_idx: LeafIdx,
    // leaf: &'a IndexLeaf<V>,
    elem_idx: usize,
}

impl<'a, V: Copy> Iterator for IndexTreeIter<'a, V> {
    // type Item = (DTRange, V);
    type Item = RleDRun<V>;

    fn next(&mut self) -> Option<Self::Item> {
        // if self.leaf_idx.0 == usize::MAX {
        // debug_assert!(self.elem_idx < LEAF_CHILDREN);
        if self.leaf_idx.0 >= self.tree.leaves.len() || self.elem_idx >= LEAF_CHILDREN { // Avoid a bounds check.
            return None;
        }

        let mut leaf = &self.tree[self.leaf_idx];
        // if self.elem_idx >= LEAF_CHILDREN || leaf.bounds[self.elem_idx] == usize::MAX {
        //     debug_assert!(leaf.is_last());
        //     return None;
        // }

        let data = leaf.children[self.elem_idx].clone();
        let start = leaf.bounds[self.elem_idx];
        if start == usize::MAX {
            // This will happen when the tree is empty.
            debug_assert_eq!(self.elem_idx, 0);
            debug_assert_eq!(self.leaf_idx.0, 0);
            return None;
        }

        self.elem_idx += 1;

        let end = 'block: {
            if self.elem_idx >= LEAF_CHILDREN || leaf.bounds[self.elem_idx] == usize::MAX {
                // Try to move to the next leaf.
                self.leaf_idx = leaf.next_leaf;
                // if self.leaf_idx.0 == usize::MAX {
                if self.leaf_idx.0 >= self.tree.leaves.len() {
                    break 'block usize::MAX;
                }
                self.elem_idx = 0;

                leaf = &self.tree[self.leaf_idx];
                leaf.bounds[0]
            } else {
                leaf.bounds[self.elem_idx]
            }
        };

        Some(RleDRun::new(start..end, data))
    }
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use rand::prelude::SmallRng;
    use rand::{Rng, SeedableRng};
    use content_tree::{ContentTreeRaw, RawPositionMetricsUsize};
    use crate::list_fuzzer_tools::fuzz_multithreaded;
    use super::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum Foo { A, B, C }
    use Foo::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
    struct X(usize);
    impl IndexContent for X {
        fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
            debug_assert!(offset > 0);
            debug_assert!(other_len > 0);
            &self.at_offset(offset) == other
        }

        fn at_offset(&self, offset: usize) -> Self {
            X(self.0 + offset)
        }

        fn eq(&self, other: &Self, _upto_len: usize) -> bool {
            self.0 == other.0
        }
    }

    #[test]
    fn empty_tree_is_empty() {
        let tree = IndexTree::<X>::new();

        tree.dbg_check_eq(&[]);
    }

    #[test]
    fn overlapping_sets() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
        // assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);
        // dbg!(&tree.leaves[0]);
        tree.set_range((5..11).into(), X(200));
        tree.dbg_check_eq(&[RleDRun::new(5..11, X(200))]);

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[
            RleDRun::new(5..10, X(100)),
            RleDRun::new(10..11, X(205)),
        ]);

        tree.set_range((2..50).into(), X(300));
        // dbg!(&tree.leaves);
        tree.dbg_check_eq(&[RleDRun::new(2..50, X(300))]);

    }

    #[test]
    fn split_values() {
        let mut tree = IndexTree::new();
        tree.set_range((10..20).into(), X(100));
        tree.set_range((12..15).into(), X(200));
        tree.dbg_check_eq(&[
            RleDRun::new(10..12, X(100)),
            RleDRun::new(12..15, X(200)),
            RleDRun::new(15..20, X(105)),
        ]);
    }

    #[test]
    fn set_inserts_1() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);

        tree.set_range((5..10).into(), X(200));
        tree.dbg_check_eq(&[RleDRun::new(5..10, X(200))]);

        // dbg!(&tree);
        tree.set_range((15..20).into(), X(300));
        // dbg!(tree.iter().collect::<Vec<_>>());
        tree.dbg_check_eq(&[
            RleDRun::new(5..10, X(200)),
            RleDRun::new(15..20, X(300)),
        ]);

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
    }

    #[test]
    fn set_inserts_2() {
        let mut tree = IndexTree::new();
        tree.set_range((5..10).into(), X(100));
        tree.set_range((1..5).into(), X(200));
        // dbg!(&tree);
        tree.dbg_check_eq(&[
            RleDRun::new(1..5, X(200)),
            RleDRun::new(5..10, X(100)),
        ]);
        dbg!(&tree.leaves[0]);

        tree.set_range((3..8).into(), X(300));
        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
        tree.dbg_check_eq(&[
            RleDRun::new(1..3, X(200)),
            RleDRun::new(3..8, X(300)),
            RleDRun::new(8..10, X(103)),
        ]);
    }

    #[test]
    fn split_leaf() {
        let mut tree = IndexTree::new();
        // Using 10, 20, ... so they don't merge.
        tree.set_range(10.into(), X(100));
        tree.dbg_check();
        tree.set_range(20.into(), X(200));
        tree.set_range(30.into(), X(100));
        tree.set_range(40.into(), X(200));
        tree.dbg_check();
        // dbg!(&tree);
        tree.set_range(50.into(), X(100));
        tree.dbg_check();

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());

        tree.dbg_check_eq(&[
            RleDRun::new(10..11, X(100)),
            RleDRun::new(20..21, X(200)),
            RleDRun::new(30..31, X(100)),
            RleDRun::new(40..41, X(200)),
            RleDRun::new(50..51, X(100)),
        ]);
    }

    #[test]
    fn clear_range() {
        // for i in 2..20 {
        for i in 2..50 {
            eprintln!("i: {i}");
            let mut tree = IndexTree::new();
            for base in 0..i {
                tree.set_range((base*3..base*3+2).into(), X(base + 100));
            }
            // dbg!(tree.iter().collect::<Vec<_>>());

            let ceil = i*3 - 2;
            // dbg!(ceil);
            // dbg!(&tree);
            tree.dbg_check();
            tree.set_range((1..ceil).into(), X(99));
            // dbg!(tree.iter().collect::<Vec<_>>());

            tree.dbg_check_eq(&[
                RleDRun::new(0..1, X(100)),
                RleDRun::new(1..ceil, X(99)),
                RleDRun::new(ceil..ceil+1, X(i - 1 + 100 + 1)),
            ]);
        }
    }

    fn fuzz(seed: u64, verbose: bool) {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree = IndexTree::new();
        // let mut check_tree: Pin<Box<ContentTreeRaw<RleDRun<Option<i32>>, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        let mut check_tree: Pin<Box<ContentTreeRaw<DTRange, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        const START_JUNK: usize = 1_000_000;
        check_tree.replace_range_at_offset(0, (START_JUNK..START_JUNK *2).into());

        for _i in 0..1000 {
            if verbose { println!("i: {}", _i); }
            // This will generate some overlapping ranges sometimes but not too many.
            let val = rng.gen_range(0..100) + 100;
            // let start = rng.gen_range(0..3);
            let start = rng.gen_range(0..1000);
            let len = rng.gen_range(0..100) + 1;
            // let start = rng.gen_range(0..100);
            // let len = rng.gen_range(0..100) + 1;

            // dbg!(&tree, start, len, val);
            // if _i == 19 {
            //     println!("blerp");
            // }

            // if _i == 14 {
            //     dbg!(val, start, len);
            //     dbg!(tree.iter().collect::<Vec<_>>());
            // }
            tree.set_range((start..start+len).into(), X(val));
            // dbg!(&tree);
            tree.dbg_check();

            // dbg!(check_tree.iter().collect::<Vec<_>>());

            check_tree.replace_range_at_offset(start, (val..val+len).into());

            // if _i == 14 {
            //     dbg!(tree.iter().collect::<Vec<_>>());
            //     dbg!(check_tree.iter_with_pos().filter_map(|(pos, r)| {
            //         if r.start >= START_JUNK { return None; }
            //         Some(RleDRun::new(pos..pos+r.len(), X(r.start)))
            //     }).collect::<Vec<_>>());
            // }

            // check_tree.iter
            tree.dbg_check_eq_2(check_tree.iter_with_pos().filter_map(|(pos, r)| {
                if r.start >= START_JUNK { return None; }
                Some(RleDRun::new(pos..pos+r.len(), X(r.start)))
            }));
        }
    }

    #[test]
    fn fuzz_once() {
        fuzz(22, true);
    }

    #[test]
    #[ignore]
    fn tree_fuzz_forever() {
        fuzz_multithreaded(u64::MAX, |seed| {
            if seed % 100 == 0 {
                println!("Iteration {}", seed);
            }
            fuzz(seed, false);
        })
    }
}

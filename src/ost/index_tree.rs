use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::swap;
use std::ops::{Index, IndexMut};
use std::ptr::NonNull;
use rle::{HasLength, MergableSpan, RleDRun, SplitableSpan};
use crate::{DTRange, LV};
use crate::ost::{NODE_CHILDREN, LeafIdx, NodeIdx, LEAF_CHILDREN};
use crate::ost::content_tree::{ContentLeaf, ContentNode, ContentTree};

#[derive(Debug, Clone)]
pub(crate) struct IndexTree<V> {
    leaves: Vec<IndexLeaf<V>>,
    nodes: Vec<IndexNode>,
    // upper_bound: LV,
    height: usize,
    root: usize,
    // cursor: Option<IndexCursor>,
    cursor: IndexCursor,
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

// #[derive(Debug, Clone)]
// struct CachedIndexCursor {
//     leaf: LeafIdx,
//     leaf_range: DTRange,
//
//     // & last index?
// }

// #[derive(Copy, Clone, Eq, PartialEq, Debug)]
// enum IdxMarker {
//     None,
//
//     /// For inserts, we store an index into the leaf node containing this item.
//     Ins(LeafIdx),
//
//     /// For deletes, we name the delete's target.
//     DelFwd(LV),
//     DelBack(LV),
// }


// const EMPTY_LEAF_DATA: (LV, LeafData) = (usize::MAX, LeafData::InsPtr(NonNull::dangling()));

const NODE_SPLIT_POINT: usize = NODE_CHILDREN / 2;
// const LEAF_CHILDREN: usize = LEAF_SIZE - 1;
const LEAF_SPLIT_POINT: usize = LEAF_CHILDREN / 2;

#[derive(Debug, Clone)]
pub struct IndexLeaf<V> {
    /// The bounds is usize::MAX for unused items. The last item has an upper bound equal to the
    /// start bound of the first item in the next leaf. This is also cached in upper_bound.
    bounds: [LV; LEAF_CHILDREN],
    children: [V; LEAF_CHILDREN],
    // /// (start of range, data). Start == usize::MAX for empty entries.
    // children: [(LV, V); LEAF_CHILDREN],

    // upper_bound: LV,
    next_leaf: LeafIdx,
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
        // children: [(usize::MAX, V::default()); LEAF_CHILDREN],
        // upper_bound: usize::MAX, // The bounds of the last item is (functionally) infinity.
        next_leaf: LeafIdx(usize::MAX),
        // parent: NodeIdx(0), // This node won't exist yet - but thats ok.
        parent: NodeIdx(usize::MAX), // This node won't exist yet - but thats ok.
    }
}

// const INITIAL_ROOT_LEAF: IndexLeaf = IndexLeaf {
//     data: [(usize::MAX, V::default()); LEAF_CHILDREN],
//     upper_bound: usize::MAX, // The bounds of the last item is (functionally) infinity.
//     next_leaf: LeafIdx(usize::MAX),
//     parent: NodeIdx(0), // This node won't exist yet - but thats ok.
// };


/// A node child specifies the LV of the (recursive) first element and an index in the data
/// structure.
type NodeChild = (LV, usize);

const EMPTY_NODE_CHILD: NodeChild = (usize::MAX, usize::MAX);

#[derive(Debug, Clone)]
pub struct IndexNode {
    /// Child entries point to either another node or a leaf. We disambiguate using the height.
    /// The named LV is the first LV of the child data.
    ///
    /// Children are (usize::MAX, usize::MAX) if they are unset.
    children: [NodeChild; NODE_CHILDREN],
    parent: NodeIdx,
}

impl<V> IndexLeaf<V> {
    fn is_full(&self) -> bool {
        *self.bounds.last().unwrap() != usize::MAX
    }

    #[inline(always)]
    fn has_space(&self, space_wanted: usize) -> bool {
        if space_wanted == 0 { return true; }
        self.bounds[LEAF_CHILDREN - space_wanted] == usize::MAX
    }

    // fn bound_for_idx(&self, idx: usize) -> usize {
    //     let next_idx = idx + 1;
    //     if next_idx >= LEAF_CHILDREN {
    //         self.upper_bound
    //     } else {
    //         let bound = self.bounds[next_idx];
    //         // If bound == usize::MAX, this item isn't used. Default to bound.
    //         if bound == usize::MAX { self.upper_bound } else { bound }
    //     }
    // }

    fn is_last(&self) -> bool { !self.next_leaf.exists() }

    fn next<'a>(&self, leaves: &'a [IndexLeaf<V>]) -> Option<&'a IndexLeaf<V>> {
        if self.is_last() { None }
        else { Some(&leaves[self.next_leaf.0]) }
    }

    fn next_mut<'a>(&self, leaves: &'a mut [IndexLeaf<V>]) -> Option<&'a mut IndexLeaf<V>> {
        if self.is_last() { None }
        else { Some(&mut leaves[self.next_leaf.0]) }
    }
}

impl IndexNode {
    fn is_full(&self) -> bool {
        self.children.last().unwrap().1 != usize::MAX
    }
}

impl<V: Default + IndexContent> Default for IndexTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Index<LeafIdx> for IndexTree<V> {
    type Output = IndexLeaf<V>;

    fn index(&self, index: LeafIdx) -> &Self::Output {
        &self.leaves[index.0]
    }
}
impl<V> IndexMut<LeafIdx> for IndexTree<V> {
    fn index_mut(&mut self, index: LeafIdx) -> &mut Self::Output {
        &mut self.leaves[index.0]
    }
}
impl<V> Index<NodeIdx> for IndexTree<V> {
    type Output = IndexNode;

    fn index(&self, index: NodeIdx) -> &Self::Output {
        &self.nodes[index.0]
    }
}
impl<V> IndexMut<NodeIdx> for IndexTree<V> {
    fn index_mut(&mut self, index: NodeIdx) -> &mut Self::Output {
        &mut self.nodes[index.0]
    }
}


/// I'm not sure if this is a good idea. The index stores its base positions separate from the
/// content.
///
/// Essentially index content must splitable & mergable be such that .truncate() / .append() are
/// no-ops. .can_append will also need the base & offset.
pub trait IndexContent: Debug + Copy + Eq {
    /// Try to append other to self. If possible, self is modified (if necessary) and true is
    /// returned.
    fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
    // fn can_append(&self, offset: usize, other: &Self) -> bool {
        debug_assert!(offset > 0);
        debug_assert!(other_len > 0);
        &self.at_offset(offset) == other
    }

    fn at_offset(&self, offset: usize) -> Self;

    // #[inline(always)]
    // fn prepend(&mut self, other: Self) {
    //     *self = other;
    // }
    //
    // // #[inline(always)]
    // fn append_at(&mut self, _offset: usize, _other: Self) {}
}


impl<V: Default + IndexContent> IndexTree<V> {
    pub fn new() -> Self {
        Self {
            leaves: vec![initial_root_leaf()],
            nodes: vec![],
            // upper_bound: 0,
            height: 0,
            root: 0,
            cursor: IndexCursor::default(),
        }
    }

    pub fn clear(&mut self) {
        self.leaves.clear();
        self.nodes.clear();
        self.height = 0;
        self.root = 0;
        self.cursor = IndexCursor::default();
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
                elem_idx -= NODE_SPLIT_POINT;
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
        // node.children.iter()
        //     // Looking for the first child which contains the needle.
        //     .position(|(lv, _)| { needle >= *lv })
        //     .expect("Invalid search in index node")
    }

    fn find_child_idx_in_node(node: &IndexNode, needle: LV) -> usize {
        // TODO: Speed up using SIMD.
        node.children.iter()
            .position(|(_, idx)| { needle == *idx })
            .expect("Invalid search in index node")
    }

    fn find_in_leaf(leaf: &IndexLeaf<V>, needle: LV) -> usize {
        // Find the index of the first item where the needle is *not* in the range, and then return
        // the previous item.

        // debug_assert!(leaf.is_last() || needle < leaf.upper_bound, "leaf: {:?} / needle {needle}", leaf);

        // There are much faster ways to write this using SIMD.
        leaf.bounds[1..].iter()
            // We're looking for the first item past the needle.
            // .position(|lv| needle <= *lv)
            .position(|bound| *bound == usize::MAX || needle < *bound)
            .unwrap_or(LEAF_CHILDREN - 1)
    }

    #[inline]
    fn upper_bound(&self, leaf: &IndexLeaf<V>) -> LV {
        if leaf.is_last() {
            usize::MAX
        } else {
            self[leaf.next_leaf].bounds[0]
        }
    }

    /// Generate a cursor which points at the specified LV.
    fn cursor_at(&self, lv: LV) -> IndexCursor {
        debug_assert!(lv < usize::MAX);
        let leaf = &self[self.cursor.leaf_idx];
        // TODO: Consider caching the upper bound of the subsequent element in the cursor.

        if lv >= leaf.bounds[0] {
            // There are 3 cases:
            // - The lv is less than the bound (or this is the last node)
            // - The lv is exactly the same as the upper bound. Use the start of the next leaf
            // - Or the LV is something else. Scan normally.

            // TODO: Take advantage of elem_idx in the cursor.
            let upper_bound = self.upper_bound(leaf);
            // let rel = self.upper_bound(leaf).map(|bound| lv.cmp(&bound)).unwrap_or(Ordering::Less);
            if lv < upper_bound {
                return IndexCursor {
                    leaf_idx: self.cursor.leaf_idx,
                    elem_idx: Self::find_in_leaf(leaf, lv),
                };
            } else if lv == upper_bound {
                return IndexCursor {
                    leaf_idx: leaf.next_leaf,
                    elem_idx: 0, // Has to be.
                };
            }
        }

        // if self.root == 1 {
        //     println!("asdf");
        // }
        // Make a cursor by descending from the root.
        let mut idx = self.root;
        for _h in 0..self.height {
            let n = &self.nodes[idx];
            let slot = Self::find_lv_in_node(n, lv);
            idx = n.children[slot].1;
        }

        // dbg!(&self, lv, idx);

        // Now idx will point to the leaf node. Search there.
        IndexCursor {
            leaf_idx: LeafIdx(idx),
            elem_idx: Self::find_in_leaf(&self.leaves[idx], lv),
        }
    }

    // pub fn get_entry(&mut self, lv: LV, hint_fwd: bool) -> (V, LV) {
    /// Returns (value, upper bound)
    pub fn get_entry_mut(&mut self, lv: LV) -> RleDRun<V> {
        let cursor = self.cursor_at(lv);
        self.cursor = cursor;
        let leaf = &self.leaves[cursor.leaf_idx.0];
        let val = leaf.children[cursor.elem_idx];
        let lower_bound = leaf.bounds[cursor.elem_idx];

        let next_elem = cursor.elem_idx + 1;
        let upper_bound = if next_elem >= LEAF_CHILDREN {
            self.upper_bound(leaf)
        } else {
            leaf.bounds[next_elem]
        };

        RleDRun {
            start: lower_bound,
            end: upper_bound,
            val
        }
    }

    pub fn get_entry(&self, lv: LV) -> RleDRun<V> {
        let cursor = self.cursor_at(lv);
        let leaf = &self.leaves[cursor.leaf_idx.0];
        let val = leaf.children[cursor.elem_idx];
        let lower_bound = leaf.bounds[cursor.elem_idx];

        let next_elem = cursor.elem_idx + 1;
        let upper_bound = if next_elem >= LEAF_CHILDREN {
            self.upper_bound(leaf)
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

    fn get_leaf_and_bound(&mut self, idx: LeafIdx) -> (&mut IndexLeaf<V>, LV) {
        let leaf = &self.leaves[idx.0];
        let upper_bound = self.upper_bound(leaf);
        (&mut self.leaves[idx.0], upper_bound)
    }

    // fn clear_range_in

    // fn next_leaf(

    /// This method clears everything out of the way for the specified element, to set its
    /// upper bound correctly.
    fn extend_upper_range(&mut self, leaf_idx: LeafIdx, elem_idx: usize, end: LV) {
        // println!("extend_upper_range)

        // There are sort of 4 cases:
        // - The range doesn't delete any elements
        // - The range deletes the suffix of a leaf (and we need to recurse up)
        // - The range deletes the prefix of the leaf
        // - The range deletes some internal set of elements. Shuffle back and terminate.

        debug_assert!(elem_idx < LEAF_CHILDREN);
        // let leaf = &mut self.leaves[leaf_idx.0];
        let (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
        dbg!((leaf_idx, elem_idx, end, leaf_upper_bound, &leaf));
        debug_assert!(end > leaf.bounds[elem_idx]); // This element will not be removed.

        // This element is fine. We start from the next.
        // elem_idx += 1;
        let trim_start = elem_idx + 1;

        // if elem_idx >= LEAF_CHILDREN {
        //     // Roll to the next item.
        //     if leaf.is_last() { return; }
        //     else {
        //         leaf_idx = leaf.next_leaf;
        //         leaf = &mut self.leaves[leaf_idx.0];
        //     }
        // }

        if trim_start < LEAF_CHILDREN {
            // This function wouldn't be called if we had nothing to do. (Though if this were the
            // case, we could return immediately).
            debug_assert!(leaf.bounds[trim_start] < end);

            let mut del_to = trim_start;
            // let mut last_idx = i;

            // let mut stop_here = false;

            loop {
                // The bounds of element i.
                let next = del_to + 1;
                let b = if next > LEAF_CHILDREN {
                    break;
                } else if next == LEAF_CHILDREN {
                    leaf_upper_bound
                } else {
                    leaf.bounds[next]
                };
                // Which may be usize::MAX.

                // if b == usize::MAX { del_to = LEAF_CHILDREN; break; }

                match end.cmp(&b) {
                    Ordering::Less => {
                        println!("Trim {del_to} to {end}");
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
            }

            if del_to >= LEAF_CHILDREN || leaf.bounds[del_to] == usize::MAX {
                // Delete the rest of this leaf and bubble up.
                leaf.bounds[trim_start..].fill(usize::MAX);
                println!("BUBBLE!");
                todo!();
            } else {
                let trimmed_items = del_to - trim_start;

                if trimmed_items >= 1 {
                    println!("trim {trim_start} <- {del_to}..");

                    // Hold onto your hats, its time to delete some items.
                    leaf.children.copy_within(del_to.., trim_start);
                    leaf.bounds.copy_within(del_to.., trim_start);
                    leaf.bounds[del_to - trimmed_items..].fill(usize::MAX);
                }
            }
        }
    }

    pub fn set_range(&mut self, range: DTRange, mut data: V, hint_fwd: bool) {
        if range.is_empty() { return; }

        let cursor = self.cursor_at(range.start);

        // Setting a range can involve deleting some number of data items, and inserting an item.
        //
        // For now, I'm never going to leave a leaf empty just so I can avoid needing to deal with
        // ever deleting nodes.

        // if !hint_fwd {
        self.cursor = cursor;
        // }

        let IndexCursor { mut leaf_idx, mut elem_idx } = cursor;
        let DTRange { mut start, mut end } = range;
        // let range = ();
        // let cursor = ();

        // let dbg_upper_bound = self.upper_bound(&self.leaves[leaf_idx.0]);
        // let mut leaf = &mut self.leaves[leaf_idx.0];
        let (mut leaf, mut leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);

        debug_assert!(start >= leaf.bounds[0] || leaf_idx.0 == 0);
        debug_assert!(start < leaf_upper_bound);
        // debug_assert!(elem_idx == LEAF_CHILDREN - 1 || start < leaf.bounds[elem_idx + 1]);
        // And the range should be < the upper bound.

        // debug_assert!(leaf.is_last() || start < leaf.upper_bound);

        assert!(elem_idx < LEAF_CHILDREN);

        let mut cur_start = leaf.bounds[elem_idx];

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

        if end < cur_end {
            // Try to append the end of the current element.
            if data.try_append(end - start, &leaf.children[elem_idx].at_offset(end), cur_end - end) {
                // Nice. We'll handle this in the special case below.
                end = cur_end;
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
                } else {
                    // Preserve the end of this item. Eg, x -> yx.
                    debug_assert!(cur_start == start);
                    debug_assert!(end < cur_end);

                    (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                    let leaf = &mut self.leaves[leaf_idx.0];

                    debug_assert_eq!(leaf.children[elem_idx + 1], leaf.children[elem_idx]);

                    debug_assert_eq!(leaf.bounds[elem_idx], start);
                    assert!(elem_idx + 1 < LEAF_CHILDREN);
                    leaf.children[elem_idx] = data;
                    leaf.bounds[elem_idx + 1] = end;
                    leaf.children[elem_idx + 1] = leaf.children[elem_idx + 1].at_offset(end - start);

                    // Since start == lower bound, the parents won't need updating.
                    // if elem_idx == 0 {
                    //     Self::recursively_update_nodes(&mut self.nodes, leaf.parent, leaf_idx.0, start);
                    // }
                }
                return;
            }
        }

        if end == cur_end {
            // Special case. Might not be worth it.
            if start == cur_start {
                // Nuke the existing item.
                leaf.children[elem_idx] = data;

                // Since start == lower bound, the parents don't need updating.
                // if elem_idx == 0 {
                //     let parent = leaf.parent;
                //     Self::recursively_update_nodes(&mut self.nodes, parent, leaf_idx.0, start);
                // }
            } else {
                // Preserve the start of the item. x -> xy.
                debug_assert!(start > cur_start);

                (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                let leaf = &mut self.leaves[leaf_idx.0];

                assert!(elem_idx + 1 < LEAF_CHILDREN);
                leaf.children[elem_idx + 1] = data;
                leaf.bounds[elem_idx + 1] = start;
                // We didn't modify [0], so no parent update.
            }
            return;
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
            if elem_idx >= LEAF_CHILDREN {
                // This is the end of the leaf node.
                // leaf.upper_bound = start;

                if leaf.is_last() {
                    panic!("I don't think this can happen");
                    // Split the last element and insert.
                    leaf_idx = self.split_leaf(leaf_idx);
                    let new_leaf = &mut self.leaves[leaf_idx.0];

                    new_leaf.children[LEAF_SPLIT_POINT] = data;
                    new_leaf.bounds[LEAF_SPLIT_POINT] = start;
                    // new_leaf.upper_bound = range.end;
                    return;
                } else {
                    // We've trimmed this leaf node. Roll the cursor to the next item.
                    leaf_idx = leaf.next_leaf;
                    (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
                    // leaf = &mut self.leaves[leaf_idx.0];
                    elem_idx = 0;
                    // TODO: Fuzz check, but I think this line isn't necessary.
                    // leaf.bounds[0] = range.start;

                    // We're going to replace the leaf's starting item.
                    let parent = leaf.parent;
                    Self::recursively_update_nodes(&mut self.nodes, parent, leaf_idx.0, start);
                    leaf = &mut self.leaves[leaf_idx.0];
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
                } else {
                    (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
                    leaf = &mut self.leaves[leaf_idx.0];
                    leaf.children[elem_idx] = data;
                    leaf.bounds[elem_idx + 1] = end;
                    leaf.children[elem_idx + 1] = leaf.children[elem_idx + 1].at_offset(end - cur_start);
                }
                return;
            }

            // cur_start = start; // Since we've pushed down the item bounds.
        } else {
            debug_assert_eq!(cur_start, start);
            debug_assert!(end > cur_end);
        }

        // We don't care about the current element at all. Just overwrite it and extend
        // the bounds.
        (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
        leaf = &mut self.leaves[leaf_idx.0];
        leaf.children[elem_idx] = data;
        self.extend_upper_range(leaf_idx, elem_idx, end);

        if hint_fwd {
            self.cursor = IndexCursor {
                leaf_idx,
                elem_idx,
            };
        }
    }
    // pub fn set_range(&mut self, mut range: DTRange, data: V, hint_fwd: bool) {
    //     if range.is_empty() { return; }
    //
    //     let cursor = self.cursor_at(range.start);
    //
    //     // Setting a range can involve deleting some number of data items, and inserting an item.
    //     //
    //     // For now, I'm never going to leave a leaf empty just so I can avoid needing to deal with
    //     // ever deleting nodes.
    //
    //     if !hint_fwd {
    //         self.cursor = cursor;
    //     }
    //
    //     let IndexCursor { mut leaf_idx, mut elem_idx } = cursor;
    //
    //     // let dbg_upper_bound = self.upper_bound(&self.leaves[leaf_idx.0]);
    //     // let mut leaf = &mut self.leaves[leaf_idx.0];
    //     let (mut leaf, mut leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
    //
    //     debug_assert!(range.start >= leaf.bounds[0] || leaf_idx.0 == 0);
    //     debug_assert!(range.start < leaf_upper_bound);
    //     // And the range should be < the upper bound.
    //
    //     // debug_assert!(leaf.is_last() || range.start < leaf.upper_bound);
    //
    //     assert!(elem_idx < LEAF_CHILDREN);
    //
    //     let mut lower_bound = leaf.bounds[elem_idx];
    //     if lower_bound == usize::MAX {
    //         // The only way this should happen is if we're inserting into the end of the tree,
    //         // after the last element.
    //         debug_assert!(leaf.is_last());
    //
    //         // This will implicitly extend the previous item to range.start, but thats ok here.
    //         leaf.bounds[elem_idx] = range.start;
    //         leaf.children[elem_idx] = data;
    //         // leaf.upper_bound = range.end;
    //
    //         if elem_idx == 0 {
    //             // I think this should only happen on the first insert. In that case, there is no
    //             // parent.
    //             debug_assert_eq!(self.height, 0);
    //             // Self::recursively_update_nodes(&mut self.nodes, leaf.parent, leaf_idx.0, range.start);
    //         }
    //         return;
    //     } else if elem_idx == 0 && range.start < lower_bound {
    //         // If we insert at the start of the tree, the cursor will point to the first element but
    //         // that element will have a lower bound above the range start. Extend downwards
    //         // first to make the logic below simpler. This check may be able to be removed later.
    //         debug_assert_eq!(leaf_idx.0, 0); // Should only happen on the first node.
    //
    //         let offset = lower_bound - range.start;
    //         leaf.bounds[0] = range.start;
    //         if data.can_append(offset, &leaf.children[0]) {
    //             // Short circuit.
    //             // leaf.children[0].prepend(data);
    //             leaf.children[0] = data;
    //             return;
    //         }
    //
    //         lower_bound = range.start;
    //     }
    //
    //     // TODO: Probably worth a short-circuit check here to see if the value even changed.
    //
    //     let upper_bound = if elem_idx >= LEAF_CHILDREN - 1 {
    //         leaf_upper_bound
    //     } else {
    //         // This is pretty gnarly.
    //         let b = leaf.bounds[elem_idx + 1];
    //         if b == usize::MAX { leaf_upper_bound } else { b }
    //     };
    //
    //     // let upper_bound = leaf.bound_for_idx(elem_idx);
    //     if range.end < upper_bound {
    //         // In this case, the item is replacing a prefix of the target slot. We'll just hardcode
    //         // these cases, since otherwise we need to deal with remainders below and thats a pain.
    //         if lower_bound < range.start {
    //             // We need to "splice in" this item. Eg, x -> xyx. This will result in 2 inserted
    //             // items.
    //
    //             // The resulting behaviour should be that:
    //             // b1 (x) b2  ---->  b1 (x) range.start (y) range.end (x) b2
    //
    //             // Check if we actually need to.
    //             if leaf.children[elem_idx].can_append(range.start - lower_bound, &data) {
    //                 // x+y == x. Also double check that y+x == x as well.
    //                 debug_assert!(
    //                     data.can_append(
    //                         range.len(),
    //                         &leaf.children[elem_idx].at_offset(range.end - lower_bound)
    //                     ));
    //                 return;
    //             }
    //
    //             // The item at elem_idx is the start of the item we're splitting. Leave it alone.
    //             // We'll replace elem_idx + 1 with data and elem_idx + 2 with remainder.
    //
    //             (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<2>(leaf_idx, elem_idx);
    //             let leaf = &mut self.leaves[leaf_idx.0];
    //
    //             assert!(elem_idx + 2 < LEAF_CHILDREN);
    //             leaf.bounds[elem_idx + 1] = range.start;
    //             leaf.children[elem_idx + 1] = data;
    //             leaf.bounds[elem_idx + 2] = range.end;
    //             // Interestingly, elem_idx + 2 should already have remainder_val because of the
    //             // memcpy.
    //             debug_assert_eq!(leaf.children[elem_idx + 2], leaf.children[elem_idx]);
    //
    //             // Subsequent bounds will be fine, because they were copied.
    //             // debug_assert_eq!(leaf.bound_for_idx(elem_idx + 2), upper_bound);
    //         } else {
    //             // Preserve the end of this item. Eg, x -> yx.
    //             debug_assert!(lower_bound == range.start);
    //
    //             // Do we need to do anything?
    //             if data.can_append(range.len(), &leaf.children[elem_idx]) {
    //                 return;
    //             }
    //
    //             (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
    //             let leaf = &mut self.leaves[leaf_idx.0];
    //
    //             debug_assert_eq!(leaf.children[elem_idx + 1], leaf.children[elem_idx]);
    //
    //             leaf.children[elem_idx] = data;
    //             leaf.bounds[elem_idx + 1] = range.end;
    //             leaf.children[elem_idx + 1] = leaf.children[elem_idx + 1].at_offset(range.len());
    //
    //             debug_assert_eq!(leaf.bounds[elem_idx], range.start);
    //
    //             // Since range.start == lower bound, the parents won't be updated anyway.
    //             // if elem_idx == 0 {
    //             //     Self::recursively_update_nodes(&mut self.nodes, leaf.parent, leaf_idx.0, range.start);
    //             // }
    //         }
    //         return;
    //     } else if range.end == upper_bound {
    //         // Special case. Might not be worth it.
    //         if range.start == lower_bound {
    //             // Nuke the existing item.
    //             leaf.children[elem_idx] = data;
    //
    //             // Since range.start == lower bound, the parents won't be updated anyway.
    //             // if elem_idx == 0 {
    //             //     let parent = leaf.parent;
    //             //     Self::recursively_update_nodes(&mut self.nodes, parent, leaf_idx.0, range.start);
    //             // }
    //         } else {
    //             // Preserve the start of the item. x -> xy.
    //             debug_assert!(range.start > lower_bound);
    //
    //             if leaf.children[elem_idx].can_append(range.start - lower_bound, &data) {
    //                 // No change.
    //                 return;
    //             }
    //
    //             (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
    //             let leaf = &mut self.leaves[leaf_idx.0];
    //
    //             leaf.children[elem_idx + 1] = data;
    //             leaf.bounds[elem_idx + 1] = range.start;
    //         }
    //         return;
    //     }
    //
    //     // To reach this point, we need to trim at least one future item.
    //     debug_assert!(range.end > upper_bound);
    //     if lower_bound < range.start {
    //         if leaf.children[elem_idx].can_append(range.start - lower_bound, &data) {
    //             // The item extends the current element. Nuke subsequent elements in this leaf.
    //             // We could just stop here, but subsequent leaves may 'need' to be cleared. I'll use
    //             // the same loop below to sort that out. This code is sort of awful since I'm
    //             // reusing some of the logic from below. I'm sure there's a nice way to write this.
    //             if range.end >= leaf_upper_bound {
    //                 // Nuke the whole leaf and continue.
    //                 leaf.bounds[elem_idx+1..].fill(usize::MAX);
    //                 if range.end == leaf_upper_bound { return; }
    //                 else {
    //                     // Can't be the last node since range.end >= leaf_upper_bound.
    //                     debug_assert!(!leaf.is_last());
    //                     range.start = leaf_upper_bound;
    //                     // And move to the next leaf and continue.
    //                     leaf_idx = leaf.next_leaf;
    //                     (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
    //                     elem_idx = 0;
    //                 }
    //             } else {
    //                 // Delete some of the items.
    //                 // range.end < leaf_upper_bound
    //                 // Bleh gross.
    //                 let mut deleted_items = 0;
    //                 elem_idx += 1;
    //
    //                 for i in elem_idx..LEAF_CHILDREN {
    //                     let bound = if i < LEAF_CHILDREN && leaf.bounds[i] != usize::MAX {
    //                         leaf.bounds[i]
    //                     } else {
    //                         leaf_upper_bound
    //                     };
    //                     if bound <= range.end {
    //                         deleted_items += 1;
    //                     } else {
    //                         leaf.bounds[i - 1] = range.end;
    //                         break;
    //                     }
    //                 }
    //                 if deleted_items > 0 {
    //                     leaf.bounds.copy_within(elem_idx + deleted_items..LEAF_CHILDREN, elem_idx + 1);
    //                     leaf.bounds[LEAF_CHILDREN - (deleted_items - 1)..].fill(usize::MAX);
    //                     leaf.children.copy_within(elem_idx + deleted_items..LEAF_CHILDREN, elem_idx + 1);
    //                 }
    //
    //                 return;
    //             }
    //
    //             let new_end = range.end.min(leaf_upper_bound);
    //
    //             return;
    //         } else {
    //             // Leave the current item alone and modify the next item.
    //             elem_idx += 1;
    //
    //             if elem_idx >= LEAF_CHILDREN {
    //                 // This is the end of the leaf node.
    //                 // leaf.upper_bound = range.start;
    //
    //                 if leaf.is_last() {
    //                     // Split the last element and insert.
    //                     leaf_idx = self.split_leaf(leaf_idx);
    //                     let new_leaf = &mut self.leaves[leaf_idx.0];
    //
    //                     new_leaf.children[LEAF_SPLIT_POINT] = data;
    //                     new_leaf.bounds[LEAF_SPLIT_POINT] = range.start;
    //                     // new_leaf.upper_bound = range.end;
    //                     return;
    //                 } else {
    //                     // We've trimmed this leaf node. Roll the cursor to the next item.
    //                     leaf_idx = leaf.next_leaf;
    //                     (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
    //                     elem_idx = 0;
    //                     // TODO: Fuzz check, but I think this line isn't necessary.
    //                     // leaf.bounds[0] = range.start;
    //                 }
    //             }
    //             // } else { leaf.bounds[elem_idx] = range.start; } // TODO Again, might not be needed.
    //         }
    //     }
    //
    //     // Scan through leaves, inserting content. The inserted content may be spread out to
    //     // preserve the current upper bounds on items, to make sure we don't need to delete any
    //     // nodes. (Because that would be a hassle).
    //     loop { // Usually just once.
    //         debug_assert!(elem_idx < LEAF_CHILDREN);
    //         debug_assert!(range.start <= leaf.bounds[elem_idx]);
    //         debug_assert!(leaf.is_last() || range.start < leaf_upper_bound);
    //         // debug_assert_ne!(leaf.bounds[elem_idx], usize::MAX);
    //
    //         // but the item we're looking at may be unused.
    //
    //         let mut deleted_items = 0;
    //
    //         for i in elem_idx + 1..LEAF_CHILDREN + 1 {
    //             let bound = if i < LEAF_CHILDREN && leaf.bounds[i] != usize::MAX {
    //                 leaf.bounds[i]
    //             } else {
    //                 leaf_upper_bound
    //             };
    //
    //             if bound <= range.end {
    //                 deleted_items += 1;
    //             } else {
    //                 // bound > range.end. Keep this item.
    //                 leaf.bounds[i - 1] = range.end;
    //                 break;
    //             }
    //         }
    //
    //         if deleted_items == 0 {
    //             // Insert here, pushing subsequent elements back.
    //             (leaf_idx, elem_idx) = self.make_space_in_leaf_for::<1>(leaf_idx, elem_idx);
    //             // leaf = &mut self.leaves[leaf_idx.0];
    //             (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
    //         } else if deleted_items > 1 {
    //             // Slide subsequent items.
    //             leaf.bounds.copy_within(elem_idx + deleted_items..LEAF_CHILDREN, elem_idx + 1);
    //             leaf.bounds[LEAF_CHILDREN - (deleted_items - 1)..].fill(usize::MAX);
    //             leaf.children.copy_within(elem_idx + deleted_items..LEAF_CHILDREN, elem_idx + 1);
    //         }
    //
    //         leaf.bounds[elem_idx] = range.start;
    //         leaf.children[elem_idx] = data;
    //
    //         if elem_idx == 0 {
    //             let parent = leaf.parent;
    //             Self::recursively_update_nodes(&mut self.nodes, parent, leaf_idx.0, range.start);
    //             leaf = &mut self.leaves[leaf_idx.0];
    //         }
    //
    //         // To avoid empty elements or needing to deal with deleting elements, I'm potentially
    //         // splitting the set operation over multiple nodes & preserving the upper bound on
    //         // each leaf node. (Or shrinking the upper bound).
    //         if range.end <= leaf_upper_bound {
    //             // We're done here.
    //             break;
    //         } else {
    //             // Advance to the next item and insert / modify the data structure there.
    //             range.start = leaf_upper_bound;
    //             leaf_idx = leaf.next_leaf;
    //             // leaf = &mut self.leaves[leaf_idx.0];
    //             (leaf, leaf_upper_bound) = self.get_leaf_and_bound(leaf_idx);
    //             elem_idx = 0;
    //             continue;
    //         }
    //     }
    //
    //     if hint_fwd {
    //         self.cursor = IndexCursor {
    //             leaf_idx,
    //             elem_idx,
    //         };
    //     }
    // }

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

    pub fn is_empty(&self) -> bool {
        let first_leaf = &self.leaves[self.first_leaf().0];
        first_leaf.bounds[0] == usize::MAX
    }

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


    fn dbg_check_walk(&self, idx: usize, height: usize, expect_start: Option<LV>, expect_parent: NodeIdx) {
        if height != 0 {
            // Visiting a node.
            assert!(idx < self.nodes.len());
            let node = &self.nodes[idx];
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
                    self.dbg_check_walk(child_idx, height - 1, Some(start), NodeIdx(idx));
                }
            }
        } else {
            // Visiting a leaf.
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
        }
    }

    #[allow(unused)]
    pub(super) fn dbg_check(&self) {
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
            for &b in &leaf.bounds[1..] {
                if b != usize::MAX {
                    assert!(b > prev, "Bounds does not monotonically increase b={:?}", &leaf.bounds);
                    // assert!(b < leaf.upper_bound);
                    // assert!(b < self.upper_bound);
                }
                prev = b;
            }

            if leaf.is_last() { break; }
            else {
                let next_leaf = &self[leaf.next_leaf];
                // assert_eq!(leaf.upper_bound, next_leaf.bounds[0]);
            }
            leaf_idx = leaf.next_leaf;
        }
        assert_eq!(leaves_visited, self.leaves.len());

        if self.height == 0 {
            assert!(self.root < self.leaves.len());
        } else {
            assert!(self.root < self.nodes.len());
        }

        // And walk the tree structure in the nodes
        self.dbg_check_walk(self.root, self.height, None, NodeIdx(usize::MAX));
    }

}

#[derive(Debug)]
pub struct IndexTreeIter<'a, V> {
    tree: &'a IndexTree<V>,
    leaf_idx: LeafIdx,
    // leaf: &'a IndexLeaf<V>,
    elem_idx: usize,
}

impl<'a, V: Clone> Iterator for IndexTreeIter<'a, V> {
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
    use std::ops::Range;
    use std::pin::Pin;
    use rand::prelude::SmallRng;
    use rand::{Rng, SeedableRng, thread_rng};
    use content_tree::{ContentTreeRaw, null_notify, RawPositionMetricsUsize};
    use crate::list_fuzzer_tools::fuzz_multithreaded;
    use super::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum Foo { A, B, C }
    use Foo::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
    struct X(usize);
    impl IndexContent for X {
        fn at_offset(&self, offset: usize) -> Self {
            X(self.0 + offset)
        }
    }

    fn expect_eq<'a, T: Default + IndexContent + 'a>(tree: &IndexTree<T>, vals: impl IntoIterator<Item = &'a RleDRun<T>>) {
        expect_eq_2(tree, vals.into_iter().copied())
    }

    fn expect_eq_2<T: Default + IndexContent>(tree: &IndexTree<T>, vals: impl IntoIterator<Item = RleDRun<T>>) {
        tree.dbg_check();

        let mut tree_iter = tree.iter();
        let mut vals_iter = vals.into_iter();

        while let Some(next_val) = vals_iter.next() {
            let RleDRun { mut start, end, val } = next_val;

            loop {
                let actual_next = tree_iter.next().expect("Tree missing item");
                // Skip anything before start.
                if actual_next.end <= start {
                    continue;
                }
                assert_eq!(val, actual_next.val, "{:?}", &tree_iter);
                if start < actual_next.start {
                    panic!("Missing element");
                }
                if actual_next.end >= end { break; }
                start = actual_next.end;
            }
        }
    }

    #[test]
    fn empty_tree_is_empty() {
        let tree = IndexTree::<X>::new();
        expect_eq(&tree, &[]);
    }

    #[test]
    fn overlapping_sets() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100), true);
        expect_eq(&tree, &[RleDRun::new(5..10, X(100))]);
        // assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);
        // dbg!(&tree.leaves[0]);
        tree.set_range((5..11).into(), X(200), true);
        expect_eq(&tree, &[RleDRun::new(5..11, X(200))]);

        tree.set_range((5..10).into(), X(100), true);
        expect_eq(&tree, &[
            RleDRun::new(5..10, X(100)),
            RleDRun::new(10..11, X(205)),
        ]);

        tree.set_range((2..50).into(), X(300), true);
        expect_eq(&tree, &[RleDRun::new(2..50, X(300))]);

    }

    #[test]
    fn split_values() {
        let mut tree = IndexTree::new();
        tree.set_range((10..20).into(), X(100), true);
        tree.set_range((12..15).into(), X(200), true);
        expect_eq(&tree, &[
            RleDRun::new(10..12, X(100)),
            RleDRun::new(12..15, X(200)),
            RleDRun::new(15..20, X(105)),
        ]);
    }

    #[test]
    fn set_inserts_1() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), X(100), true);
        expect_eq(&tree, &[RleDRun::new(5..10, X(100))]);

        tree.set_range((5..10).into(), X(200), true);
        expect_eq(&tree, &[RleDRun::new(5..10, X(200))]);

        // dbg!(&tree);
        tree.set_range((15..20).into(), X(300), true);
        // dbg!(tree.iter().collect::<Vec<_>>());
        expect_eq(&tree, &[
            RleDRun::new(5..10, X(200)),
            RleDRun::new(15..20, X(300)),
        ]);

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
    }

    #[test]
    fn set_inserts_2() {
        let mut tree = IndexTree::new();
        tree.set_range((5..10).into(), X(100), true);
        tree.set_range((1..5).into(), X(200), true);
        // dbg!(&tree);
        expect_eq(&tree, &[
            RleDRun::new(1..5, X(200)),
            RleDRun::new(5..10, X(100)),
        ]);
        dbg!(&tree.leaves[0]);

        tree.set_range((3..8).into(), X(300), true);
        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
        expect_eq(&tree, &[
            RleDRun::new(1..3, X(200)),
            RleDRun::new(3..8, X(300)),
            RleDRun::new(8..10, X(103)),
        ]);
    }

    #[test]
    fn split_leaf() {
        let mut tree = IndexTree::new();
        tree.set_range((1..2).into(), X(100), true);
        tree.dbg_check();
        tree.set_range((2..3).into(), X(200), true);
        tree.set_range((3..4).into(), X(100), true);
        tree.set_range((4..5).into(), X(200), true);
        tree.dbg_check();
        // dbg!(&tree);
        tree.set_range((5..6).into(), X(100), true);
        tree.dbg_check();

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());

        expect_eq(&tree, &[
            RleDRun::new(1..2, X(100)),
            RleDRun::new(2..3, X(200)),
            RleDRun::new(3..4, X(100)),
            RleDRun::new(4..5, X(200)),
            RleDRun::new(5..6, X(100)),
        ]);
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
            let val = rng.gen_range(0..10) + 100;
            let start = rng.gen_range(0..100);
            let len = rng.gen_range(0..100) + 1;

            // dbg!(&tree, start, len, val);
            // if _i == 11 {
            //     println!("blerp");
            // }
            tree.set_range((start..start+len).into(), X(val), true);
            // dbg!(&tree);
            tree.dbg_check();

            // dbg!(check_tree.iter().collect::<Vec<_>>());

            // let mut cursor = check_tree.mut_cursor_at_offset_pos(start, false);
            // unsafe {
            //     ContentTreeRaw::unsafe_mutate_entries_notify(|marker| {
            //         *marker = RleDRun::new(start..start + len, val);
            //     }, &mut cursor, len, null_notify);
            // }
            check_tree.replace_range_at_offset(start, (val..val+len).into());

            // if _i == 533 {
            //     dbg!(tree.iter().collect::<Vec<_>>());
            //     dbg!(check_tree.iter().filter_map(|r| {
            //         let val = r.val?;
            //         Some(RleDRun::new(r.start..r.end, val))
            //     }).collect::<Vec<_>>());
            // }

            // check_tree.iter
            expect_eq_2(&tree, check_tree.iter_with_pos().filter_map(|(pos, r)| {
                if r.start >= START_JUNK { return None; }
                Some(RleDRun::new(pos..pos+r.len(), X(r.start)))
            }));
            // expect_eq_2(&tree, check_tree.iter().filter_map(|r| {
            //     if r.start >= START_JUNK { return None; }
            //     Some(RleDRun::new(r.start..r.end, val))
            // }));
            // check_tree.replace_range_at_offset()
        }
    }

    #[test]
    fn fuzz_once() {
        fuzz(1152921504606846000, true);
    }

    #[test]
    #[ignore]
    fn fuzz_forever() {
        fuzz_multithreaded(u64::MAX, |seed| {
            if seed % 100 == 0 {
                println!("Iteration {}", seed);
            }
            fuzz(seed, false);
        })
    }
}

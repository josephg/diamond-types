use std::fmt::Debug;
use std::ops::{Index, IndexMut};
use std::ptr::NonNull;
use crate::{DTRange, LV};
use crate::ost::{NODE_CHILDREN, LEAF_CHILDREN, LeafIdx, NodeIdx};
use crate::ost::content_tree::{ContentLeaf, ContentNode, ContentTree};

#[derive(Debug, Clone)]
pub(super) struct IndexTree<V> {
    leaves: Vec<IndexLeaf<V>>,
    nodes: Vec<IndexNode>,
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


type LeafData = crate::listmerge::markers::Marker;

// const EMPTY_LEAF_DATA: (LV, LeafData) = (usize::MAX, LeafData::InsPtr(NonNull::dangling()));

#[derive(Debug, Clone)]
pub struct IndexLeaf<V> {
    // data: [IdxMarker; LEAF_CHILDREN],
    /// (start of range, data). Start == usize::MAX for empty entries.
    data: [(LV, V); LEAF_CHILDREN],
    // data: [LeafData; LEAF_CHILDREN],
    upper_bound: LV,
    next_leaf: LeafIdx,
    parent: NodeIdx,
}

fn initial_root_leaf<V: Default + Copy>() -> IndexLeaf<V> {
    IndexLeaf {
        data: [(usize::MAX, V::default()); LEAF_CHILDREN],
        upper_bound: usize::MAX, // The bounds of the last item is (functionally) infinity.
        next_leaf: LeafIdx(usize::MAX),
        parent: NodeIdx(0), // This node won't exist yet - but thats ok.
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
    children: [NodeChild; NODE_CHILDREN],
    parent: NodeIdx,
}

// impl Default for IndexNode {
//     fn default() -> Self {
//         Self {
//             children: [EMPTY_NODE_CHILD; NODE_CHILDREN],
//             parent: NodeIdx::default(),
//         }
//     }
// }

impl<V> IndexLeaf<V> {
    fn is_full(&self) -> bool {
        self.data.last().unwrap().0 != usize::MAX
    }

    fn bound_for_idx(&self, idx: usize) -> usize {
        let next_idx = idx + 1;
        if next_idx >= LEAF_CHILDREN {
            self.upper_bound
        } else {
            self.data[next_idx].0
        }
    }
}

impl IndexNode {
    fn is_full(&self) -> bool {
        self.children.last().unwrap().1 != usize::MAX
    }
}

impl<V: Default + Copy + Debug> Default for IndexTree<V> {
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

const NODE_SPLIT_POINT: usize = NODE_CHILDREN / 2;
const LEAF_SPLIT_POINT: usize = LEAF_CHILDREN / 2;


impl<V: Default + Copy + Debug> IndexTree<V> {
    pub(super) fn new() -> Self {
        Self {
            leaves: vec![initial_root_leaf()],
            nodes: vec![],
            height: 0,
            root: 0,
            cursor: IndexCursor::default(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.leaves.clear();
        self.nodes.clear();
        self.height = 0;
        self.root = 0;
        self.cursor = IndexCursor::default();
        self.leaves.push(initial_root_leaf());
    }

    // fn create_new_root_node(&mut self, child_a: usize, child_b: NodeChild) -> NodeIdx {
    fn create_new_root_node(root_height: &mut usize, nodes: &mut Vec<IndexNode>, child_a: usize, split_point: LV, child_b: usize) -> NodeIdx {
        // self.height += 1;
        *root_height += 1;
        let mut new_node = IndexNode {
            children: [EMPTY_NODE_CHILD; NODE_CHILDREN],
            parent: Default::default(),
        };
        new_node.children[0] = (0, child_a);
        new_node.children[1] = (split_point, child_b);

        let new_idx = nodes.len();
        nodes.push(new_node);
        NodeIdx(new_idx)
    }

    fn split_node(&mut self, idx: NodeIdx, children_are_leaves: bool) -> NodeIdx {
        // Split a full internal node into 2 nodes.
        let new_node_idx = self.nodes.len();
        let mut old_node = &mut self.nodes[idx.0];

        // The old leaf must be full before we split it.
        debug_assert!(old_node.is_full());

        let split_lv = old_node.children[NODE_SPLIT_POINT].0;
        let parent = if idx.0 == self.root {
            // We'll make a new root.
            let parent = Self::create_new_root_node(&mut self.height, &mut self.nodes,
                                                    idx.0, split_lv, new_node_idx);
            old_node = &mut self.nodes[idx.0]; // Reborrow for borrowck.
            old_node.parent = parent;
            parent
        } else {
            old_node.parent
        };

        let mut new_node = IndexNode {
            children: [EMPTY_NODE_CHILD; NODE_CHILDREN],
            parent
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

        let split_point_lv = new_node.children[0].0;
        self.nodes.push(new_node);
        if idx.0 != self.root {
            self.insert_into_node(parent, (split_point_lv, new_node_idx), idx.0, false);
        }

        NodeIdx(new_node_idx)
    }

    fn insert_into_node(&mut self, mut node_idx: NodeIdx, new_child: NodeChild, after_child: usize, children_are_leaves: bool) -> NodeIdx {
        let mut node = &mut self[node_idx];

        // Where will the child go? I wonder if the compiler can do anything smart with this...
        let mut insert_pos = node.children
            .iter()
            .position(|(_, idx)| { *idx == after_child })
            .unwrap();

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

        node_idx
    }

    fn split_leaf(&mut self, idx: LeafIdx) -> LeafIdx {
        // This function splits a full leaf node in the middle, into 2 new nodes.
        // The result is two nodes - old_leaf with items 0..N/2 and new_leaf with items N/2..N.

        let old_height = self.height;
        let new_leaf_idx = self.leaves.len(); // Weird instruction order for borrowck.
        let old_leaf = &mut self.leaves[idx.0];
        // let parent = old_leaf.parent;
        let split_lv = old_leaf.data[LEAF_SPLIT_POINT].0;

        let parent = if old_height == 0 {
            // Insert this leaf into a new root node. This has to be the first node.
            let parent = Self::create_new_root_node(&mut self.height, &mut self.nodes,
                                                    idx.0, split_lv, new_leaf_idx);
            debug_assert_eq!(parent, NodeIdx(0));
            // let parent = NodeIdx(self.nodes.len());
            debug_assert_eq!(old_leaf.parent, NodeIdx(0)); // Ok because its the default.
            // old_leaf.parent = NodeIdx(0); // Could just default nodes to have a parent of 0.
            NodeIdx(0)
        } else {
            old_leaf.parent
        };

        // The old leaf must be full before we split it.
        // debug_assert!(old_leaf.data.last().unwrap().is_some());
        debug_assert!(old_leaf.is_full());

        let mut new_leaf = IndexLeaf {
            data: [(usize::MAX, V::default()); LEAF_CHILDREN],
            upper_bound: old_leaf.upper_bound,
            next_leaf: old_leaf.next_leaf,
            parent,
        };

        // We'll steal the second half of the items in OLD_LEAF.
        // Could use ptr::copy_nonoverlapping but this is safe, and they compile to the same code.
        new_leaf.data[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.data[LEAF_SPLIT_POINT..]);
        // The old leaf's new bound is the first copied item's position.
        old_leaf.upper_bound = split_lv;
        old_leaf.data[LEAF_SPLIT_POINT..].fill((usize::MAX, V::default()));

        old_leaf.next_leaf = LeafIdx(new_leaf_idx);

        if old_height != 0 {
            self.insert_into_node(parent, (split_lv, new_leaf_idx), idx.0, true);
        }
        self.leaves.push(new_leaf);

        LeafIdx(new_leaf_idx)
    }

    fn find_in_leaf(leaf: &IndexLeaf<V>, needle: LV) -> usize {
        // Find the index of the first item where the needle is *not* in the range, and then return
        // the previous item.

        // There are much faster ways to write this using SIMD.
        leaf.data.iter()
            // We're looking for the first item past the needle.
            .position(|(lv, _)| needle <= *lv)
            .unwrap_or(LEAF_CHILDREN)
    }

    /// This function blindly assumes the item is definitely in the recursive children.
    fn find_in_node(node: &IndexNode, needle: LV) -> usize {
        // TODO: Speed up using SIMD.
        node.children.iter()
            // Looking for the first child which contains the needle.
            .position(|(lv, _)| { needle >= *lv })
            .expect("Invalid search in index node")
    }

    fn cursor_at(&self, lv: LV) -> IndexCursor {
        debug_assert!(lv < usize::MAX);
        let leaf = &self[self.cursor.leaf_idx];
        if lv >= leaf.data[0].0 && lv < leaf.upper_bound {
            // Ok! This is the node to use.
            // TODO: Take advantage of elem_idx in the cursor.
            return IndexCursor {
                leaf_idx: self.cursor.leaf_idx,
                elem_idx: Self::find_in_leaf(leaf, lv),
            }
        } else if lv == leaf.upper_bound {
            // Use the next node.
            return IndexCursor {
                leaf_idx: leaf.next_leaf,
                elem_idx: 0, // Has to be.
            }
        }

        // Make a cursor by descending from the root.
        let mut idx = self.root;
        for _h in 0..self.height {
            let n = &self.nodes[idx];
            idx = Self::find_in_node(n, lv);
        }

        // Now idx will point to the leaf node. Search there.
        IndexCursor {
            leaf_idx: LeafIdx(idx),
            elem_idx: Self::find_in_leaf(&self.leaves[idx], lv),
        }
    }

    pub fn set_range(&mut self, mut range: DTRange, data: V, hint_fwd: bool) {
        let cursor = self.cursor_at(range.start);
        dbg!((range.start, &cursor));

        // Setting a range can involve deleting some number of data items, and inserting an item.
        //
        // For now I'm requiring that the bounds of each item only shrink, never grow. So if the new
        // item overwrites multiple leaves, it'll be split between those leaves based on the leaves'
        // existing bounds.

        if !hint_fwd {
            self.cursor = cursor;
        }

        let IndexCursor { mut leaf_idx, mut elem_idx } = cursor;

        let mut leaf = &mut self[leaf_idx];
        debug_assert!(range.start < leaf.upper_bound);

        if elem_idx == LEAF_CHILDREN {
            // The item needs to be inserted into the next node because this one is full. But this
            // node's bound will be reduced.
            //
            // We could just split this node instead and insert here anyway, but chances are the
            // next node isn't full so thats probably a better bet.
            debug_assert!(leaf.is_full());
            leaf.upper_bound = range.start;
            elem_idx = 0;
            leaf_idx = leaf.next_leaf;
            leaf = &mut self[leaf_idx];
        }

        // dbg!(&leaf);

        assert!(elem_idx < LEAF_CHILDREN);

        // Inserting at the end of the data structure.
        if leaf.data[elem_idx].0 == usize::MAX {
            // If this isn't true, we would still need to scan subsequent items.
            debug_assert_eq!(leaf.upper_bound, usize::MAX);

            // We're inserting at the end of the data structure. We can just insert here directly.
            leaf.data[elem_idx] = (range.start, data);
            let next_idx = elem_idx + 1;

            if next_idx >= LEAF_CHILDREN {
                leaf.upper_bound = range.end;
            } else {
                leaf.data[next_idx].0 = range.end;
            }

            if hint_fwd {
                self.cursor = IndexCursor {
                    leaf_idx,
                    elem_idx: next_idx,
                };
            }

            return;
        }

        loop { // Scan through leaves, inserting content. Usually we'll just visit 1.
            let elem = &mut leaf.data[elem_idx];
            println!("visit {:?}", elem);

            // For each leaf, we'll insert 1 element and remove 0-n elements.
            let mut deleted_items = 0;
            for i in elem_idx..LEAF_CHILDREN {
                let elem_end = leaf.bound_for_idx(i);
                // let elem_end = if i >= LEAF_CHILDREN {
                //     leaf.upper_bound
                // } else {
                //     leaf.data[i + 1].0
                // };
                if elem_end <= range.end {
                    deleted_items += 1;
                } else {
                    // elem_end > range.end
                    leaf.data[i].0 = range.end;
                    // But otherwise leave this item alone.
                    break;
                }
            }

            if deleted_items == 0 {
                // Insert here, pushing subsequent elements back.
                // Consider splitting this into another function.

                if leaf.is_full() {
                    let new_node = self.split_leaf(leaf_idx);

                    if elem_idx >= LEAF_SPLIT_POINT {
                        // We're inserting into the newly created node.
                        leaf_idx = new_node;
                        elem_idx -= NODE_SPLIT_POINT;
                    }
                    // For borrowck.
                    leaf = &mut self[leaf_idx];
                }

                // Could scan to find the actual length of the children, then only memcpy that many. But
                // memcpy is cheap.
                leaf.data.copy_within(elem_idx..LEAF_CHILDREN - 1, elem_idx + 1);
                leaf.data[elem_idx] = (range.start, data);
            } else {
                // Replace the item with the new content.
                leaf.data[elem_idx].1 = data;

                if deleted_items > 1 {
                    // And slide back subsequent items.
                    leaf.data.copy_within(elem_idx + deleted_items..LEAF_CHILDREN, elem_idx + 1);
                }
            }

            // To avoid empty elements or needing to deal with deleting elements, I'm potentially
            // splitting the set operation over multiple nodes & preserving the upper bound on
            // each leaf node. (Or shrinking the upper bound).
            if range.end <= leaf.upper_bound {
                // We're done here.
                break;
            } else {
                // Advance to the next item and insert / modify the data structure there.
                range.start = leaf.upper_bound;
                leaf_idx = leaf.next_leaf;
                leaf = &mut self[leaf_idx];
                elem_idx = 0;
                continue;
            }
        }

        if hint_fwd {
            self.cursor = IndexCursor {
                leaf_idx,
                elem_idx,
            };
        }
    }

    #[allow(unused)]
    pub(super) fn dbg_check(&self) {
        // Invariants:
        // - All index markers point to the node which contains the specified item.
        // - Except for the root item, all leaves must have at least 1 data entry.
        // - The "left edge" of items should all have a lower bound of 0
        // - The last leaf node should have an upper bound and node_next of usize::MAX.
        if self.height == 0 {
            assert!(self.root < self.leaves.len());
        } else {
            assert!(self.root < self.nodes.len());
        }
    }

    fn first_leaf(&self) -> LeafIdx {
        if cfg!(debug_assertions) {
            let mut idx = self.root;
            for _ in 0..self.height {
                idx = self.nodes[idx].children[0].1;
            }
            debug_assert_eq!(idx, 0);
        }
        LeafIdx(0)
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

    pub fn to_vec(&self) -> Vec<(DTRange, V)> {
        self.iter().collect::<Vec<_>>()
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
    type Item = (DTRange, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.leaf_idx.0 == usize::MAX {
            return None;
        }

        let mut leaf = &self.tree[self.leaf_idx];
        if self.elem_idx >= LEAF_CHILDREN || leaf.data[self.elem_idx].0 == usize::MAX {
            // Try to move to the next leaf.
            self.leaf_idx = leaf.next_leaf;
            if self.leaf_idx.0 == usize::MAX {
                return None;
            }
            self.elem_idx = 0;

            leaf = &self.tree[self.leaf_idx];
        }

        let upper_bound = leaf.bound_for_idx(self.elem_idx);
        let elem = &leaf.data[self.elem_idx];
        self.elem_idx += 1;
        if upper_bound == usize::MAX {
            None
        } else {
            Some((DTRange { start: elem.0, end: upper_bound }, elem.1.clone()))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum Foo { A, B, C }
    use Foo::*;

    #[test]
    fn empty_tree_is_empty() {
        let tree = IndexTree::<()>::new();
        let mut iter = tree.iter();
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn set_inserts_1() {
        let mut tree = IndexTree::new();

        tree.set_range((5..10).into(), Some(A), true);
        assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);

        tree.set_range((5..10).into(), Some(B), true);
        assert_eq!(tree.to_vec(), &[((5..10).into(), Some(B))]);

        // dbg!(&tree);
        tree.set_range((15..20).into(), Some(C), true);
        assert_eq!(tree.to_vec(), &[
            ((5..10).into(), Some(B)),
            ((10..15).into(), None),
            ((15..20).into(), Some(C)),
        ]);

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
    }

    #[test]
    fn set_inserts_2() {
        let mut tree = IndexTree::new();
        tree.set_range((5..10).into(), Some(A), true);
        tree.set_range((1..5).into(), Some(B), true);
        assert_eq!(tree.to_vec(), &[
            ((1..5).into(), Some(B)),
            ((5..10).into(), Some(A)),
        ]);

        tree.set_range((3..8).into(), Some(C), true);
        assert_eq!(tree.to_vec(), &[
            ((1..3).into(), Some(B)),
            ((3..8).into(), Some(C)),
            ((8..10).into(), Some(A)),
        ]);

        // dbg!(&tree);
        // dbg!(tree.iter().collect::<Vec<_>>());
    }

    #[test]
    fn split_leaf() {
        let mut tree = IndexTree::new();
        tree.set_range((1..2).into(), Some(A), true);
        tree.set_range((2..3).into(), Some(B), true);
        tree.set_range((3..4).into(), Some(A), true);
        tree.set_range((4..5).into(), Some(B), true);
        tree.set_range((5..6).into(), Some(A), true);
        dbg!(&tree);
        dbg!(tree.iter().collect::<Vec<_>>());

    }
}



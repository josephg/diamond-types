
use std::ops::{Index, IndexMut};
use content_tree::ContentLength;
use rle::HasLength;
use crate::listmerge::yjsspan::CRDTSpan;
use crate::ost::{LEAF_CHILDREN, LeafIdx, LenPair, NODE_CHILDREN, NodeIdx};

// const LEAF_CHILDREN: usize = LEAF_SIZE - 1;

#[derive(Debug)]
pub(super) struct ContentTree {
    // The order of these vectors is arbitrary. I'm using Vec essentially as a simple memory pool.
    leaves: Vec<ContentLeaf>,
    nodes: Vec<ContentNode>,
    /// This counts the number of levels of internal nodes.
    height: usize,
    root: usize,
    // cursor: Option<ContentCursor>,
    cursor: Option<CachedContentCursor>,

    total_len: LenPair,
}

#[derive(Debug, Clone)]
struct ContentCursor {
    leaf: LeafIdx,

    /// The global starting position of the leaf node we're pointing to. This is used when the
    /// cursor is cached.
    ///
    /// This is a "current pos".
    leaf_global_start: usize,
    // leaf_start: LenPair,

    // The cursor points to a specific location within the specified leaf node.
    elem_idx: usize,
    offset: usize,

    /// This is the distance from the start of the tree to the current element / offset position.
    /// This is a "current pos".
    offset_global_pos: usize,
}

#[derive(Debug, Clone)]
struct CachedContentCursor {
    c: ContentCursor,
    leaf_current_end: usize,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ContentLeaf {
    /// Data entries are filled from the left. All unused entries have an empty ID.
    ///
    /// TODO: Try replacing this with a gap buffer.
    data: [CRDTSpan; LEAF_CHILDREN],

    // Redundant information. Could look at the parent. But this is useful for cursor
    // calculations.
    // size: LenPair,
    cur_size: usize,

    // TODO: Consider adding prev_leaf as well.

    /// usize::MAX for the last leaf node.
    next_leaf: LeafIdx,

    parent: NodeIdx,
}

// #[derive(Debug, Clone, Copy, Eq, PartialEq)]
// struct ItemSize {
//     current: usize,
//     end: usize,
// }

#[derive(Debug, Clone, Default)]
pub(super) struct ContentNode {
    // SoA or AoS?

    child_indexes: [usize; NODE_CHILDREN],

    /// The size (width) of each child item at the current point in time.
    cur_size: [usize; NODE_CHILDREN],

    /// The size (width) of each child item after all items have been merged.
    end_size: [usize; NODE_CHILDREN],
}

impl ContentLeaf {
    /// The number of children of this node "in use". Might be worth caching? Hard to tell.
    fn num_children(&self) -> usize {
        // TODO: SIMD accelerate me.
        for (i, e) in self.data.iter().enumerate() {
            if e.is_empty() { return i; }
        }
        return self.data.len()
    }
}

impl ContentNode {
    fn iter(&self) -> impl Iterator<Item = (usize, LenPair)> + '_ {
        // TODO: Would this generate better code with .copied() ?
        self.child_indexes.iter()
            .zip(self.cur_size.iter())
            .zip(self.end_size.iter())
            .take_while(|((idx, _), _)| **idx != usize::MAX)
            .map(|((&idx, &cur), &end)| (idx, LenPair { cur, end }))
    }

    fn len_of_child(&self, i: usize) -> LenPair {
        LenPair { cur: self.cur_size[i], end: self.end_size[i] }
    }

    // /// The number of children of this node "in use". Might be worth caching? Hard to tell.
    // fn num_children(&self) -> usize {
    //     // TODO: SIMD accelerate me.
    //     for (i, idx) in self.child_indexes.iter().enumerate() {
    //         if *idx == usize::MAX { return i; }
    //     }
    //     return self.child_indexes.len()
    // }
}

impl Index<LeafIdx> for ContentTree {
    type Output = ContentLeaf;

    fn index(&self, index: LeafIdx) -> &Self::Output {
        &self.leaves[index.0]
    }
}
impl IndexMut<LeafIdx> for ContentTree {
    fn index_mut(&mut self, index: LeafIdx) -> &mut Self::Output {
        &mut self.leaves[index.0]
    }
}
impl Index<NodeIdx> for ContentTree {
    type Output = ContentNode;

    fn index(&self, index: NodeIdx) -> &Self::Output {
        &self.nodes[index.0]
    }
}
impl IndexMut<NodeIdx> for ContentTree {
    fn index_mut(&mut self, index: NodeIdx) -> &mut Self::Output {
        &mut self.nodes[index.0]
    }
}

impl Default for ContentTree {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentTree {
    pub(super) fn new() -> Self {
        Self {
            leaves: vec![ContentLeaf::default()],
            nodes: vec![],
            height: 0,
            root: 0,
            cursor: None,
            total_len: Default::default(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.leaves.clear();
        self.nodes.clear();
        self.height = 0;
        self.root = 0;
        self.cursor = None;
        self.leaves.push(ContentLeaf::default());
    }

    fn cursor_within_leaf(&self, req_pos: usize, leaf_idx: LeafIdx, leaf_global_start: usize, stick_end: bool) -> ContentCursor {
        let mut p = leaf_global_start;
        let leaf = &self[leaf_idx];

        for (i, e) in leaf.data.iter().enumerate() {
            let c_len = e.content_len();
            let next_pos = p + c_len;
            if next_pos > req_pos || stick_end && next_pos == req_pos {
                return ContentCursor {
                    leaf: leaf_idx,
                    leaf_global_start,
                    elem_idx: i,
                    offset: req_pos - p,
                    offset_global_pos: req_pos,
                }
            }
            p = next_pos;
        }
        unreachable!("Cursor metadata is invalid");
    }

    fn try_cursor_at_current_cached(&self, req_cur_pos: usize, stick_end: bool) -> Option<ContentCursor> {
        // First check if we can use the cached cursor.
        if let Some(c) = self.cursor.as_ref() {
            // if req_cur_pos == c.c.leaf_global_start + c.c.offset_global_pos {
            if req_cur_pos == c.c.offset_global_pos {
                // The cursor is exactly where we expect. Take it!
                // TODO: Try this with &mut self and cursor.take().
                return Some(c.c.clone());
            }

            if req_cur_pos >= c.c.leaf_global_start {
                if req_cur_pos < c.leaf_current_end {
                    return Some(self.cursor_within_leaf(req_cur_pos, c.c.leaf, c.c.leaf_global_start, stick_end));
                } else if req_cur_pos == c.leaf_current_end {
                    let leaf = &self[c.c.leaf];
                    // The cursor points to the end of the last item in this node.
                    if stick_end {
                        // Make a cursor here.
                        let last_idx = leaf.num_children() - 1;

                        return Some(ContentCursor {
                            leaf: c.c.leaf,
                            leaf_global_start: c.c.leaf_global_start,
                            elem_idx: last_idx,
                            offset: leaf.data[last_idx].len(),
                            offset_global_pos: c.leaf_current_end,
                        })
                    } else {
                        // Make a cursor at the start of the subsequent node.
                        return Some(ContentCursor {
                            leaf: leaf.next_leaf,
                            leaf_global_start: c.leaf_current_end,
                            elem_idx: 0,
                            offset: 0,
                            offset_global_pos: c.leaf_current_end,
                        })
                    }
                }
            }
        }

        None
    }

    fn cursor_at_current(&self, req_pos: usize, stick_end: bool) -> ContentCursor {
        if let Some(c) = self.try_cursor_at_current_cached(req_pos, stick_end) {
            return c;
        }

        // Scan the tree.
        let mut idx = self.root;
        let mut req_pos_remaining = req_pos;

        'outer: for _h in 0..self.height {
            // Scan down this internal node.
            let n = &self.nodes[idx];

            // Scan across.
            // TODO: SIMD somehow?
            for i in 0..n.child_indexes.len() {
                // If we run out of childen.
                debug_assert_ne!(n.child_indexes[i], usize::MAX);

                if n.cur_size[i] > req_pos_remaining {
                    // Go down.
                    idx = n.child_indexes[i];
                    continue 'outer;
                } else {
                    req_pos_remaining -= n.cur_size[i];
                }

            }
            unreachable!("Could not find child element. Tree is corrupt.");
        }

        // preload the leaf?

        // Scan the leaf.
        return self.cursor_within_leaf(req_pos, LeafIdx(idx), req_pos - req_pos_remaining, stick_end);
    }

    fn cache_cursor(&mut self, c: ContentCursor) {
        let n = &self[c.leaf];
        self.cursor = Some(CachedContentCursor {
            leaf_current_end: c.leaf_global_start + n.cur_size,
            c,
        });
    }

    // fn insert_at_cursor<N>(&mut self, e: CRDTSpan, c: &ContentCursor, notify: &mut N)
    //     where N: FnMut(&CRDTSpan, LeafIdx)
    // {
    //     let leaf = &mut self[c.leaf];
    //     let width = e.len_pair();
    //
    //     let mut idx = c.elem_idx;
    //     let mut slot = &mut leaf.data[idx];
    //     let mut offset = c.offset;
    //
    //     // cur, end.
    //     let mut size_update = LenUpdate::default();
    //
    //     let remainder = if offset == 0 && idx > 0 {
    //         // We'll roll the cursor back to opportunistically see if we can append.
    //         idx -= 1;
    //         slot = &mut leaf.data[idx];
    //         offset = slot.len();
    //         None
    //     } else if offset < slot.len() {
    //         // Splice off the end of the current item.
    //         let remainder = slot.truncate(offset);
    //         size_update.dec_by(&remainder);
    //         Some(remainder)
    //     } else { None };
    //
    //     if offset != 0 {
    //         // Try and append the inserted item here.
    //         if slot.can_append(&e) {
    //             size_update.inc_by(&e);
    //             notify(&e, c.leaf);
    //             slot.append(e);
    //         } else {
    //             offset = 0;
    //             // Go to the next slot.
    //             if idx + 1 < leaf.data.len() {
    //                 idx += 1;
    //                 slot = &mut leaf.data[idx];
    //             }
    //         }
    //     }
    //
    //     // if let remainder =
    //
    //     leaf.cur_size += width.cur;
    //
    //
    //     // let mut slot = &mut leaf.data[c.elem_idx];
    //     if leaf.data[c.elem_idx].is_empty() {
    //         // The cursor points to the end of the node.
    //         debug_assert!(c.elem_idx == 0 || !leaf.data[c.elem_idx - 1].is_empty(), "Invalid cursor");
    //
    //         leaf.data[c.elem_idx] = e;
    //     }
    // }

    /// Check is implemented recursively. Because why not.
    fn dbg_check_walk(&self, idx: usize, height: usize, expect_len: LenPair, global_cpos: usize) {
        if height != 0 {
            assert!(idx < self.nodes.len());
            let node = &self.nodes[idx];

            let mut actual_child_len = LenPair::default();

            // Count the size and recurse.
            let mut finished = false;
            for (i, &child_idx) in node.child_indexes.iter().enumerate() {
                if child_idx == usize::MAX {
                    finished = true;
                } else {
                    assert_eq!(finished, false);
                    let child_len = node.len_of_child(i);
                    self.dbg_check_walk(child_idx, height - 1, child_len, global_cpos + actual_child_len.cur);
                    actual_child_len += child_len;
                }
            }

            assert_eq!(actual_child_len, expect_len);
        } else {
            // Look at the leaf at idx.
            assert!(idx < self.leaves.len());
            let leaf = &self.leaves[idx];

            // Count the size.
            let mut actual_len = LenPair::default();
            let mut finished = false;
            for d in &leaf.data {
                // Any empty elements should be at the end.
                if d.is_empty() {
                    finished = true;
                } else {
                    assert_eq!(finished, false);
                    actual_len += d.len_pair();
                }
            }

            assert_eq!(actual_len, expect_len);
            assert_eq!(actual_len.cur, leaf.cur_size);

            // Check the cached cursor.
            if let Some(c) = self.cursor.as_ref() {
                if c.c.leaf.0 == idx {
                    // Check the cursor makes sense in this leaf.
                    assert_eq!(c.c.leaf_global_start, global_cpos);
                    assert_eq!(c.leaf_current_end, global_cpos + leaf.cur_size);

                    // Check the offset position.
                    let mut offset_pos = global_cpos;
                    for e in &leaf.data[0..c.c.elem_idx] {
                        offset_pos += e.content_len();
                    }
                    assert_eq!(c.c.offset_global_pos, offset_pos + c.c.offset);
                }
            }
        }
    }

    #[allow(unused)]
    pub(super) fn dbg_check(&self) {
        // Invariants:
        // - All content sizes match
        // - The root item contains all other items
        // - Next pointers make sense in the leaves
        if self.height == 0 {
            assert!(self.root < self.leaves.len());
        } else {
            assert!(self.root < self.nodes.len());
        }

        // Walk the content tree.
        self.dbg_check_walk(self.root, self.height, self.total_len, 0);
    }
}


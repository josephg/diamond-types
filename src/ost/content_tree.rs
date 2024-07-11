use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::mem::{replace, take};
use std::ops::{Index, IndexMut, Range, Sub};
use std::ptr::NonNull;
use content_tree::{NodeLeaf, UnsafeCursor};
use rle::{HasLength, HasRleKey, MergableSpan, MergeableIterator, RleDRun, Searchable, SplitableSpan, SplitableSpanHelpers};
use crate::{DTRange, LV};
use crate::ost::{LEAF_CHILDREN, LeafIdx, LenPair, LenUpdate, NODE_CHILDREN, NodeIdx, remove_from_array, remove_from_array_fill};

pub(crate) trait Content: SplitableSpan + MergableSpan + Copy + HasLength {
    /// The length of the item. If IS_CUR then this is the "current length". Otherwise, this is the
    /// end length of the item.
    fn content_len<const IS_CUR: bool>(&self) -> usize {
        if self.takes_up_space::<IS_CUR>() { self.len() } else { 0 }
    }
    fn content_len_cur(&self) -> usize { self.content_len::<true>() }
    fn content_len_end(&self) -> usize { self.content_len::<false>() }
    fn content_len_pair(&self) -> LenPair {
        LenPair {
            cur: self.content_len_cur(),
            end: self.content_len_end(),
        }
    }

    /// The default item must "not exist".
    fn exists(&self) -> bool;
    fn takes_up_space<const IS_CUR: bool>(&self) -> bool;
    // fn current_len(&self) -> usize;

    // split_at_current_len() ?

    // fn underwater() -> Self;

    fn none() -> Self;
}

trait LeafMap {
    fn notify(&mut self, range: DTRange, leaf_idx: LeafIdx);
}

pub(crate) trait FlushUpdate: Default {
    // fn flush_delta_len(&mut self, leaf_idx: LeafIdx, delta: LenUpdate) {
    fn flush<V: Content>(&self, tree: &mut ContentTree<V>, leaf_idx: LeafIdx);

    #[inline]
    fn flush_and_clear<V: Content>(&mut self, tree: &mut ContentTree<V>, leaf_idx: LeafIdx) {
        self.flush(tree, leaf_idx);
        *self = Self::default();
    }
}

impl FlushUpdate for () {
    fn flush<V: Content>(&self, _tree: &mut ContentTree<V>, _leaf_idx: LeafIdx) {}
}
impl FlushUpdate for LenUpdate {
    fn flush<V: Content>(&self, tree: &mut ContentTree<V>, leaf_idx: LeafIdx) {
        tree.flush_delta_len(leaf_idx, *self);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ContentTree<V: Content> {
    leaves: Vec<ContentLeaf<V>>,
    nodes: Vec<ContentNode>,

    /// The number of internal nodes between the root and the leaves. This is initialized to 0,
    /// indicating we start with no internal nodes and just a single leaf.
    height: usize,

    /// The root node. If height == 0, this is a leaf (and has value 0). Otherwise, this is an index
    /// into the nodes vec pointing to the node representing the root.
    root: usize,
    total_len: LenPair,

    // cursor: ContentCursor,
    /// There is a cached cursor currently at some content position, with a held delta update.
    // cursor: Cell<Option<(LenPair, LenUpdate, ContentCursor)>>,
    // cursor: Option<(LenPair, MutContentCursor)>,
    cursor: Option<(LenPair, ContentCursor, LenUpdate)>,

    // Linked lists.
    // free_leaf_pool_head: LeafIdx,
    // free_node_pool_head: NodeIdx,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ContentCursor {
    // The item pointed to by the cursor should still be in the CPU's L1 cache. I could cache some
    // properties of the cursor's leaf item here, but I think it wouldn't improve performance -
    // since we wouldn't be saving any memory loads anyway.
    pub leaf_idx: LeafIdx,
    pub elem_idx: usize,

    /// Offset into the item.
    pub offset: usize,
}

// Wouldn't need this impl if LeafIdx defaulted to 0...
impl Default for ContentCursor {
    fn default() -> Self {
        ContentCursor {
            leaf_idx: LeafIdx(0),
            elem_idx: 0,
            offset: 0,
        }
    }
}

pub struct DeltaCursor(pub ContentCursor, pub LenUpdate);

// /// Same as a cursor, but with a cached delta object. This delta must be flushed whenever the
// /// cursor changes leaf node.
// #[derive(Debug, Clone, Copy)]
// pub(crate) struct MutContentCursor {
//     inner: ContentCursor,
//     delta: LenUpdate,
// }
//
// impl From<ContentCursor> for MutContentCursor {
//     fn from(inner: ContentCursor) -> Self {
//         MutContentCursor {
//             inner,
//             delta: Default::default(),
//         }
//     }
// }
//
// impl MutContentCursor {
//     pub fn clone_immutable(&self) -> ContentCursor {
//         self.inner
//     }
// }

// impl From<MutCursor> for ContentCursor {
//     fn from(cursor: MutCursor) -> Self {
//         ContentCursor {
//             leaf_idx: cursor.leaf_idx,
//             elem_idx: cursor.elem_idx,
//             offset: cursor.offset,
//         }
//     }
// }

// const EMPTY_LEAF_DATA: (LV, LeafData) = (usize::MAX, LeafData::InsPtr(NonNull::dangling()));

const NODE_SPLIT_POINT: usize = NODE_CHILDREN / 2;
// const LEAF_CHILDREN: usize = LEAF_SIZE - 1;
const LEAF_SPLIT_POINT: usize = LEAF_CHILDREN / 2;

#[derive(Debug, Clone)]
pub struct ContentLeaf<V> {
    /// Each child object knows its own bounds.
    ///
    /// It may turn out to be more efficient to split each field in children into its own sub-array.
    children: [V; LEAF_CHILDREN],

    // /// (start of range, data). Start == usize::MAX for empty entries.
    // children: [(LV, V); LEAF_CHILDREN],

    // upper_bound: LV,
    next_leaf: LeafIdx,
    parent: NodeIdx,
}

#[derive(Debug, Clone)]
pub struct ContentNode {
    /// The index is either an index into the internal nodes or leaf nodes depending on the height.
    ///
    /// Children have an index of usize::MAX if the slot is unused.
    child_indexes: [usize; NODE_CHILDREN],

    /// Child entries point to either another node or a leaf. We disambiguate using the height.
    /// The named LV is the first LV of the child data.
    child_width: [LenPair; NODE_CHILDREN],
    parent: NodeIdx,
}

// fn initial_root_leaf<V: Content>() -> ContentLeaf<V> {
fn initial_root_leaf<V: Content>() -> ContentLeaf<V> {
    // The tree is initialized with an "underwater" item covering the range.
    // let mut children = [V::default(); LEAF_CHILDREN];
    // children[0] = V::underwater();

    ContentLeaf {
        children: [V::none(); LEAF_CHILDREN],
        next_leaf: LeafIdx(usize::MAX),
        parent: NodeIdx(usize::MAX), // This node won't exist yet - but thats ok.
    }
}

// /// A node child specifies the width of the recursive children and an index in the data
// /// structure.
// type ContentNodeChild = (LenPair, usize);
//
// const EMPTY_NODE_CHILD: ContentNodeChild = (LenPair { cur: 0, end: 0 }, usize::MAX);

const EMPTY_LEN_PAIR: LenPair = LenPair { cur: 0, end: 0 };

impl<V: Content> ContentLeaf<V> {
    fn is_full(&self) -> bool {
        self.children.last().unwrap().exists()
    }

    #[inline(always)]
    fn has_space(&self, space_wanted: usize) -> bool {
        if space_wanted == 0 { return true; }
        !self.children[LEAF_CHILDREN - space_wanted].exists()
    }

    fn is_last(&self) -> bool { !self.next_leaf.exists() }

    fn next<'a>(&self, leaves: &'a [ContentLeaf<V>]) -> Option<&'a ContentLeaf<V>> {
        if self.is_last() { None }
        else { Some(&leaves[self.next_leaf.0]) }
    }

    fn next_mut<'a>(&self, leaves: &'a mut [ContentLeaf<V>]) -> Option<&'a mut ContentLeaf<V>> {
        if self.is_last() { None }
        else { Some(&mut leaves[self.next_leaf.0]) }
    }

    fn remove_children(&mut self, del_range: Range<usize>) {
        remove_from_array_fill(&mut self.children, del_range, V::none());
    }
}

impl ContentNode {
    fn is_full(&self) -> bool {
        *self.child_indexes.last().unwrap() != usize::MAX
    }

    fn remove_children(&mut self, del_range: Range<usize>) {
        remove_from_array_fill(&mut self.child_indexes, del_range.clone(), usize::MAX);
        remove_from_array(&mut self.child_width, del_range.clone());
    }

    /// Returns the (local) index of the named child. Aborts if the child is not in this node.
    fn idx_of_child(&self, child: usize) -> usize {
        self.child_indexes
            .iter()
            .position(|i| *i == child)
            .unwrap()
    }
}

impl<V: Content> Default for ContentTree<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Content> Index<LeafIdx> for ContentTree<V> {
    type Output = ContentLeaf<V>;

    fn index(&self, index: LeafIdx) -> &Self::Output {
        &self.leaves[index.0]
    }
}
impl<V: Content> IndexMut<LeafIdx> for ContentTree<V> {
    fn index_mut(&mut self, index: LeafIdx) -> &mut Self::Output {
        &mut self.leaves[index.0]
    }
}
impl<V: Content> Index<NodeIdx> for ContentTree<V> {
    type Output = ContentNode;

    fn index(&self, index: NodeIdx) -> &Self::Output {
        &self.nodes[index.0]
    }
}
impl<V: Content> IndexMut<NodeIdx> for ContentTree<V> {
    fn index_mut(&mut self, index: NodeIdx) -> &mut Self::Output {
        &mut self.nodes[index.0]
    }
}

#[inline]
fn inc_delta_update<V: Content>(delta_len: &mut LenUpdate, e: &V) {
    delta_len.cur += e.content_len_cur() as isize;
    delta_len.end += e.content_len_end() as isize;
}
#[inline]
fn dec_delta_update<V: Content>(delta_len: &mut LenUpdate, e: &V) {
    delta_len.cur -= e.content_len_cur() as isize;
    delta_len.end -= e.content_len_end() as isize;
}

impl ContentCursor {

    /// Move a cursor at the end of an item to the next item.
    ///
    /// Returns false if there is no next item.
    pub(crate) fn roll_next_item<V: Content>(&mut self, tree: &ContentTree<V>) -> (bool, Option<LeafIdx>) {
        let leaf = &tree[self.leaf_idx];
        if self.offset < leaf.children[self.elem_idx].len() { return (true, None); }

        self.next_entry(tree)
    }

    pub(crate) fn next_entry<V: Content>(&mut self, tree: &ContentTree<V>) -> (bool, Option<LeafIdx>) {
        let leaf = &tree[self.leaf_idx];

        self.elem_idx += 1;
        self.offset = 0;

        if self.elem_idx >= leaf.children.len() || !leaf.children[self.elem_idx].exists() {
            // Go to the next node.
            let old_leaf = self.leaf_idx;
            // let old_delta = take(&mut self.delta);

            self.leaf_idx = leaf.next_leaf;
            self.elem_idx = 0;

            // flush.flush_and_clear(tree, old_leaf);
            // self.flush_delta_len(old_leaf, old_delta);

            (self.leaf_idx.exists(), Some(old_leaf))
        } else { (true, None) }
    }


    /// Modifies the cursor to point to the next item
    pub(crate) fn inc_offset<V: Content>(&mut self, tree: &ContentTree<V>) {
        if cfg!(debug_assertions) {
            let leaf = &tree[self.leaf_idx];
            let e = &leaf.children[self.elem_idx];
            // assert!(e.takes_up_space::<true>());
            assert!(self.offset < e.len());
        }

        self.offset += 1;
    }

    fn flush_delta<V: Content>(&self, tree: &mut ContentTree<V>, delta: LenUpdate) {
        tree.flush_delta_len(self.leaf_idx, delta);
    }

    pub fn get_item<'a, V: Content>(&self, tree: &'a ContentTree<V>) -> (&'a V, usize) {
        let leaf = &tree[self.leaf_idx];
        (&leaf.children[self.elem_idx], self.offset)
    }

    /// Get the current position of the cursor. This is inefficient, and it should not normally be
    /// called.
    ///
    /// Note that any outstanding delta is not relevant, as the delta position only affects the pos
    /// of later items. The cursor itself is (just) early enough to be unaffected.
    pub(crate) fn get_pos<V: Content>(&self, tree: &ContentTree<V>) -> LenPair {
        let mut result = LenPair::default();

        let leaf = &tree[self.leaf_idx];
        let e = &leaf.children[self.elem_idx];
        if e.takes_up_space::<true>() { result.cur += self.offset; }
        if e.takes_up_space::<false>() { result.end += self.offset; }

        for c in leaf.children[0..self.elem_idx].iter() {
            result += c.content_len_pair();
        }

        // Then recurse up.
        let mut p = leaf.parent;
        let mut last_child = self.leaf_idx.0;
        while !p.is_root() {
            let node = &tree[p];

            for i in 0..node.child_indexes.len() {
                if node.child_indexes[i] == last_child { break; }
                result += node.child_width[i];
            }
            last_child = p.0;
            p = node.parent;
        }

        result
    }

    pub fn cmp<V: Content>(&self, other: &Self, tree: &ContentTree<V>) -> Ordering {
        if self.leaf_idx == other.leaf_idx {
            self.elem_idx.cmp(&other.elem_idx)
                .then(self.offset.cmp(&other.offset))
        } else {
            // Recursively walk up the trees to find a common ancestor. Because a b-tree is always
            // perfectly balanced, we can walk in lock step until both nodes are the same.
            let mut c1 = self.leaf_idx.0;
            let mut n1 = tree[self.leaf_idx].parent;
            let mut c2 = other.leaf_idx.0;
            let mut n2 = tree[other.leaf_idx].parent;

            while n1 != n2 {
                // Go up the tree.
                c1 = n1.0;
                n1 = tree[n1].parent;
                c2 = n2.0;
                n2 = tree[n2].parent;

                debug_assert!(!n1.is_root());
                debug_assert!(!n2.is_root());
            }

            // Find the relative order of c1 and c2.
            debug_assert_eq!(n1, n2);
            debug_assert_ne!(c1, c2);
            let node = &tree[n1];
            node.idx_of_child(c1).cmp(&node.idx_of_child(c2))
        }
    }
}

impl DeltaCursor {
    pub(crate) fn roll_next_item<V: Content>(&mut self, tree: &mut ContentTree<V>) -> bool {
        let (has_next, flush_leaf) = self.0.roll_next_item(tree);
        if let Some(flush_leaf) = flush_leaf {
            tree.flush_delta_and_clear(flush_leaf, &mut self.1);
        }

        has_next
    }

    pub(crate) fn next_entry<V: Content>(&mut self, tree: &mut ContentTree<V>) -> bool {
        let (has_next, flush_leaf) = self.0.next_entry(tree);
        if let Some(flush_leaf) = flush_leaf {
            tree.flush_delta_and_clear(flush_leaf, &mut self.1);
        }

        has_next
    }

    pub fn flush<V: Content>(self, tree: &mut ContentTree<V>) {
        tree.flush_delta_len(self.0.leaf_idx, self.1);
    }

    pub fn flush_delta_and_clear<V: Content>(&mut self, tree: &mut ContentTree<V>) {
        tree.flush_delta_and_clear(self.0.leaf_idx, &mut self.1);
    }
}


impl<V: Content> ContentTree<V> {
    pub fn new() -> Self {
        debug_assert_eq!(V::none().content_len_pair(), LenPair::default());
        // debug_assert_eq!(V::none().len(), 0);
        debug_assert_eq!(V::none().exists(), false);

        Self {
            leaves: vec![initial_root_leaf()],
            nodes: vec![],
            // upper_bound: 0,
            height: 0,
            root: 0,
            cursor: Default::default(),
            total_len: Default::default(),
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
        self.total_len = Default::default();
        // self.free_leaf_pool_head = LeafIdx(usize::MAX);
        // self.free_node_pool_head = NodeIdx(usize::MAX);

        self.leaves.push(initial_root_leaf());
    }

    pub fn set_single_item_notify<F>(&mut self, item: V, notify: F)
        where F: FnOnce(V, LeafIdx)
    {
        debug_assert!(self.is_empty());
        debug_assert!(self.cursor.is_none());

        self.total_len = item.content_len_pair();
        notify(item, LeafIdx(0));
        self.leaves[0].children[0] = item;
    }

    // fn create_new_root_node(&mut self, child_a: usize, child_b: usize, split_point: LenPair) -> NodeIdx {
    fn create_new_root_node(&mut self, child_a: usize, child_b: usize, b_size: LenPair) -> NodeIdx {
        self.height += 1;
        let mut new_root = ContentNode {
            child_indexes: [usize::MAX; NODE_CHILDREN],
            child_width: [Default::default(); NODE_CHILDREN],
            parent: Default::default(),
        };

        new_root.child_indexes[0] = child_a;
        new_root.child_indexes[1] = child_b;
        new_root.child_width[0] = self.total_len - b_size;
        new_root.child_width[1] = b_size;

        let new_idx = self.nodes.len();
        // println!("Setting root to {new_idx}");
        self.root = new_idx;
        self.nodes.push(new_root);
        NodeIdx(new_idx)
    }

    pub fn insert_notify<N>(&mut self, item: V, cursor: &mut DeltaCursor, notify: &mut N)
        where N: FnMut(V, LeafIdx)
    {
        // let mut delta_len = LenUpdate::default();
        self.insert(item, cursor, true, notify);
        // self.flush_delta_len(cursor.leaf_idx, delta_len);

        // if cfg!(debug_assertions) {
        //     self.dbg_check();
        // }
    }

    fn total_len(&self) -> LenPair {
        let mut len = self.total_len;
        // TODO: Try rewriting this into branch-free code.
        if let Some((_, _, flush)) = self.cursor.as_ref() {
            len.update_by(*flush);
        }
        len
    }

    /// Mutate in-place up to replace_max items in the next entry pointed at by the cursor.
    ///
    /// The cursor ends up right after the modified item.
    pub(crate) fn mutate_entry<N, MapFn, R>(&mut self, dc: &mut DeltaCursor, replace_max: usize, notify: &mut N, map_fn: MapFn) -> (usize, R)
    where N: FnMut(V, LeafIdx), MapFn: FnOnce(&mut V) -> R
    {
        if !dc.roll_next_item(self) { panic!("Cannot mutate at end of data structure") }
        let DeltaCursor(cursor, delta) = dc;
        // TODO: Make a variant of roll_next_item that doesn't roll delta.

        let leaf = &mut self.leaves[cursor.leaf_idx.0];
        let entry = &mut leaf.children[cursor.elem_idx];
        let entry_len = entry.len();
        if cursor.offset == 0 && entry_len <= replace_max {
            // Replace in-place.
            dec_delta_update(delta, entry);
            let r = map_fn(entry);
            inc_delta_update(delta, entry);
            // self.flush_delta_len(cursor.leaf_idx, cursor.delta);
            cursor.offset = entry_len;
            
            // We'll also do a brief best-effort attempt at merging this modified item with
            // subsequent items in the leaf.
            let mut entry = leaf.children[cursor.elem_idx];
            let scan_start = cursor.elem_idx + 1;
            let mut elem_idx2 = scan_start;
            while elem_idx2 < LEAF_CHILDREN {
                let entry2 = &leaf.children[elem_idx2];
                if entry2.exists() && entry.can_append(entry2) {
                    entry.append(*entry2);
                    elem_idx2 += 1;
                } else {
                    break;
                }
            }
            if elem_idx2 > scan_start {
                leaf.children[cursor.elem_idx] = entry;
                remove_from_array_fill(&mut leaf.children, scan_start..elem_idx2, V::none());
            }
            
            return (entry_len, r);
        }

        // Otherwise we've got ourselves a situation.
        let (len, r) = if cursor.offset > 0 {
            let mut rest = entry.truncate(cursor.offset);
            dec_delta_update(delta, &rest);

            let len = rest.len();
            if len <= replace_max {
                // Not so bad. Just splice in the replaced item. This will automatically try and
                // join the item to nearby items.
                let r = map_fn(&mut rest);
                self.insert(rest, dc, false, notify);
                (len, r)
            } else {
                // Ugh. We're modifying the middle of this item. We'll use splice_in_internal, which
                // does not try and join the updated item - since its more convenient, and we
                // probably can't join it to nearby items anyway.
                let remainder = rest.truncate(replace_max);
                let r = map_fn(&mut rest);
                cursor.offset = replace_max; // Cursor ends up after the item.
                let (leaf_idx, elem_idx) = self.splice_in_internal(
                    rest, Some(remainder),
                    cursor.leaf_idx, cursor.elem_idx + 1, delta,
                    false, notify
                );
                cursor.leaf_idx = leaf_idx;
                cursor.elem_idx = elem_idx;
                (replace_max, r)
            }
        } else {
            debug_assert!(entry_len > replace_max);
            // In this case, we need to cut the existing item down and modify the start of it.
            // There's a few ways to do this. The simplest is to just chop out the modified bit and
            // re-insert it.
            let mut e = entry.truncate_keeping_right(replace_max);
            dec_delta_update(delta, &e);
            // The cursor offset is already at 0.
            let r = map_fn(&mut e);
            self.insert(e, dc, false, notify);
            // splice_in will try and join the item to the previous item - which is what we want
            // here. And the cursor will be moved to right after the item in all cases.
            (replace_max, r)
        };

        // self.flush_delta_len(cursor.leaf_idx, delta_len);
        (len, r)
    }

    pub fn insert<N>(&mut self, item: V, DeltaCursor(cursor, delta): &mut DeltaCursor, notify_here: bool, notify: &mut N)
        where N: FnMut(V, LeafIdx)
    {
        debug_assert!(item.exists());
        let mut leaf_idx = cursor.leaf_idx;
        let mut elem_idx = cursor.elem_idx;
        let mut offset = cursor.offset;

        let node = &mut self[leaf_idx];
        debug_assert_ne!(offset, usize::MAX);

        let remainder = if offset == 0 && elem_idx > 0 {
            // Roll the cursor back to opportunistically see if we can append.
            elem_idx -= 1;
            offset = node.children[elem_idx].len(); // blerp could be cleaner.
            None
        } else if offset == node.children[elem_idx].len() || offset == 0 {
            None
        } else {
            // We could also roll back to the previous leaf node if offset == 0 and
            // elem_idx == 0 but when I tried it, it didn't make any difference in practice
            // because insert() is always called with stick_end.

            // Remainder is the trimmed off returned value.
            // splice the item into the current cursor location.
            let entry: &mut V = &mut node.children[elem_idx];
            let remainder = entry.truncate(offset);
            dec_delta_update(delta, &remainder);
            // We don't need to update cursor since its already where it needs to be.

            Some(remainder)
        };

        if offset != 0 {
            // We're at the end of an element. Try and append here.
            debug_assert_eq!(offset, node.children[elem_idx].len());
            // Try and append as much as we can after the current entry
            let cur_entry: &mut V = &mut node.children[elem_idx];
            if cur_entry.can_append(&item) {
                inc_delta_update(delta, &item);
                // flush_marker += next.content_len() as isize;
                if notify_here { notify(item, leaf_idx); }
                cur_entry.append(item);
                cursor.elem_idx = elem_idx;
                cursor.offset = cur_entry.len();

                if let Some(remainder) = remainder {
                    let (leaf_idx_2, elem_idx_2) = self.splice_in_internal(remainder, None, leaf_idx, elem_idx + 1, delta, notify_here, notify);
                    // If the remainder was inserted into a new item, we might need to update the
                    // cursor.
                    if leaf_idx_2 != leaf_idx {
                        if elem_idx_2 > 0 {
                            // This is a bit of a hack. Move the cursor to the item before the
                            // remainder.
                            cursor.leaf_idx = leaf_idx_2;
                            cursor.elem_idx = elem_idx_2 - 1;
                        } else {
                            // The remainder is on a subsequent element. This is fine, but now delta
                            // refers to the item the remainder is on, not the cursor element.
                            // So we need to flush it.
                            // TODO: Urgh this is gross. Rewrite me!
                            self.flush_delta_and_clear(leaf_idx_2, delta);
                        }
                    }
                }
                return;
            }

            // Insert in the next slot.

            elem_idx += 1; // NOTE: Cursor might point past the end of the node.
            // offset = 0; // Offset isn't used anymore anyway.

            // Try and prepend to the start of the next item.
            // This optimization improves performance when the user hits backspace. We end up
            // merging all the deleted elements together. This adds complexity in exchange for
            // making the tree simpler. (For real edit sequences (like the automerge-perf data
            // set) this gives about an 8% performance increase on an earlier version of this code)

            if remainder.is_none()
                // This is the same as the two lines below. TODO: Check which the compiler prefers.
                // && node.children.get(elem_idx).is_some_and(|v| v.exists())
                && elem_idx < node.children.len()
                && node.children[elem_idx].exists()
            {
                // It may be worth being more aggressive here. We're currently not trying this trick
                // when the cursor is at the end of the current node. That might be worth trying!
                let cur_entry = &mut node.children[elem_idx];
                if item.can_append(cur_entry) {
                    inc_delta_update(delta, &item);
                    // Always notify for the item itself.
                    if notify_here { notify(item, leaf_idx); }
                    // trailing_offset += item.len();
                    cursor.elem_idx = elem_idx;
                    cursor.offset = item.len();
                    cur_entry.prepend(item);
                    debug_assert!(remainder.is_none());
                    return;
                }
            }
        }

        cursor.offset = item.len();
        (leaf_idx, elem_idx) = self.splice_in_internal(item, remainder, leaf_idx, elem_idx, delta, notify_here, notify);
        cursor.leaf_idx = leaf_idx;
        cursor.elem_idx = elem_idx;
    }

    /// Splice in an item, and optionally remainder afterwards. Returns the (leaf_idx, elem_idx) of
    /// the inserted item, but NOT the remainder.
    fn splice_in_internal<N>(&mut self, item: V, remainder: Option<V>, mut leaf_idx: LeafIdx, mut elem_idx: usize, delta: &mut LenUpdate, notify_here: bool, notify: &mut N) -> (LeafIdx, usize)
        where N: FnMut(V, LeafIdx)
    {
        let space_needed = 1 + remainder.is_some() as usize;
        let (new_leaf_idx, new_elem_idx) = self.make_space_in_leaf_for(space_needed, leaf_idx, elem_idx, delta, notify);
        // Only call notify if either we're notifying in all cases, or if the item is inserted into
        // a different leaf than we were passed.
        let moved = new_leaf_idx != leaf_idx;
        if notify_here || moved { notify(item, new_leaf_idx); }

        (leaf_idx, elem_idx) = (new_leaf_idx, new_elem_idx);

        let leaf = &mut self.leaves[leaf_idx.0];
        inc_delta_update(delta, &item);
        leaf.children[elem_idx] = item;

        if let Some(remainder) = remainder {
            if moved { notify(remainder, leaf_idx); }
            inc_delta_update(delta, &remainder);
            leaf.children[elem_idx + 1] = remainder;
        }

        (leaf_idx, elem_idx)
    }

    fn flush_delta_len(&mut self, leaf_idx: LeafIdx, delta: LenUpdate) {
        if delta.is_empty() { return; }

        let mut idx = self.leaves[leaf_idx.0].parent;
        let mut child = leaf_idx.0;
        while !idx.is_root() {
            let n = &mut self.nodes[idx.0];
            let pos = n.idx_of_child(child);
            debug_assert!(pos < n.child_width.len());

            n.child_width[pos % n.child_width.len()].update_by(delta);

            child = idx.0;
            idx = n.parent;
        }

        self.total_len.update_by(delta);
    }

    #[inline]
    fn flush_delta_and_clear(&mut self, leaf_idx: LeafIdx, delta: &mut LenUpdate) {
        self.flush_delta_len(leaf_idx, take(delta));
    }

    // #[inline]
    // pub fn flush_cursor_delta(&mut self, cursor: MutContentCursor) {
    //     self.flush_delta_len(cursor.leaf_idx, cursor.delta);
    // }
    // #[inline]
    // fn flush_cursor_delta_and_clear(&mut self, cursor: &mut MutContentCursor) {
    //     self.flush_delta_len(cursor.inner.leaf_idx, take(&mut cursor.delta));
    // }


    fn make_space_in_leaf_for<F>(&mut self, space_wanted: usize, mut leaf_idx: LeafIdx, mut elem_idx: usize, delta_len: &mut LenUpdate, notify: &mut F) -> (LeafIdx, usize)
        where F: FnMut(V, LeafIdx)
    {
        assert!(space_wanted == 1 || space_wanted == 2);

        if self.leaves[leaf_idx.0].has_space(space_wanted) {
            let leaf = &mut self.leaves[leaf_idx.0];

            // Could scan to find the actual length of the children, then only memcpy that many. But
            // memcpy is cheap.
            leaf.children.copy_within(elem_idx..LEAF_CHILDREN - space_wanted, elem_idx + space_wanted);
        } else {
            self.flush_delta_and_clear(leaf_idx, delta_len);
            let new_node = self.split_leaf(leaf_idx, notify);

            if elem_idx >= LEAF_SPLIT_POINT {
                // We're inserting into the newly created node.
                (leaf_idx, elem_idx) = (new_node, elem_idx - LEAF_SPLIT_POINT);
            }

            let leaf = &mut self.leaves[leaf_idx.0];
            leaf.children.copy_within(elem_idx..LEAF_SPLIT_POINT, elem_idx + space_wanted);
        }
        (leaf_idx, elem_idx)
    }

    /// This method always splits a node in the middle. This isn't always optimal, but its simpler.
    /// TODO: Try splitting at the "correct" point and see if that makes any difference to
    /// performance.
    fn split_node(&mut self, old_idx: NodeIdx, children_are_leaves: bool) -> NodeIdx {
        // Split a full internal node into 2 nodes.
        let new_node_idx = self.nodes.len();
        // println!("split node -> {new_node_idx}");
        let old_node = &mut self.nodes[old_idx.0];
        // The old leaf must be full before we split it.
        debug_assert!(old_node.is_full());

        let split_size: LenPair = old_node.child_width[NODE_SPLIT_POINT..].iter().copied().sum();

        // eprintln!("split node {:?} -> {:?} + {:?} (leaves: {children_are_leaves})", old_idx, old_idx, new_node_idx);
        // eprintln!("split start {:?} / {:?}", &old_node.children[..NODE_SPLIT_POINT], &old_node.children[NODE_SPLIT_POINT..]);

        let mut new_node = ContentNode {
            child_indexes: [usize::MAX; NODE_CHILDREN],
            child_width: [LenPair::default(); NODE_CHILDREN],
            parent: NodeIdx(usize::MAX), // Overwritten below.
        };

        new_node.child_indexes[0..NODE_SPLIT_POINT].copy_from_slice(&old_node.child_indexes[NODE_SPLIT_POINT..]);
        new_node.child_width[0..NODE_SPLIT_POINT].copy_from_slice(&old_node.child_width[NODE_SPLIT_POINT..]);
        old_node.child_indexes[NODE_SPLIT_POINT..].fill(usize::MAX);

        if children_are_leaves {
            for idx in &new_node.child_indexes[..NODE_SPLIT_POINT] {
                self.leaves[*idx].parent = NodeIdx(new_node_idx);
            }
        } else {
            for idx in &new_node.child_indexes[..NODE_SPLIT_POINT] {
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
            // We'll make a new root.
            let parent = self.create_new_root_node(old_idx.0, new_node_idx, split_size);
            self.nodes[old_idx.0].parent = parent;
            self.nodes[new_node_idx].parent = parent
        } else {
            let parent = old_node.parent;
            self.nodes[new_node_idx].parent = self.split_child_of_node(parent, old_idx.0, new_node_idx, split_size, false);
        }

        NodeIdx(new_node_idx)
    }

    #[must_use]
    fn split_child_of_node(&mut self, mut node_idx: NodeIdx, child_idx: usize, new_child_idx: usize, stolen_len: LenPair, children_are_leaves: bool) -> NodeIdx {
        let mut node = &mut self[node_idx];

        // Where will the child go? I wonder if the compiler can do anything smart with this...
        let mut child_pos = node.child_indexes
            .iter()
            .position(|idx| { *idx == child_idx })
            .unwrap() % node.child_width.len();

        if node.is_full() {
            let new_node = self.split_node(node_idx, children_are_leaves);

            if child_pos >= NODE_SPLIT_POINT {
                // Actually we're inserting into the new node.
                child_pos -= NODE_SPLIT_POINT;
                node_idx = new_node;
            }
            // Technically this only needs to be reassigned in the if() above, but reassigning it
            // in all cases is necessary for the borrowck.
            node = &mut self[node_idx];
        }

        node.child_width[child_pos] -= stolen_len;

        let insert_pos = (child_pos + 1) % node.child_width.len();

        // dbg!(&node);
        // println!("insert_into_node n={:?} after_child {after_child} pos {insert_pos}, new_child {:?}", node_idx, new_child);


        // Could scan to find the actual length of the children, then only memcpy that many. But
        // memcpy is cheap.
        node.child_indexes.copy_within(insert_pos..NODE_CHILDREN - 1, insert_pos + 1);
        node.child_indexes[insert_pos] = new_child_idx;

        node.child_width.copy_within(insert_pos..NODE_CHILDREN - 1, insert_pos + 1);
        node.child_width[insert_pos] = stolen_len;

        node_idx
    }

    fn split_leaf<F>(&mut self, old_idx: LeafIdx, notify: &mut F) -> LeafIdx
        where F: FnMut(V, LeafIdx)
    {
        // This function splits a full leaf node in the middle, into 2 new nodes.
        // The result is two nodes - old_leaf with items 0..N/2 and new_leaf with items N/2..N.

        let old_height = self.height;
        // TODO: This doesn't currently use the pool of leaves that we have so carefully prepared.

        let new_leaf_idx = self.leaves.len(); // Weird instruction order for borrowck.
        let mut old_leaf = &mut self.leaves[old_idx.0];
        // debug_assert!(old_leaf.is_full());
        debug_assert!(!old_leaf.has_space(2));

        let mut new_size = LenPair::default();
        for v in &old_leaf.children[LEAF_SPLIT_POINT..] {
            // This index isn't actually valid yet, but because we've borrowed self mutably
            // here, the borrow checker will make sure that doesn't matter.
            if v.exists() {
                notify(v.clone(), LeafIdx(new_leaf_idx));
                new_size += v.content_len_pair();
            } else { break; } // TODO: This probably makes the code slower?
        }

        let parent = if old_height == 0 {
            // Insert this leaf into a new root node. This has to be the first node.
            let parent = self.create_new_root_node(old_idx.0, new_leaf_idx, new_size);
            old_leaf = &mut self.leaves[old_idx.0]; // borrowck
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

            parent = self.split_child_of_node(parent, old_idx.0, new_leaf_idx, new_size, true);
            old_leaf = &mut self.leaves[old_idx.0]; // borrowck.
            old_leaf.parent = parent; // If the node was split, we may have a new parent.
            parent
        };

        // The old leaf must be full before we split it.
        // debug_assert!(old_leaf.data.last().unwrap().is_some());

        let mut new_leaf = ContentLeaf {
            children: [V::none(); LEAF_CHILDREN],
            next_leaf: old_leaf.next_leaf,
            parent,
        };

        // We'll steal the second half of the items in OLD_LEAF.
        // Could use ptr::copy_nonoverlapping but this is safe, and they compile to the same code.
        new_leaf.children[0..LEAF_SPLIT_POINT].copy_from_slice(&old_leaf.children[LEAF_SPLIT_POINT..]);

        // Needed to mark that these items are gone now.
        old_leaf.children[LEAF_SPLIT_POINT..].fill(V::none());

        // old_leaf.upper_bound = split_lv;
        old_leaf.next_leaf = LeafIdx(new_leaf_idx);

        self.leaves.push(new_leaf);
        debug_assert_eq!(self.leaves.len() - 1, new_leaf_idx);

        LeafIdx(new_leaf_idx)
    }

    /// This function blindly assumes the item is definitely in the recursive children.
    ///
    /// Returns (child index, len_remaining).
    fn find_pos_in_node<const IS_CUR: bool>(node: &ContentNode, mut at_pos: usize) -> (usize, usize) {
        for i in 0..NODE_CHILDREN {
            let width = node.child_width[i].get::<IS_CUR>();
            if at_pos <= width { return (node.child_indexes[i], at_pos); }
            at_pos -= width;
        }
        panic!("Position not in node");
    }

    // /// This function blindly assumes the item is definitely in the recursive children.
    // ///
    // /// Returns (child index, relative position, requested len remaining).
    // fn find_pos_in_node_2<const IS_CUR: bool>(node: &ContentNode, at_pos: usize) -> (usize, LenPair) {
    //     let mut offset = LenPair::default();
    //     for i in 0..NODE_CHILDREN {
    //         let width = node.child_width[i];
    //         if at_pos <= offset.get::<IS_CUR>() + width.get::<IS_CUR>() {
    //             return (node.child_indexes[i], offset);
    //         }
    //         offset += width;
    //     }
    //     panic!("Position not in node");
    // }

    /// This function blindly assumes the item is definitely in the recursive children.
    ///
    /// Returns (child index, relative end pos of the index, len remaining).
    fn find_cur_pos_in_node(node: &ContentNode, mut at_cur_pos: usize) -> (usize, usize, usize) {
        let mut end_pos_offset = 0;
        for i in 0..NODE_CHILDREN {
            let width = node.child_width[i];
            // if at_cur_pos <= width.cur {
            if at_cur_pos < width.cur {
                return (node.child_indexes[i], end_pos_offset, at_cur_pos);
            }
            at_cur_pos -= width.cur;
            end_pos_offset += width.end;
        }
        panic!("Position not in node");
    }

    /// Returns (index, offset).
    fn find_pos_in_leaf<const IS_CUR: bool>(leaf: &ContentLeaf<V>, mut at_pos: usize) -> (usize, usize) {
        for i in 0..LEAF_CHILDREN {
            let width = leaf.children[i].content_len::<IS_CUR>();
            if at_pos <= width { return (i, at_pos); }
            at_pos -= width;
        }
        panic!("Position not in leaf");
    }

    /// Returns (index, end_pos, offset).
    fn find_cur_pos_in_leaf(leaf: &ContentLeaf<V>, mut at_cur_pos: usize) -> (usize, usize, usize) {
        let mut end_pos = 0;
        for i in 0..LEAF_CHILDREN {
            let width = leaf.children[i].content_len_pair();
            // if at_cur_pos <= width.cur {
            if at_cur_pos < width.cur {
                // We return the end pos of the offset position, not just the start of this child.
                end_pos += if leaf.children[i].takes_up_space::<false>() { at_cur_pos } else { 0 };
                return (i, end_pos, at_cur_pos);
            }
            at_cur_pos -= width.cur;
            end_pos += width.end;
        }
        panic!("Position not in leaf");
    }

    // /// Returns (index, relative position in leaf, offset in item).
    // fn find_pos_in_leaf_2<const IS_CUR: bool>(leaf: &ContentLeaf<V>, at_pos: usize) -> (usize, LenPair, usize) {
    //     let mut offset = LenPair::default();
    //     for i in 0..LEAF_CHILDREN {
    //         let width = leaf.children[i].content_len_pair();
    //         if at_pos <= offset.get::<IS_CUR>() + width.get::<IS_CUR>() {
    //             if width.end { offset.end +=
    //             return (i, offset);
    //         }
    //         // if at_pos <= width { return (i, at_pos); }
    //         // at_pos -= width;
    //         offset += width;
    //     }
    //     panic!("Position not in leaf");
    // }

    // fn check_cursor_at(&self, cursor: ContentCursor, lv: LV, at_end: bool) {
    //     assert!(cfg!(debug_assertions));
    //     let leaf = &self.leaves[cursor.leaf_idx.0];
    //     let lower_bound = leaf.bounds[cursor.elem_idx];
    //
    //     let next = cursor.elem_idx + 1;
    //     let upper_bound = if next < LEAF_CHILDREN && leaf.bounds[next] != usize::MAX {
    //         leaf.bounds[next]
    //     } else {
    //         self.leaf_upper_bound(leaf)
    //     };
    //     assert!(lv >= lower_bound);
    //
    //     if at_end {
    //         assert_eq!(lv, upper_bound);
    //     } else {
    //         assert!(lv < upper_bound, "Cursor is not within expected bound. Expect {lv} / upper_bound {upper_bound}");
    //     }
    // }

    // fn cursor_to_next(&self, cursor: &mut ContentCursor) {
    //     let leaf = &self.leaves[cursor.leaf_idx.0];
    //     let next_idx = cursor.elem_idx + 1;
    //     if next_idx >= LEAF_CHILDREN || leaf.bounds[next_idx] == usize::MAX {
    //         cursor.elem_idx = 0;
    //         cursor.leaf_idx = leaf.next_leaf;
    //     } else {
    //         cursor.elem_idx += 1;
    //     }
    // }

    // Returns the end length slid past
    fn slide_cursor_to_next_content<F: FlushUpdate>(&mut self, cursor: &mut ContentCursor, flush: &mut F) -> usize {
        let mut leaf = &self.leaves[cursor.leaf_idx.0];
        let e = &leaf.children[cursor.elem_idx];
        // if cursor.offset < e.len()
        if !e.exists() || (e.takes_up_space::<true>() && cursor.offset < e.len()) { return 0; }

        let mut end_slide_len = if e.takes_up_space::<false>() {
            e.len() - cursor.offset
        } else { 0 };
        cursor.elem_idx += 1;
        cursor.offset = 0;

        loop {
            // This walks linearly through the nodes. It would be "big-O faster" to walk up and down
            // the tree in this case, but I think this will usually be faster in practice.
            if cursor.elem_idx >= leaf.children.len() || !leaf.children[cursor.elem_idx].exists() {
                // Go to next leaf.
                let next_leaf = leaf.next_leaf;
                if next_leaf.exists() {
                    flush.flush_and_clear(self, cursor.leaf_idx);
                    // self.flush_cursor_delta_and_clear(cursor);
                    cursor.leaf_idx = next_leaf;
                    leaf = &self.leaves[cursor.leaf_idx.0];
                    cursor.elem_idx = 0;
                } else {
                    // The cursor points past the end of the list. !@#?
                    panic!("Unreachable?");
                }
            }

            let e = &leaf.children[cursor.elem_idx];
            if e.takes_up_space::<true>() {
                break;
            }

            end_slide_len += e.content_len_end();
            cursor.elem_idx += 1;
        }

        end_slide_len
    }


    pub fn cursor_at_start(&mut self) -> ContentCursor {
        // I'm never using the cached cursor here because it may have slid to the next content.
        if let Some((_, cursor, delta)) = self.cursor.take() {
            self.flush_delta_len(cursor.leaf_idx, delta);
            // self.flush_cursor_delta(cursor)
        }

        // This is always valid because there is always at least 1 leaf item, and its always
        // the first item in the tree.
        ContentCursor::default().into()
    }

    pub fn cursor_at_start_nothing_emplaced(&self) -> ContentCursor {
        debug_assert!(self.cursor.is_none());
        ContentCursor::default().into()
    }

    pub fn mut_cursor_at_start(&mut self) -> DeltaCursor {
        DeltaCursor(self.cursor_at_start(), Default::default())
    }

    // fn cursor_at_content_pos<const IS_CUR: bool>(&self, content_pos: usize) -> (LenUpdate, ContentCursor) {

    /// Create and return a cursor pointing to (just before) the specified content item. The item
    /// must take up space (cur pos size).
    ///
    /// Returns a tuple containing the end pos and the new cursor.
    ///
    /// We never "stick end" - ie, the cursor is moved to the start of the next item with actual
    /// content.
    pub fn mut_cursor_before_cur_pos(&mut self, content_pos: usize) -> (usize, DeltaCursor) {
        if let Some((mut pos, mut cursor, mut delta)) = self.cursor.take() {
            if pos.cur == content_pos {
                pos.end += self.slide_cursor_to_next_content(&mut cursor, &mut delta);
                return (pos.end, DeltaCursor(cursor, delta));
            }

            // Throw the old cursor away.
            self.flush_delta_len(cursor.leaf_idx, delta);
        }

        // Make a cursor by descending from the root.
        let mut idx = self.root;
        let mut end_pos = 0;
        let mut content_pos_remaining = content_pos;

        for _h in 0..self.height {
            let n = &self.nodes[idx];

            let (child_idx, rel_end_pos, cpr) = Self::find_cur_pos_in_node(n, content_pos_remaining);
            end_pos += rel_end_pos;
            content_pos_remaining = cpr;
            idx = child_idx;
        }

        // let (elem_idx, offset) = Self::find_pos_in_leaf::<IS_CUR>(&self.leaves[idx], pos_remaining);
        let (elem_idx, rel_end_pos, offset) = Self::find_cur_pos_in_leaf(&self.leaves[idx], content_pos_remaining);
        // We're guaranteed that the item under elem_idx has size in CUR. Well, unless the tree is empty.
        debug_assert!(
            (content_pos == 0 && self.is_empty())
            || self.leaves[idx].children[elem_idx].takes_up_space::<true>());

        (
            end_pos + rel_end_pos,
            DeltaCursor(ContentCursor {
                leaf_idx: LeafIdx(idx),
                elem_idx,
                offset,
            }, Default::default())
        )
    }

    // fn advance_cursor_by_len(&self, cursor: &mut MutCursor, len: usize) {
    //
    // }

    pub(crate) fn emplace_cursor(&mut self, pos: LenPair, DeltaCursor(cursor, delta): DeltaCursor) {
        assert!(self.cursor.is_none());
        self.cursor = Some((pos, cursor, delta));

        if cfg!(debug_assertions) {
            let actual_pos = self.cursor.clone().unwrap().1.get_pos(self);
            assert_eq!(pos, actual_pos);
        }
    }

    pub(crate) fn cursor_before_item(&self, id: V::Item, leaf_idx: LeafIdx) -> ContentCursor where V: Searchable {
        // debug_assert!(self.cursor.is_none());

        let leaf = &self[leaf_idx];

        let mut elem_idx = usize::MAX;
        let mut offset = usize::MAX;
        for (idx, e) in leaf.children.iter().enumerate() {
            if let Some(off) = e.get_offset(id) {
                elem_idx = idx;
                offset = off;
                break;
            }
        }

        assert_ne!(elem_idx, usize::MAX, "Could not find element in leaf");

        ContentCursor { leaf_idx, elem_idx, offset }
    }

    pub(crate) fn mut_cursor_before_item(&mut self, id: V::Item, leaf_idx: LeafIdx) -> (DeltaCursor, Option<LenPair>)
        where V: Searchable
    {
        if let Some((mut pos, mut cursor, delta)) = self.cursor.take() {
            let (item, cur_offset) = cursor.get_item(self);
            if let Some(actual_offset) = item.get_offset(id) {
                // The cursor already points to the item.

                // TODO: Rewrite this to use wrapping_add and non-branching code.
                if item.takes_up_space::<false>() {
                    pos.end -= cur_offset;
                    pos.end += actual_offset;
                }
                if item.takes_up_space::<true>() {
                    pos.cur -= cur_offset;
                    pos.cur += actual_offset;
                }
                cursor.offset = actual_offset;

                debug_assert_eq!(cursor.get_pos(self), pos);

                return (DeltaCursor(cursor, delta), Some(pos));
            }

            // Throw the old cursor away.
            self.flush_delta_len(cursor.leaf_idx, delta);
        }

        // Otherwise just make a fresh cursor.
        (DeltaCursor(self.cursor_before_item(id, leaf_idx), LenUpdate::default()), None)
    }

    fn first_leaf(&self) -> LeafIdx {
        if cfg!(debug_assertions) {
            // dbg!(&self);
            let mut idx = self.root;
            for _ in 0..self.height {
                idx = self.nodes[idx].child_indexes[0];
            }
            debug_assert_eq!(idx, 0);
        }
        LeafIdx(0)
    }

    pub fn is_empty(&self) -> bool {
        let first_leaf = &self.leaves[self.first_leaf().0];
        !first_leaf.children[0].exists()
    }

    // pub fn count_items(&self) -> usize {
    //     let mut count = 0;
    //     let mut leaf = &self[self.first_leaf()];
    //     loop {
    //         // SIMD should make this fast.
    //         count += leaf.bounds.iter().filter(|b| **b != usize::MAX).count();
    //
    //         // There is always at least one leaf.
    //         if leaf.is_last() { break; }
    //         else {
    //             leaf = &self[leaf.next_leaf];
    //         }
    //     }
    //
    //     count
    // }

    /// Iterate over the contents of the index. Note the index tree may contain extra entries
    /// for items within the range, with a value of V::default.
    pub fn iter(&self) -> ContentTreeIter<V> {
        ContentTreeIter {
            tree: self,
            // If the iterator points to a valid leaf, it should never be empty. This makes the
            // iteration logic simpler.
            leaf_idx: if self.is_empty() { LeafIdx::default() } else { self.first_leaf() },
            elem_idx: 0,
        }
    }

    pub fn iter_rle(&self) -> impl Iterator<Item = V> + '_ {
        self.iter().merge_spans()
    }

    pub fn to_vec(&self) -> Vec<V> {
        self.iter().collect::<Vec<_>>()
    }

    pub fn count_entries(&self) -> usize {
        let mut count = 0;
        for (_idx, children) in self.iter_leaves() {
            for c in children.iter() {
                if !c.exists() { break; }
                count += 1;
            }
        }
        count
    }


    /// On the walk this returns the size of all children (recursive) and the expected next visited
    /// leaf idx.
    fn dbg_check_walk_internal(&self, idx: usize, height: usize, mut expect_next_leaf_idx: LeafIdx, expect_parent: NodeIdx) -> (LenPair, LeafIdx, Option<LenUpdate>) {
        if height == self.height {
            assert!(idx < self.leaves.len());
            // The item is a leaf node. Check that the previous leaf is correct.
            let leaf = &self.leaves[idx];
            assert_eq!(leaf.parent, expect_parent);
            assert_eq!(idx, expect_next_leaf_idx.0);

            let leaf_size: LenPair = leaf.children.iter()
                .filter(|c| c.exists())
                .map(|c| c.content_len_pair())
                .sum();

            let mut delta = None;
            if let Some((_pos, cursor, c_delta)) = self.cursor.as_ref() {
                if cursor.leaf_idx.0 == idx {
                    delta = Some(*c_delta);
                }
            }

            // assert_eq!(leaf_size, expect_size);

            (leaf_size, leaf.next_leaf, delta)
        } else {
            assert!(idx < self.nodes.len());
            let node = &self.nodes[idx];
            assert_eq!(node.parent, expect_parent);

            let mut actual_node_size = LenPair::default();
            let mut delta = None;

            for i in 0..node.child_indexes.len() {
                let child_idx = node.child_indexes[i];
                if child_idx == usize::MAX {
                    assert!(i >= 1); // All nodes have at least 1 child.
                    // All subsequent child_indexes must be usize::MAX.
                    assert!(node.child_indexes[i..].iter().all(|i| *i == usize::MAX));
                    break;
                }

                let (actual_child_size, idx, d) = self.dbg_check_walk_internal(child_idx, height + 1, expect_next_leaf_idx, NodeIdx(idx));
                expect_next_leaf_idx = idx;

                if d.is_some() {
                    assert!(replace(&mut delta, d).is_none());
                }

                let mut expect_child_size = node.child_width[i];
                expect_child_size.update_by(d.unwrap_or_default()); // The stored child width is wrong by d.
                assert_eq!(actual_child_size, expect_child_size);

                actual_node_size += expect_child_size;
            }
            // assert_eq!(actual_node_size, expect_size);

            (actual_node_size, expect_next_leaf_idx, delta)
        }
    }

    fn dbg_check_walk(&self) {
        let (actual_len, last_next_ptr, delta) = self.dbg_check_walk_internal(self.root, 0, LeafIdx(0), NodeIdx(usize::MAX));
        // dbg!(actual_len, delta, self.total_len);
        let mut total_len = self.total_len;
        total_len.update_by(delta.unwrap_or_default());
        assert_eq!(actual_len, total_len);

        assert_eq!(last_next_ptr.0, usize::MAX);
    }


    #[allow(unused)]
    pub(crate) fn dbg_check(&self) {
        // Invariants:
        // - Except for the root item, all leaves must have at least 1 data entry.
        // - The next pointers iterate through all items in sequence
        // - There is at least 1 leaf node
        // - The width of all items is correct.

        // This code does 2 traversals of the data structure:
        // 1. We walk the leaves by following next_leaf pointers in each leaf node
        // 2. We recursively walk the tree

        // Walk the tree structure in the nodes.
        self.dbg_check_walk();

        // Walk the leaves in sequence.
        let mut leaves_visited = 0;
        let mut leaf_idx = self.first_leaf();
        loop {
            let leaf = &self[leaf_idx];
            leaves_visited += 1;

            if leaf_idx == self.first_leaf() {
                // First leaf. This can be empty - but only if the whole data structure is empty.
                if !leaf.children[0].exists() {
                    assert!(!leaf.next_leaf.exists());
                    assert_eq!(self.total_len, LenPair::default());
                }
            } else {
                assert!(leaf.children[0].exists(), "Only the first leaf can be empty");
            }

            // The size is checked in dbg_check_walk().

            if leaf.is_last() { break; }
            else {
                let next_leaf = &self[leaf.next_leaf];
                // assert!(next_leaf.bounds[0] > prev);
                // assert_eq!(leaf.upper_bound, next_leaf.bounds[0]);
            }
            leaf_idx = leaf.next_leaf;
        }
        assert_eq!(leaves_visited, self.leaves.len());

        // let mut leaf_pool_size = 0;
        // let mut i = self.free_leaf_pool_head;
        // while i.0 != usize::MAX {
        //     leaf_pool_size += 1;
        //     i = self.leaves[i.0].next_leaf;
        // }
        // assert_eq!(leaves_visited + leaf_pool_size, self.leaves.len());
        //
        // if self.height == 0 {
        //     assert!(self.root < self.leaves.len());
        // } else {
        //     assert!(self.root < self.nodes.len());
        // }


        // let (lv, cursor) = self.cursor.get();
        // self.check_cursor_at(cursor, lv, false);
    }

    pub(crate) fn iter_leaves(&self) -> ContentLeafIter<'_, V> {
        ContentLeafIter {
            tree: self,
            leaf_idx: self.first_leaf(),
        }
    }
}

#[derive(Debug)]
pub struct ContentTreeIter<'a, V: Content> {
    tree: &'a ContentTree<V>,
    leaf_idx: LeafIdx,
    // leaf: &'a ContentLeaf<V>,
    elem_idx: usize,
}

impl<'a, V: Content> Iterator for ContentTreeIter<'a, V> {
    // type Item = (DTRange, V);
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        // if self.leaf_idx.0 == usize::MAX {
        debug_assert!(self.elem_idx < LEAF_CHILDREN);
        if self.leaf_idx.0 >= self.tree.leaves.len() || self.elem_idx >= LEAF_CHILDREN { // Avoid a bounds check.
            return None;
        }

        let leaf = &self.tree[self.leaf_idx];

        let data = leaf.children[self.elem_idx].clone();

        self.elem_idx += 1;
        if self.elem_idx >= LEAF_CHILDREN || !leaf.children[self.elem_idx].exists() {
            self.leaf_idx = leaf.next_leaf;
            self.elem_idx = 0;
        }

        Some(data)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ContentLeafIter<'a, V: Content> {
    tree: &'a ContentTree<V>,
    leaf_idx: LeafIdx,
}

impl<'a, V: Content> Iterator for ContentLeafIter<'a, V> {
    // type Item = (LeafIdx, &'a ContentLeaf<V>);
    type Item = (LeafIdx, &'a [V; LEAF_CHILDREN]);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.leaf_idx.exists() { return None; }

        let cur_leaf = self.leaf_idx;
        let leaf = &self.tree[cur_leaf];
        self.leaf_idx = leaf.next_leaf;

        Some((cur_leaf, &leaf.children))
    }
}


#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::ops::Range;
    use std::pin::Pin;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use content_tree::{ContentLength, ContentTreeRaw, FullMetricsUsize};
    use rle::{HasLength, HasRleKey, MergableSpan, SplitableSpan, SplitableSpanHelpers};
    use crate::list_fuzzer_tools::fuzz_multithreaded;
    use crate::ost::{LeafIdx, LenPair, LenUpdate};
    use super::{Content, ContentTree};

    /// This is a simple span object for testing.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestRange {
        id: u32,
        len: u32,
        is_activated: bool,
        exists: bool,
    }

    impl Default for TestRange {
        fn default() -> Self {
            Self {
                id: u32::MAX,
                len: u32::MAX,
                is_activated: false,
                exists: false,
            }
        }
    }

    impl HasLength for TestRange {
        fn len(&self) -> usize { self.len as usize }
    }
    impl SplitableSpanHelpers for TestRange {
        fn truncate_h(&mut self, at: usize) -> Self {
            assert!(at > 0 && at < self.len as usize);
            assert!(self.exists);
            let other = Self {
                id: self.id + at as u32,
                len: self.len - at as u32,
                is_activated: self.is_activated,
                exists: self.exists,
            };
            self.len = at as u32;
            other
        }

        fn truncate_keeping_right_h(&mut self, at: usize) -> Self {
            let mut other = *self;
            *self = other.truncate(at);
            other
        }
    }
    impl MergableSpan for TestRange {
        fn can_append(&self, other: &Self) -> bool {
            assert!(self.exists);
            other.id == self.id + self.len && other.is_activated == self.is_activated
        }

        fn append(&mut self, other: Self) {
            assert!(self.can_append(&other));
            self.len += other.len;
        }

        fn prepend(&mut self, other: Self) {
            assert!(other.can_append(self));
            self.len += other.len;
            self.id = other.id;
        }
    }

    impl HasRleKey for TestRange {
        fn rle_key(&self) -> usize {
            self.id as usize
        }
    }

    impl Content for TestRange {
        fn exists(&self) -> bool {
            self.exists
        }

        fn takes_up_space<const IS_CUR: bool>(&self) -> bool {
            if !self.exists { false }
            else if IS_CUR { self.is_activated }
            else { true }
        }

        fn none() -> Self {
            Self::default()
        }
    }

    fn null_notify<V>(_v: V, _idx: LeafIdx) {}
    fn debug_notify<V: Debug>(v: V, idx: LeafIdx) {
        println!("Notify {:?} at {:?}", v, idx);
    }
    fn panic_notify<V>(_v: V, _idx: LeafIdx) {
        panic!("Notify erroneously called")
    }

    #[test]
    fn simple_inserts() {
        let mut tree: ContentTree<TestRange> = ContentTree::new();
        tree.dbg_check();

        // let mut cursor = tree.cursor_at_content_pos::<true>(0);
        let mut cursor = tree.mut_cursor_at_start();

        tree.insert_notify(TestRange {
            id: 123,
            len: 10,
            is_activated: false,
            exists: true,
        }, &mut cursor, &mut debug_notify);
        // tree.dbg_check(); // checking here doesn't work because we have an outstanding cursor.
        // dbg!(&cursor);

        cursor.0.offset = 2;
        tree.insert_notify(TestRange {
            id: 321,
            len: 20,
            is_activated: true,
            exists: true,
        }, &mut cursor, &mut debug_notify);
        tree.emplace_cursor((20, 2 + 20).into(), cursor);
        // tree.flush_cursor(cursor);
        tree.dbg_check();

        // dbg!(&cursor);

        // dbg!(&tree);

        // dbg!(tree.iter().collect::<Vec<_>>());
        assert!(tree.iter().eq([
            TestRange { id: 123, len: 2, is_activated: false, exists: true },
            TestRange { id: 321, len: 20, is_activated: true, exists: true },
            TestRange { id: 125, len: 8, is_activated: false, exists: true },
        ].into_iter()));
    }

    #[test]
    fn replace_item() {
        let mut tree: ContentTree<TestRange> = ContentTree::new();
        // let mut cursor = tree.cursor_at_start();
        let mut cursor = tree.mut_cursor_at_start();

        tree.insert_notify(TestRange {
            id: 123,
            len: 10,
            is_activated: true,
            exists: true,
        }, &mut cursor, &mut null_notify);
        tree.emplace_cursor((10, 10).into(), cursor);
        tree.dbg_check();

        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(2);
        assert_eq!(end_pos, 2);
        // assert_eq!(tree.get_cursor_pos(&cursor), LenPair::new(2, 2));
        // cursor.offset = 2;
        let (len, _r) = tree.mutate_entry(&mut cursor, 5, &mut panic_notify, |e| {
            assert_eq!(e.id, 125);
            assert_eq!(e.len, 5);
            e.is_activated = false;
        });
        assert_eq!(len, 5);
        tree.emplace_cursor((2, 7).into(), cursor);

        tree.dbg_check();

        // dbg!(tree.get_cursor_pos(&cursor));
        // dbg!(tree.iter().collect::<Vec<_>>());
        assert!(tree.iter().eq([
            TestRange { id: 123, len: 2, is_activated: true, exists: true },
            TestRange { id: 125, len: 5, is_activated: false, exists: true },
            TestRange { id: 130, len: 3, is_activated: true, exists: true },
        ].into_iter()));

        // Now re-activate part of the middle item.
        // let (end_pos, mut cursor) = tree.mut_cursor_at_end_pos(5);
        // I can't get a cursor where I want it. This is dirty as anything.

        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(1);
        assert_eq!(end_pos, 1);
        cursor.0.elem_idx += 1; cursor.0.offset = 3; // hack hack hack.
        let (len, _r) = tree.mutate_entry(&mut cursor, 5, &mut panic_notify, |e| {
            // dbg!(&e);
            e.is_activated = true;
        });
        assert!(tree.iter().eq([
            TestRange { id: 123, len: 2, is_activated: true, exists: true },
            TestRange { id: 125, len: 3, is_activated: false, exists: true },
            TestRange { id: 128, len: 5, is_activated: true, exists: true },
        ].into_iter()));
        assert_eq!(len, 2);
        // dbg!(tree.iter().collect::<Vec<_>>());

        tree.emplace_cursor((4, 7).into(), cursor);
        tree.dbg_check();
    }


//     use std::ops::Range;
//     use std::pin::Pin;
//     use rand::prelude::SmallRng;
//     use rand::{Rng, SeedableRng, thread_rng};
//     use content_tree::{ContentTreeRaw, null_notify, RawPositionMetricsUsize};
//     use crate::list_fuzzer_tools::fuzz_multithreaded;
//     use super::*;
//
//     #[derive(Debug, Copy, Clone, Eq, PartialEq)]
//     enum Foo { A, B, C }
//     use Foo::*;
//
//     #[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
//     struct X(usize);
//     impl IndexContent for X {
//         fn try_append(&mut self, offset: usize, other: &Self, other_len: usize) -> bool {
//             debug_assert!(offset > 0);
//             debug_assert!(other_len > 0);
//             &self.at_offset(offset) == other
//         }
//
//         fn at_offset(&self, offset: usize) -> Self {
//             X(self.0 + offset)
//         }
//
//         fn eq(&self, other: &Self, _upto_len: usize) -> bool {
//             self.0 == other.0
//         }
//     }
//
//     #[test]
//     fn empty_tree_is_empty() {
//         let tree = ContentTree::<X>::new();
//
//         tree.dbg_check_eq(&[]);
//     }
//
//     #[test]
//     fn overlapping_sets() {
//         let mut tree = ContentTree::new();
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
//         // assert_eq!(tree.to_vec(), &[((5..10).into(), Some(A))]);
//         // dbg!(&tree.leaves[0]);
//         tree.set_range((5..11).into(), X(200));
//         tree.dbg_check_eq(&[RleDRun::new(5..11, X(200))]);
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[
//             RleDRun::new(5..10, X(100)),
//             RleDRun::new(10..11, X(205)),
//         ]);
//
//         tree.set_range((2..50).into(), X(300));
//         // dbg!(&tree.leaves);
//         tree.dbg_check_eq(&[RleDRun::new(2..50, X(300))]);
//
//     }
//
//     #[test]
//     fn split_values() {
//         let mut tree = ContentTree::new();
//         tree.set_range((10..20).into(), X(100));
//         tree.set_range((12..15).into(), X(200));
//         tree.dbg_check_eq(&[
//             RleDRun::new(10..12, X(100)),
//             RleDRun::new(12..15, X(200)),
//             RleDRun::new(15..20, X(105)),
//         ]);
//     }
//
//     #[test]
//     fn set_inserts_1() {
//         let mut tree = ContentTree::new();
//
//         tree.set_range((5..10).into(), X(100));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(100))]);
//
//         tree.set_range((5..10).into(), X(200));
//         tree.dbg_check_eq(&[RleDRun::new(5..10, X(200))]);
//
//         // dbg!(&tree);
//         tree.set_range((15..20).into(), X(300));
//         // dbg!(tree.iter().collect::<Vec<_>>());
//         tree.dbg_check_eq(&[
//             RleDRun::new(5..10, X(200)),
//             RleDRun::new(15..20, X(300)),
//         ]);
//
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//     }
//
//     #[test]
//     fn set_inserts_2() {
//         let mut tree = ContentTree::new();
//         tree.set_range((5..10).into(), X(100));
//         tree.set_range((1..5).into(), X(200));
//         // dbg!(&tree);
//         tree.dbg_check_eq(&[
//             RleDRun::new(1..5, X(200)),
//             RleDRun::new(5..10, X(100)),
//         ]);
//         dbg!(&tree.leaves[0]);
//
//         tree.set_range((3..8).into(), X(300));
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//         tree.dbg_check_eq(&[
//             RleDRun::new(1..3, X(200)),
//             RleDRun::new(3..8, X(300)),
//             RleDRun::new(8..10, X(103)),
//         ]);
//     }
//
//     #[test]
//     fn split_leaf() {
//         let mut tree = ContentTree::new();
//         // Using 10, 20, ... so they don't merge.
//         tree.set_range(10.into(), X(100));
//         tree.dbg_check();
//         tree.set_range(20.into(), X(200));
//         tree.set_range(30.into(), X(100));
//         tree.set_range(40.into(), X(200));
//         tree.dbg_check();
//         // dbg!(&tree);
//         tree.set_range(50.into(), X(100));
//         tree.dbg_check();
//
//         // dbg!(&tree);
//         // dbg!(tree.iter().collect::<Vec<_>>());
//
//         tree.dbg_check_eq(&[
//             RleDRun::new(10..11, X(100)),
//             RleDRun::new(20..21, X(200)),
//             RleDRun::new(30..31, X(100)),
//             RleDRun::new(40..41, X(200)),
//             RleDRun::new(50..51, X(100)),
//         ]);
//     }
//

    impl ContentLength for TestRange {
        fn content_len(&self) -> usize { self.content_len_cur() }

        fn content_len_at_offset(&self, offset: usize) -> usize {
            if self.is_activated { offset } else { 0 }
        }
    }

    fn random_entry(rng: &mut SmallRng) -> TestRange {
        TestRange {
            id: rng.gen_range(0..10),
            len: rng.gen_range(1..10),
            is_activated: rng.gen_bool(0.5),
            exists: true,
        }
    }

    fn fuzz(seed: u64, mut verbose: bool) {
        verbose = verbose; // suppress mut warning.
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree = ContentTree::<TestRange>::new();
        // let mut check_tree: Pin<Box<ContentTreeRaw<RleDRun<Option<i32>>, RawPositionMetricsUsize>>> = ContentTreeRaw::new();
        let mut check_tree: Pin<Box<ContentTreeRaw<TestRange, FullMetricsUsize>>> = ContentTreeRaw::new();
        const START_JUNK: u32 = 1_000_000;
        check_tree.replace_range_at_offset(0, TestRange {
            id: START_JUNK,
            len: START_JUNK,
            is_activated: false,
            exists: false,
        });

        for _i in 0..1000 {
            if verbose { println!("i: {}", _i); }
            // println!("i: {}", _i);

            // if _i == 31 {
            //     println!("asdf");
            //     // verbose = true;
            // }

            if tree.total_len().cur == 0 || rng.gen_bool(0.6) {

                // tree.dbg_check();
                // Insert something.
                let cur_pos = rng.gen_range(0..=tree.total_len().cur);
                let item = random_entry(&mut rng);

                if verbose { println!("inserting {:?} at {}", item, cur_pos); }

                // Insert into check tree
                {
                    // check_tree.check();
                    // check_tree.print_ptr_tree();
                    let mut cursor = check_tree.mut_cursor_at_content_pos(cur_pos, true);
                    cursor.insert(item);
                    assert_eq!(cursor.count_content_pos(), cur_pos + item.content_len_cur());
                }

                // Insert into our tree.
                {
                    // if verbose { dbg!(&tree); }

                    // This code mirrors the equivalent code in merge.rs
                    let (end_pos, mut cursor) = if cur_pos == 0 {
                        (0, tree.mut_cursor_at_start())
                    } else {
                        // // Equivalent of getting a cursor with stick_end: true.
                        // let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        // tree.emplace_cursor((cur_pos - 1, end_pos).into(), cursor);
                        //
                        // let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        // tree.cursor_inc_offset(&mut cursor);
                        // tree.emplace_cursor((cur_pos, end_pos + 1).into(), cursor);


                        let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(cur_pos - 1);
                        cursor.0.inc_offset(&tree);
                        (end_pos + 1, cursor)
                    };
                    // let mut cursor = tree.cursor_at_content_pos::<false>(pos);
                    // dbg!(&cursor);
                    let pre_pos = LenPair::new(cur_pos, end_pos);
                    tree.insert_notify(item, &mut cursor, &mut null_notify);
                    // dbg!(&cursor);

                    // if verbose { dbg!(&tree); }
                    // tree.dbg_check();

                    // This will check that the position makes sense.
                    tree.emplace_cursor(pre_pos + item.content_len_pair(), cursor);

                    // let post_pos = tree.get_cursor_pos(&cursor);
                    // // dbg!(pre_pos, item.content_len_pair(), post_pos);
                    // assert_eq!(pre_pos + item.content_len_pair(), post_pos);
                }
            } else {

                let gen_range = |rng: &mut SmallRng, range: Range<usize>| {
                    if range.is_empty() { range.start }
                    else { rng.gen_range(range) }
                };

                // Modify something.
                //
                // Note this has a subtle sort-of flaw: The first item we touch will always be
                // active. But we might make some later items active again in the range.
                let modify_len = gen_range(&mut rng, 1..20.min(tree.total_len().cur));
                // let modify_len = 1;
                debug_assert!(modify_len <= tree.total_len().cur);
                let pos = gen_range(&mut rng, 0..tree.total_len().cur - modify_len);
                let new_is_active = rng.gen_bool(0.5);

                // The chunking of the two tree implementations might differ, so we'll run modify
                // in a loop.
                {
                    let mut len_remaining = modify_len;
                    let mut cursor = check_tree.mut_cursor_at_content_pos(pos, false);
                    while len_remaining > 0 {
                        let (changed, _) = cursor.mutate_single_entry_notify(len_remaining, content_tree::null_notify, |e| {
                            e.is_activated = new_is_active;
                        });
                        cursor.roll_to_next_entry();
                        len_remaining -= changed;
                    }
                }

                {
                    let mut len_remaining = modify_len;
                    // let mut cursor = tree.cursor_at_content_pos::<false>(pos);
                    let (end_pos, mut cursor) = tree.mut_cursor_before_cur_pos(pos);
                    let mut cursor_pos = LenPair::new(pos, end_pos);

                    while len_remaining > 0 {
                        // let pre_pos = tree.get_cursor_pos(&cursor);
                        let (changed, len_here) = tree.mutate_entry(&mut cursor, len_remaining, &mut null_notify, |e| {
                            e.is_activated = new_is_active;
                            e.content_len_pair()
                        });
                        cursor_pos += len_here;
                        // let post_pos = tree.get_cursor_pos(&cursor);
                        // assert_eq!(pre_pos.end + changed, post_pos.end);
                        len_remaining -= changed;
                    }

                    tree.emplace_cursor(cursor_pos, cursor);
                }
            }

            // Check that both trees have identical content.
            tree.dbg_check();
            assert!(check_tree.iter().filter(|e| e.id < START_JUNK)
                .eq(tree.iter_rle()));
        }
    }

    #[test]
    fn content_tree_fuzz_once() {
        // fuzz(3322, true);
        // for seed in 8646911284551352000..8646911284551353000 {
        //
        //     fuzz(seed, true);
        // }
        fuzz(0, true);
    }

    #[test]
    #[ignore]
    fn content_tree_fuzz_forever() {
        fuzz_multithreaded(u64::MAX, |seed| {
            if seed % 100 == 0 {
                println!("Iteration {}", seed);
            }
            fuzz(seed, false);
        })
    }
}





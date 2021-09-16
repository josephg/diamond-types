
use crate::{NodeLeaf, ContentTraits, TreeIndex, Cursor, ContentTreeRaw};
use rle::{Searchable, MergeIter, merge_items};

/// Iterator for all the items inside the entries. Unlike entry iteration we use the offset here.
#[derive(Debug)]
pub struct ItemIterator<'a, E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize>(pub Cursor<'a, E, I, IE, LE>);

impl<'a, E: ContentTraits + Searchable, I: TreeIndex<E>, const IE: usize, const LE: usize> Iterator for ItemIterator<'a, E, I, IE, LE> {
    type Item = E::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // I'll set idx to an invalid value
        if self.0.inner.idx == usize::MAX {
            None
        } else {
            let entry = self.0.get_raw_entry();
            let len = entry.len();
            let item = entry.at_offset(self.0.inner.offset);
            self.0.inner.offset += 1;

            if self.0.inner.offset >= len {
                // Skip to the next entry for the next query.
                let has_next = self.0.inner.next_entry();
                if !has_next {
                    // We're done.
                    self.0.inner.idx = usize::MAX;
                }
            }
            Some(item)
        }
    }
}

/// Iterator for whole nodes in the tree. This lets you iterate through chunks of items efficiently.
#[derive(Debug)]
pub struct NodeIter<'a, E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize>(Option<&'a NodeLeaf<E, I, IE, LE>>);

impl<'a, E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> Iterator for NodeIter<'a, E, I, IE, LE> {
    type Item = &'a NodeLeaf<E, I, IE, LE>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf) = self.0 {
            let this_ref = self.0;
            self.0 = leaf.next.map(|ptr| unsafe { ptr.as_ref() });
            this_ref
        } else { None }
    }
}

impl<E: ContentTraits, I: TreeIndex<E>, const IE: usize, const LE: usize> ContentTreeRaw<E, I, IE, LE> {
    /// Iterate through all the items "raw" - which is to say, without merging anything.
    ///
    /// This is different from iter() because in some editing situations the tree will not be
    /// perfectly flattened. That is, it may be possible to merge some items in the tree. This
    /// iterator method will not merge anything, and instead just iterate through all items as they
    /// are stored.
    ///
    /// Whether specific items are merged or not is an implementation detail, and should not be
    /// relied upon by your application. If you expect all mergable items to be merged, use iter().
    pub fn raw_iter(&self) -> Cursor<E, I, IE, LE> { self.cursor_at_start() }

    /// Iterate through all entries in the content tree. This iterator will yield all entries
    /// merged according to the methods in SplitableSpan.
    pub fn iter(&self) -> MergeIter<Cursor<E, I, IE, LE>> { merge_items(self.cursor_at_start()) }

    pub fn item_iter(&self) -> ItemIterator<E, I, IE, LE> {
        ItemIterator(self.raw_iter())
    }

    pub fn node_iter(&self) -> NodeIter<E, I, IE, LE> {
        let leaf_ref = self.leaf_at_start();
        NodeIter(if leaf_ref.num_entries > 0 { Some(leaf_ref) } else { None })
    }
}

#[cfg(test)]
mod test {
    use crate::ContentTree;
    use crate::testrange::TestRange;

    #[test]
    fn node_iter_smoke_test() {
        let mut tree = ContentTree::new();

        let mut iter = tree.node_iter();
        assert!(iter.next().is_none());

        tree.push(TestRange { id: 1000, len: 100, is_activated: true });

        let mut iter = tree.node_iter();
        let first = iter.next().unwrap();
        assert_eq!(first.num_entries, 1);
        assert!(iter.next().is_none());
    }
}
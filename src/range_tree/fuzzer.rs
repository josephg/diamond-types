use crate::list::Order;
use crate::splitable_span::SplitableSpan;
use crate::range_tree::{EntryTraits, CRDTItem, EntryWithContent, RangeTree, RawPositionIndex, FullIndex, null_notify};
use rand::prelude::*;
use crate::merge_iter::merge_items;

/// This is a simple span object for testing.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TestRange {
    pub order: Order,
    pub len: u32,
    pub is_activated: bool,
}

impl Default for TestRange {
    fn default() -> Self {
        Self {
            order: Order::MAX,
            len: u32::MAX,
            is_activated: false
        }
    }
}

impl SplitableSpan for TestRange {
    fn len(&self) -> usize { self.len as usize }
    fn truncate(&mut self, at: usize) -> Self {
        let other = Self {
            order: self.order + at as u32,
            len: self.len - at as u32,
            is_activated: self.is_activated
        };
        self.len = at as u32;
        other
    }
    fn can_append(&self, other: &Self) -> bool {
        other.order == self.order + self.len && other.is_activated == self.is_activated
    }

    fn append(&mut self, other: Self) {
        assert!(self.can_append(&other));
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        assert!(other.can_append(&self));
        self.len += other.len;
        self.order = other.order;
    }
}

impl EntryTraits for TestRange {
    type Item = ();

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let mut other = *self;
        *self = other.truncate(at);
        other
    }

    fn contains(&self, _loc: Self::Item) -> Option<usize> { unimplemented!() }
    fn is_valid(&self) -> bool { self.order != Order::MAX }
    fn at_offset(&self, _offset: usize) -> Self::Item { () }
}

impl CRDTItem for TestRange {
    fn is_activated(&self) -> bool {
        self.is_activated
    }

    fn mark_activated(&mut self) {
        assert!(!self.is_activated);
        self.is_activated = true;
    }

    fn mark_deactivated(&mut self) {
        assert!(self.is_activated);
        self.is_activated = false;
    }
}

impl EntryWithContent for TestRange {
    fn content_len(&self) -> usize {
        if self.is_activated { self.len() } else { 0 }
    }
}

fn random_entry(rng: &mut SmallRng) -> TestRange {
    TestRange {
        order: rng.gen_range(0..10),
        len: rng.gen_range(1..10),
        is_activated: rng.gen_bool(0.5)
    }
}

fn insert_into_list(list: &mut Vec<TestRange>, pos: usize, entry: TestRange) {
    let mut idx = 0;
    let mut cur_pos = 0;

    loop {
        if cur_pos == pos {
            list.insert(idx, entry);
            break;
        } else {
            let e = &list[idx];

            if cur_pos + e.len() > pos {
                // Split the item.
                let remainder = list[idx].truncate(pos - cur_pos);
                list.insert(idx + 1, entry);
                list.insert(idx + 2, remainder);
                break;
            }

            idx += 1;
            cur_pos += e.len();
        }
    }
}

#[test]
fn random_edits() {
    let mut rng = SmallRng::seed_from_u64(20);

    // So for this test we'll make a range tree and a list, make random changes to both, and make
    // sure the content is always the same.

    for _i in 0..1000 {
        println!("i {}", _i);
        // TestRange is overkill for this, but eh.
        let mut tree = RangeTree::<TestRange, FullIndex>::new();
        let mut list = vec![];

        for _j in 0..100 {
            // println!("j {}", _j);
            // if list.is_empty() || rng.gen_bool(0.33) {
            if list.is_empty() || true {
                // Insert something.
                let pos = rng.gen_range(0..=tree.len().0);
                let item = random_entry(&mut rng);

                // println!("inserting {:?} at {}", item, pos);
                // dbg!(&tree);
                let cursor = tree.cursor_at_offset_pos(pos as usize, true);
                tree.insert(cursor, item, null_notify);
                insert_into_list(&mut list, pos as usize, item);
            }

            // dbg!(&tree);
            tree.check();

            let list_len = list.iter().fold(0usize, |sum, item| sum + item.len());
            assert_eq!(list_len, tree.count.0 as usize);

            let list_content = list.iter().fold(0usize, |sum, item| sum + item.content_len());
            assert_eq!(list_content, tree.count.1 as usize);

            let tree_iter = merge_items(tree.iter());
            let list_iter = merge_items(list.iter().copied());
            assert!(tree_iter.eq(list_iter));
        }
    }
}
use crate::list::Order;
use rle::splitable_span::SplitableSpan;
use crate::content_tree::*;
use rand::prelude::*;
use rle::merge_iter::merge_items;
use crate::entry::Toggleable;

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
        assert!(at > 0 && at < self.len as usize);
        let other = Self {
            order: self.order + at as u32,
            len: self.len - at as u32,
            is_activated: self.is_activated
        };
        self.len = at as u32;
        other
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let mut other = *self;
        *self = other.truncate(at);
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

impl Toggleable for TestRange {
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

impl ContentLength for TestRange {
    fn content_len(&self) -> usize {
        if self.is_activated { self.len() } else { 0 }
    }
}

impl Searchable for TestRange {
    type Item = (Order, bool);

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        if self.is_activated == loc.1 && loc.0 >= self.order && loc.0 < (self.order + self.len) {
            Some((loc.0 - self.order) as usize)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        (offset as Order + self.order, self.is_activated)
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

fn delete_in_list(list: &mut Vec<TestRange>, pos: usize, mut del_span: usize) {
    let mut idx = 0;
    let mut cur_pos = 0;

    while del_span > 0 && idx < list.len() {
        let e_len = list[idx].len();
        if cur_pos == pos {
            if e_len > del_span {
                list[idx].truncate_keeping_right(del_span);
                break;
            } else {
                del_span -= e_len;
                list.remove(idx);
                // And continue keeping the current index.
            }
        } else {
            if cur_pos + e_len > pos {
                // Split the item.
                let mut remainder = list[idx].truncate(pos - cur_pos);
                if del_span < remainder.len() {
                    remainder.truncate_keeping_right(del_span);
                    list.insert(idx + 1, remainder);
                    return;
                } else {
                    // Discard r1.
                    del_span -= remainder.len();
                }
            }

            cur_pos += list[idx].len();
            idx += 1;
        }
    }
}

fn replace_in_list(list: &mut Vec<TestRange>, pos: usize, entry: TestRange) {
    // Wheee testing laziness!
    delete_in_list(list, pos, entry.len());
    insert_into_list(list, pos, entry);
}

fn random_edits_once(verbose: bool, iterations: usize) {
    let mut rng = SmallRng::seed_from_u64(20);

    // So for this test we'll make a range tree and a list, make random changes to both, and make
    // sure the content is always the same.

    for _i in 0..iterations {
        if verbose { println!("i {}", _i); }
        // TestRange is overkill for this, but eh.
        let mut tree = ContentTree::<TestRange, FullIndex, DEFAULT_IE, DEFAULT_LE>::new();
        let mut list = vec![];
        let mut expected_len = 0;

        for _j in 0..200 {
            if verbose { println!("  j {} / i {}", _j, _i); }
            if list.is_empty() || rng.gen_bool(0.33) {
                // Insert something.
                let pos = rng.gen_range(0..=tree.len().0);
                let item = random_entry(&mut rng);

                if verbose { println!("inserting {:?} at {}", item, pos); }
                // dbg!(&tree);
                let mut cursor = tree.cursor_at_offset_pos(pos as usize, true);
                unsafe {
                    assert_eq!(cursor.count_pos().0, pos);
                    cursor.check();

                    tree.insert(&mut cursor, item, null_notify);
                    assert_eq!(cursor.count_pos().0, pos + item.len);
                    cursor.check();
                }

                insert_into_list(&mut list, pos as usize, item);

                expected_len += item.len();
            } else if tree.count.0 > 10 && rng.gen_bool(0.5) {
                // Modify something.
                let item = random_entry(&mut rng);
                // let pos = rng.gen_range(0..tree.count.0 - item.len);
                let pos = rng.gen_range(0..=tree.count.0);

                if verbose {
                    println!("Replacing {} entries at position {} with {:?}", item.len(), pos, item);
                }
                let mut cursor = tree.cursor_at_offset_pos(pos as usize, true);
                unsafe {
                    assert_eq!(cursor.count_pos().0, pos);
                    cursor.check();
                    tree.replace_range(&mut cursor, item, null_notify);
                    assert_eq!(cursor.count_pos().0, pos + item.len);
                    cursor.check();
                }

                replace_in_list(&mut list, pos as usize, item);

                // this could be cleaned up but eh.
                let amt_deleted = usize::min(item.len(), expected_len - pos as usize);
                expected_len = expected_len - amt_deleted + item.len();
            } else {
                // Delete something
                assert!(tree.count.0 > 0);

                // Delete up to 20 items, but not more than we have in the document!
                // let del_span = rng.gen_range(1..=u32::min(tree.count.0, 20));
                let del_span = rng.gen_range(0..=20);
                // let pos = rng.gen_range(0..=tree.count.0 - del_span);
                let pos = rng.gen_range(0..=tree.count.0);

                if verbose {
                    println!("Deleting {} entries at position {} (size {})", pos, del_span, expected_len);
                }
                let mut cursor = tree.cursor_at_offset_pos(pos as usize, true);
                unsafe {
                    assert_eq!(cursor.count_pos().0, pos);
                    cursor.check();

                    tree.delete(&mut cursor, del_span as _, null_notify);
                    assert_eq!(cursor.count_pos().0, pos);
                    cursor.check();
                }

                delete_in_list(&mut list, pos as usize, del_span as usize);

                // expected_len -= del_span as usize;
                let amt_deleted = usize::min(expected_len - pos as usize, del_span as usize);
                expected_len -= amt_deleted;
            }

            tree.check();

            let list_len = list.iter().fold(0usize, |sum, item| sum + item.len());
            assert_eq!(expected_len, list_len);
            assert_eq!(expected_len, tree.count.0 as usize);

            let list_content = list.iter().fold(0usize, |sum, item| sum + item.content_len());
            assert_eq!(list_content, tree.count.1 as usize);

            let tree_iter = merge_items(tree.iter());
            let list_iter = merge_items(list.iter().copied());
            assert!(tree_iter.eq(list_iter));
        }
    }
}

#[test]
fn random_edits() {
    random_edits_once(false, 300);
}
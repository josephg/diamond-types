
// There's 3 states a position component can be in:
// - Not inserted (yet), with a postlen
// - Inserted (and in the document)
// - Inserted then deleted

use content_tree::{ContentLength, ContentTreeWithIndex, FullIndex};
use rle::{Searchable, SplitableSpan};

use crate::list::ListCRDT;
use crate::list::time::positionmap::PositionComponent::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum PositionComponent {
    NotInsertedYet,
    Inserted,
    Deleted(u32) // Storing the number of times this has been deleted. >0.
}

// It would be nicer to just use RleRun but I want to customize
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
struct PositionRun {
    val: PositionComponent,
    len: usize // This is the full length that we take up in the final document
}

impl Default for PositionComponent {
    fn default() -> Self { NotInsertedYet }
}

impl PositionRun {
    fn new(val: PositionComponent, len: usize) -> Self {
        Self { val, len }
    }
}

impl SplitableSpan for PositionRun {
    fn len(&self) -> usize { self.len }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        Self { val: self.val, len: remainder }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.val == other.val
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

impl ContentLength for PositionRun {
    fn content_len(&self) -> usize {
        // This is the amount of space we take up right now.
        if self.val == Inserted { self.len } else { 0 }
    }
}

type PositionMap = ContentTreeWithIndex<PositionRun, FullIndex>;

impl ListCRDT {
    pub fn foo(&self) {
        // let mut p = PositionMap::new();
        //
        // // TODO: This is something we should cache somewhere.
        // let total_post_len: usize = self.range_tree.raw_iter().map(|e| e.len()).sum();
        // p.push(PositionRun::new(NotInsertedYet, total_post_len));
        //
        // for walk in self.txns.txn_spanning_tree_iter() {
        //     for _range in walk.retreat {
        //         unimplemented!();
        //     }
        //
        //     for _range in walk.advance_rev {
        //         unimplemented!();
        //     }
        //
        //     let mut r = walk.consume;
        //     while r.start < r.end {
        //
        //     }
        // }
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn positionrun_is_splitablespan() {
        test_splitable_methods_valid(PositionRun::new(NotInsertedYet, 5));
        test_splitable_methods_valid(PositionRun::new(Inserted, 5));
        test_splitable_methods_valid(PositionRun::new(Deleted(1), 5));

        assert!(PositionRun::new(Deleted(1), 1)
            .can_append(&PositionRun::new(Deleted(1), 2)));
        assert!(!PositionRun::new(Deleted(1), 1)
            .can_append(&PositionRun::new(Deleted(999), 2)));
    }
}
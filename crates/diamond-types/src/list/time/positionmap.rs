
// There's 3 states a position component can be in:
// - Not inserted (yet), with a postlen
// - Inserted (and in the document)
// - Inserted then deleted

use content_tree::{ContentLength, ContentTreeWithIndex, FullIndex};
use rle::SplitableSpan;

use crate::list::ot::positional::InsDelTag;
use crate::list::time::positionmap::PositionMapComponent::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum PositionMapComponent {
    NotInsertedYet,
    Inserted,
    Deleted,
}

// It would be nicer to just use RleRun but I want to customize
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub(super) struct PositionRun {
    pub(super) val: PositionMapComponent,
    pub(super) len: usize // This is the full length that we take up in the final document
}

impl Default for PositionMapComponent {
    fn default() -> Self { NotInsertedYet }
}

impl From<PositionMapComponent> for InsDelTag {
    fn from(c: PositionMapComponent) -> Self {
        match c {
            NotInsertedYet => panic!("Invalid component for conversion"),
            Inserted => InsDelTag::Ins,
            Deleted => InsDelTag::Del,
        }
    }
}
impl From<InsDelTag> for PositionMapComponent {
    fn from(c: InsDelTag) -> Self {
        match c {
            InsDelTag::Ins => Inserted,
            InsDelTag::Del => Deleted,
        }
    }
}

impl PositionRun {
    pub(crate) fn new(val: PositionMapComponent, len: usize) -> Self {
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

pub(super) type PositionMap = ContentTreeWithIndex<PositionRun, FullIndex>;

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn positionrun_is_splitablespan() {
        test_splitable_methods_valid(PositionRun::new(NotInsertedYet, 5));
        test_splitable_methods_valid(PositionRun::new(Inserted, 5));
        test_splitable_methods_valid(PositionRun::new(Deleted, 5));

        // assert!(PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(1), 2)));
        // assert!(!PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(999), 2)));
    }
}
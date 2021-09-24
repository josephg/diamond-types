
// There's 3 states a position component can be in:
// - Not inserted (yet), with a postlen
// - Inserted (and in the document)
// - Inserted then deleted

use content_tree::{ContentLength, ContentTreeWithIndex, FullIndex};
use rle::SplitableSpan;

use crate::list::time::positionmap::MapTag::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum MapTag {
    NotInsertedYet,
    Inserted,
    Upstream,
}

// It would be nicer to just use RleRun but I want to customize
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub(super) struct PositionRun {
    pub(super) tag: MapTag,
    pub(super) final_len: usize, // This is the full length that we take up in the final document
    pub(super) content_len: usize, // 0 if we're in the NotInsertedYet state.
}

impl Default for MapTag {
    fn default() -> Self { MapTag::NotInsertedYet }
}

// impl From<InsDelTag> for PositionMapComponent {
//     fn from(c: InsDelTag) -> Self {
//         match c {
//             InsDelTag::Ins => Inserted,
//             InsDelTag::Del => Deleted,
//         }
//     }
// }

impl PositionRun {
    // pub(crate) fn new(val: PositionMapComponent, len: usize) -> Self {
    //     Self { val, content_len: len, final_len: 0 }
    // }
    pub(crate) fn new_void(len: usize) -> Self {
        Self { tag: MapTag::NotInsertedYet, final_len: len, content_len: 0 }
    }

    pub(crate) fn new_ins(len: usize) -> Self {
        Self { tag: MapTag::Inserted, final_len: len, content_len: len }
    }

    pub(crate) fn new_upstream(final_len: usize, content_len: usize) -> Self {
        Self { tag: MapTag::Upstream, final_len, content_len }
    }
}

impl SplitableSpan for PositionRun {
    fn len(&self) -> usize { self.final_len }

    fn truncate(&mut self, at: usize) -> Self {
        assert_ne!(self.tag, MapTag::Upstream);

        let remainder = self.final_len - at;
        self.final_len = at;

        match self.tag {
            NotInsertedYet => {
                Self { tag: self.tag, final_len: remainder, content_len: 0 }
            }
            Inserted => {
                self.content_len = at;
                Self { tag: self.tag, final_len: remainder, content_len: remainder }
            }
            Upstream => unreachable!()
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.tag == other.tag
    }

    fn append(&mut self, other: Self) {
        self.final_len += other.final_len;
        self.content_len += other.content_len;
    }
}

impl ContentLength for PositionRun {
    fn content_len(&self) -> usize {
        self.content_len
        // This is the amount of space we take up right now.
        // if self.tag == Inserted { self.final_len } else { 0 }
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        match self.tag {
            NotInsertedYet => 0,
            Inserted => offset,
            Upstream => panic!("Cannot service call")
        }
    }
}

pub(super) type PositionMap = ContentTreeWithIndex<PositionRun, FullIndex>;

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn positionrun_is_splitablespan() {
        test_splitable_methods_valid(PositionRun::new_void(5));
        test_splitable_methods_valid(PositionRun::new_ins(5));
        // test_splitable_methods_valid(PositionRun::new(Deleted, 5));

        // assert!(PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(1), 2)));
        // assert!(!PositionRun::new(Deleted(1), 1)
        //     .can_append(&PositionRun::new(Deleted(999), 2)));
    }
}
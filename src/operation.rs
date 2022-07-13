use rle::{HasLength, MergableSpan};
use crate::{Op, OpContents};

impl HasLength for OpContents {
    fn len(&self) -> usize {
        match self {
            OpContents::Text(metrics) => metrics.len(),
            _ => 1,
        }
    }
}

impl MergableSpan for OpContents {
    fn can_append(&self, other: &Self) -> bool {
        match (self, other) {
            (OpContents::Text(a), OpContents::Text(b)) => a.can_append(b),
            _ => false,
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (OpContents::Text(a), OpContents::Text(b)) => a.append(b),
            _ => panic!("Cannot append"),
        }
    }
}

impl HasLength for Op {
    fn len(&self) -> usize { self.contents.len() }
}

impl MergableSpan for Op {
    fn can_append(&self, other: &Self) -> bool {
        self.crdt_id == other.crdt_id && self.contents.can_append(&other.contents)
    }

    fn append(&mut self, other: Self) {
        self.contents.append(other.contents)
    }
}
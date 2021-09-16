use content_tree::{SplitableSpan, ContentTree};
use std::ops::Range;

#[derive(Debug, Clone, Copy, Default)]
struct RleRange {
    // We can't embed a Range because it doesn't implement Copy. And Copy is needed for ContentTree.
    start: usize,
    end: usize,
}

impl From<Range<usize>> for RleRange {
    fn from(range: Range<usize>) -> Self {
        RleRange { start: range.start, end: range.end }
    }
}

impl SplitableSpan for RleRange {
    fn len(&self) -> usize { self.end - self.start }

    fn truncate(&mut self, at: usize) -> Self {
        let old_end = self.end;

        // Truncate self
        self.end = self.start + at;

        // And return
        RleRange { start: self.end, end: old_end }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.end == other.start
    }

    fn append(&mut self, other: Self) {
        self.end = other.end;
    }
}

fn main() {
    let mut list = ContentTree::new();
    list.push((0..15).into());
    list.push((15..20).into());
    // Both items are merged!
    println!("List contains {:?}", list.iter().collect::<Vec<RleRange>>());
    // List contains [RleRange { start: 0, end: 20 }]

    list.insert_at_offset(5, (100..101).into());
    println!("List contains {:#?}", list.iter().collect::<Vec<RleRange>>());
    // List contains [
    //     RleRange { start: 0, end: 5, },
    //     RleRange { start: 100, end: 101, },
    //     RleRange { start: 5, end: 20, },
    // ]
}
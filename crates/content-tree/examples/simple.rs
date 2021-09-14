use content_tree::{SplitableSpan, ContentTree};

#[derive(Debug, Clone, Copy, Default)]
struct BitRun {
    value: bool,
    len: usize
}

impl SplitableSpan for BitRun {
    fn len(&self) -> usize { self.len }
    fn truncate(&mut self, at: usize) -> Self {
        let remainder = self.len - at;
        self.len = at;
        BitRun { value: self.value, len: remainder }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.value == other.value
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

fn main() {
    let mut list = ContentTree::new();
    list.push(BitRun { value: false, len: 10 });

    list.insert_at_offset(5, BitRun { value: true, len: 2 });
    println!("List contains {:?}", list.iter().collect::<Vec<BitRun>>());
}
# Content Tree

This is a fancy data structure for managing packed run-length encoded data. Its like:

- [A rope](https://en.wikipedia.org/wiki/Rope_(data_structure)), except for arbitrary data.
- A list / array, except with inline run-length encoding and supporting efficient inserts and removals at any location
- A b-tree, except instead of each item having a fixed key, each item is indexed according to its current position in the list. The position of each item will move when other items are inserted or deleted before the current item.

## Features

- High performance based on a reasonably well optimized b-tree
- Inline RLE compaction. Items are automatically split and merged as needed.
- Support for custom internal indexing of items
- Support for external indexing - that is, using an external data structure to find items in the b-tree.

## Example

Lets say you want to store RLE runs of bits. First make a struct which implements SplitableSpan (and Copy and Default):

```rust
use content_tree::SplitableSpan;

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
```

Then you can make a ContentTree using your type:

```rust
let mut list = ContentTree::new();
list.push(BitRun { value: false, len: 10 });
list.insert_at_offset(5, BitRun { value: true, len: 2 });
println!("List contains {:?}", list.iter().collect::<Vec<BitRun>>());

// List contains [
//   BitRun { value: false, len: 5 },
//   BitRun { value: true, len: 2 },
//   BitRun { value: false, len: 5 }
// ]
```

---

This code was implemented as part of [diamond types](https://github.com/josephg/diamond-types).
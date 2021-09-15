# Content Tree

This is a fancy data structure for managing packed run-length encoded data. Its like:

- [A rope](https://en.wikipedia.org/wiki/Rope_(data_structure)), except for arbitrary data.
- A list / array, except with inline run-length encoding and supporting efficient inserts and removals at any location
- A b-tree, except instead of each item having a fixed key, each item is indexed according to its current position in the list. The position of each item will move when other items are inserted or deleted before the current item.

## Features

- High performance based on a reasonably well optimized b-tree (capable of handling millions of edits per second)
- Inline RLE compaction. Items are automatically split and merged as needed.
- Support for custom internal indexing of items
- Support for external indexing - that is, using an external data structure to find items in the b-tree.

## Example

Lets say you want to store RLE runs of bits. We could make our own SplitableSpan RLE type for our data, but in this case we can use the builtin `RleRun<bool>` type:

```rust
use content_tree::{ContentTree, RleRun};

fn main() {
    let mut list = ContentTree::new();
    list.push(RleRun { val: false, len: 10 });

    // Insert in the middle (at offset 5) in the run of 10 items:
    list.insert_at_offset(5, RleRun { val: true, len: 2 });
    println!("List contains {:?}", list.iter().collect::<Vec<RleRun<bool>>>());

    // List contains [
    //  RleRun { val: false, len: 5 },
    //  RleRun { val: true, len: 2 },
    //  RleRun { val: false, len: 5 }
    // ]
}
```

But you aren't limited to simple runs of items. Lets suppose you want to store auto-compacting ranges of identifiers. We can make a custom struct for that, so long as it implements:

- [SplitableSpan](https://docs.rs/rle/0.1.0/rle/trait.SplitableSpan.html)
- Copy
- Default (though this constraint could be removed with some work. Open an issue if this is bothersome).

For a range type, our implementation would look something like this:

```rust
#[derive(Debug, Clone, Copy, Default)]
struct RleRange {
    // Sadly we can't just embed a Range because it doesn't implement
    // Copy. And Copy is needed for ContentTree.
    start: usize,
    end: usize,
}

impl SplitableSpan for RleRange {
    fn len(&self) -> usize { self.end - self.start }

    fn truncate(&mut self, at: usize) -> Self {
        let old_end = self.end;

        // Truncate self to [start..start+at)
        self.end = self.start + at;

        // And return the trimmed remainder
        RleRange { start: self.end, end: old_end }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.end == other.start
    }

    fn append(&mut self, other: Self) {
        self.end = other.end;
    }
}
```

Then you can make a ContentTree using your type:

```rust
fn main() {
    let mut list = ContentTree::new();
    list.push(RleRange { start: 0, end: 15 });
    list.push(RleRange { start: 15, end: 20 });
    
    // Both items are automatically merged!
    println!("List contains {:?}", list.iter().collect::<Vec<RleRange>>());
    // List contains [RleRange { start: 0, end: 20 }]
}
```

See [examples/custom_entry.rs](examples/custom_entry.rs) for a fully worked example.


## TODO

- Add a cache for the last cursor position
- Move to using a trait for list configuration
- Handle notify functions in a cleaner way - preferably passed in via a generic parameter on the type
- Consider removing internal list sizes

---

This code was implemented as part of [diamond types](https://github.com/josephg/diamond-types).
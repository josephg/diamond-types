# Marker tree

This is a B-Tree implementation to store packed CRDT markers efficiently, for list based CRDTs (like arrays and text documents).

The tree is designed to optimize the two slow operations performed for list CRDTs, namely:

- Convert a list offset (or text document position) to an immutable predecessor ID
- Look up a CRDT ID (defined by an *(agent,seq)* pair) and discover the current document position

These operations stay fast (*O(log n)*) even when items are being inserted and deleted.

Note the marker tree only stores CRDT IDs. It does not store the CRDT content itself. To use the marker tree, the document contents will usually be stored in an adjacent data structure. Updates to the document contents must update both data structures.


### Entries

The marker tree is composed of *entries*, which reference the items in the CRDT in document order:

```
struct Entry {
    loc: CRDTLocation {
        agent: u16, // Locally assigned ID for each unique editor
        seq: u32
    },
    len: i32, // Range length. Negative if the range is deleted
}
```

Each entry references a range of sequential items in the list. To collapse multiple list items into an entry:

- All list items must have the same agent ID
- The items must have sequential sequence numbers (eg 10, 11, 12, 13)
- Either all the items must still exist in the document, or all the items must have been deleted.

The `loc.seq` field references the first sequence number in the range, and `len` names the length of the contained range.

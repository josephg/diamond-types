# Diamond Types list type internals

This sounds weird, but a CRDT stores information about *space* and *time*.

The *spatial component* of a CRDT is its data - what does the document actually look like (at some moment in time)?

The *temporal component* of a CRDT is its history of changes. What happened, when? By whom? How did the document change from point in time A to B?

Diamond types (like automerge) stores information about both the temporal and spatial dimensions of a document.


## Space

Diamond types stores a list of entries internally, one for each item in the document. This is stored in document-sorted order.

Conceptually, this is a list of Yjs entries. For a plain text document, it would look like this:

```rust
struct YjsEntry {
    id: Id,

    // Note that after an entry is deleted, we still keep the entry!
    value: Option<char>,

    // Needed to order remote edits. Based on Yjs (with small changes)
    origin_left: Id,
    origin_right: Id,
}

type DocData = Vec<YjsEntry>;
```

But you won't find this structure in the codebase. The actual implementation of this structure is a little different because of a handful of optimizations operating in concert:

- These entries are actually stored run-length encoded when possible. Adjacent, consecutive entries are compacted together.
- Semantically we use attributed IDs: `(agent ID, seq)`. But internally all operations are locally linearized in time. So we only store a `u32` "order number" for each ID internally. These numbers are simply linearly increasing with each change seen at this peer. They're also local only - other peers will end up with different id-to-order mappings.
- The values of each entry are pulled out into a separate data structure. (SoA instead of AoS). This makes it much easier to make the structure to be adapted to different kinds of data (text, rich text, lists of objects, etc).
- These values are stored in a non-traditional b-tree, which I'm calling a [range tree](https://en.wikipedia.org/wiki/Range_tree) until someone convinces me not to.

The result of all this is that a diamond list stores the following information for a document:

- Range tree of RLE entries
- Index into the range tree, to map from item order to entry.
- Bidirectional mapping from Order <-> `(agent, seq)` pairs


## Time

Each change which happens to a document is called an "operation" (or sometimes "patch"). Each operation has a unique ID and one or more *parents*.

For now, operations in diamond types are one of two types:

- Insert some value at a position in the list
- Mark a value in the list for deletion

More operation types will probably be added over time.

Each operation 'consumes' the next ID in sequence from the user which authored that change. Eg, ('seph', 1), ('seph', 2), ('seph', 3).

Each change also specifies a set of one or more parent IDs. This works the same way as commit parents in git, where an operation's parents are the IDs which came 'directly' before that operation. Changes end up forming a [DAG (Directed Acyclic Graph)](https://en.wikipedia.org/wiki/Directed_acyclic_graph). In the braid working group, we've taken to describing this structure as the "time DAG".

The first change has a special parent of "ROOT".

Semantically we could describe the time DAG like this:

```rust
enum OpContent {
    Insert(YjsEntry),
    Delete(Id),
}

struct Op {
    id: Id,
    content: OpContent,
    parents: Vec<Id>
}

type TimeDag = Set<Op>;
```

This structure is very "stringy" in practice. Again, a series of optimizations allow this to be normally stored in a tiny amount of memory in practice.

Each peer flattens all operations into a list based on *when* each operation was locally observed. The first operation a peer sees is item 0, then item 1, then 2, and so on. These item indexes are called "order numbers", and they're used everywhere internally. They are not shared between peers though - as peers may see the same operations in different orders.

This list always maintains partial order. An item with order X will always have parents with order lower than X.


### Time formats

There's 3 ways to name a moment in time in diamond types:

1. Using a full vector clock. This is a set of (id, seq) pairs for every agent which has ever contributed operations to the document. This structure will naturally be big (and grow over time) as more agent IDs make changes. But it can be interpreted by any peer at any time. This is useful for syncing peers which know nothing about one another - and may each have changes the other peer has never seen.
2. A frontier set. If we consider the time DAG, there will always be a set of one or more items in the tree which have no children. We can transitively figure out the entire set of parents by following the parents' tree. When an operation is created, its parents set to be the document's frontier at the time that operation was created. (And in turn, that operation's ID will become the new frontier). This is much smaller than using the full vector clock (and it doesn't grow over time). It can be shared between peers - but if a peer is missing the latest changes, the frontier set will be incomprehensible.
3. The "next order" number. This is a local only number naming the order which the next operation we see will be assigned. This is used by the OT bridge.


# All together

Taken together, the core document data structure (currently) looks something like this:

```rust
pub struct ListCRDT {
    // *** Space DAG stuff ***

    /// The marker tree maps from order positions to btree entries, so we can map between orders and
    /// document locations.
    ///
    /// This is the CRDT chum for the space DAG.
    range_tree: RangeTree<YjsSpan>,

    /// We need to be able to map each location to an item in the associated BST.
    /// Note for inserts which insert a lot of contiguous characters, this will
    /// contain a lot of repeated pointers. I'm trading off memory for simplicity
    /// here - which might or might not be the right approach.
    ///
    /// This is a map from insert Order -> a pointer to the leaf node which contains that insert.
    index: RleBTreeMap<Order, RangeTreeLeafPtr>,

    /// This is used to map Order -> External CRDT locations.
    client_with_order: RleVec<(Order, CRDTSpan)>,
    /// This is used to map external CRDT locations -> Order numbers.
    client_data: Vec<ClientData>,

    /// The content of the document itself. This will become generic with time.
    document_content: Ropey::Rope,

    // *** Time DAG stuff ***

    /// The set of txn orders with no children in the document. With a single writer this will
    /// always just be the last order we've seen.
    ///
    /// Never empty. Starts pointing at the root order.
    frontier: Vec<Order>,

    /// Compact 'parents' for all operations
    txns: RleVec<TxnSpan>,

    /// Optimizations around deletes are a little complex. Essentially this
    /// maps from delete operations -> which items each operation deleted.
    deletes: RleVec<(Order, OrderSpan)>,
    double_deletes: RleVec<(Order, DoubleDelete)>,
}
```
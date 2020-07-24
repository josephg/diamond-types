# Text CRDT Prototype

This is a prototype of a simple high performance CRDT for text. Its loosely
based off [automerge](https://github.com/automerge/automerge).

I wrote this because I want to have a simple data model to benchmark, to see how
it performs. I suspect it'll be very fast once its working correctly - which
would be a bit of a game changer

Each client / device has a unique ID, and each character typed on each device is
assigned an incrementing sequence number (starting at 0). Each character in the
document can thus be uniquely identified by the tuple of (client ID, sequence
number). This allows any location in the document to be uniquely named, so an
operation can then be defined by (preceeding character's (client id, sequence
number), inserted string, client id, client seq).


## Internals

We want the system to go fast at two main operations:

- Text edit to CRDT operation (Eg, "user A inserts at position 100" -> "user A
  seq 1000 inserts at (B, 50)")
- CRDT operation to text edit ("user A
  seq 1000 inserts at (B, 50)" -> "insert at document position 100")

Actually inserting text into a rope (or something) is reasonably easy, and
[ropey](https://github.com/cessen/ropey/) and
[xi-rope](https://crates.io/crates/xi-rope) both seem very [high
performance](https://home.seph.codes/public/c3/ins_random/report/index.html). So
in this library I'm worrying about the P2P operation boundary.

Internally, each client ID is mapped to a local integer. These integers are
never sent over the wire, so it doesn't matter if they aren't common between
peers.

Then we have two main data structures internally:

- A modified B-Tree of the entire document in sequence order. Entries are run
  encoded - which is to say, sequences of (client: 1, seq: 100), (client: 1,
  seq: 101), (client: 1, seq: 102), ... are collapsed into entries of (client:
  1, seq: 100, len: 5). Deleted characters are marked with a negative length.
  The tree's internal nodes store subtree sizes, so we can map from an entry in
  the tree to a document position (and back again) in O(log n) time.
- Per client operation lists. These are simply arrays of pointers mapping each
  sequential sequence number to the leaf node in the tree containing that item.
  These lists could also be run-length encoded - which would save memory, but
  then we'd need to binary search to find any given location instead of doing a
  simple array lookup.

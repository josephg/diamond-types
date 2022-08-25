# Diamond Types

This repository contains a high performance rust CRDT for text editing. This is a
special data type which supports concurrent editing of lists or strings
(text documents) by multiple users in a P2P network without needing a
centralized server.

This version of diamond types only supports plain text editing. Work is underway to add support for other JSON-style data types. See the `more_types` branch for details.

This project was initially created as a prototype to see how fast a well optimized CRDT could be made to go. The answer is really fast - faster than other similar libraries. This library is currently in the process of being expanded into a fast, feature rich CRDT in its own right.

For much more detail about how this library works, see:

- The talk I gave on this library at a recent [braid user meetings](https://braid.org/meeting-14) or
- [INTERNALS.md](INTERNALS.md) in this repository.
- [This blog post on making diamond types 5000x faster than competing CRDT implementations](https://josephg.com/blog/crdts-go-brrr/)
  - And since that blog post came out, performance has increased another 10-80x (!). 

As well as being lightning fast, this library is also designed to be interoperable with positional updates. This allows simple peers to interact with the data structure via operational transform.


## Internals

Each client / device has a unique ID. Each character typed or deleted on
each device is assigned an incrementing sequence number (starting at 0).
Each character in the document can thus be uniquely identified by the
tuple of `(client ID, sequence number)`. This allows any location in the
document to be uniquely named.

The internal data structures are designed to optimize two main operations:

- Text edit to CRDT operation (Eg, "user A inserts at position 100" -> "user A
  seq 1000 inserts at (B, 50)")
- CRDT operation to text edit ("user A
  seq 1000 inserts at (B, 50)" -> "insert at document position 100")

Much more detail on the internal data structures used is in [INTERNALS.md](INTERNALS.md).


# LICENSE

This code is published under the ISC license.


# Acknowledgements

This work has been made possible by funding from the [Invisible College](https://invisible.college/).
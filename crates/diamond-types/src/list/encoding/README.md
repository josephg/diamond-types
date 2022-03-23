# Diamond types binary encoding

Diamond types fundamentally has 2 data structures we need to save and load:

1. Operation logs. These are records of all the changes made to a document between two points in time.
2. Branches. A branch stores the document's state at some point in time. Branches are a tuple of `(version, value)`.

Branches are relatively simple. Most of the complexity of diamond types exists in the operation log.


## Time

Time in diamond types is represented in a DAG. Each node represents a point in time, the document state at that point in time and an operation (the last operation which moved the document into its current state).

Each point in time has a unique *ID* (a tuple of (agentID, seq)) and a set of one or more *parents*. This forms a DAG (directed acyclic graph), where edges express the "after" relationship. (An item comes *after* each of its parents). This relationship is transitive. Ie, if A is after B, and B is after C, then A is after C.

The document's history starts at a special `ROOT` time, at which the document is empty.

For example:

```
    ROOT
   /    \
(a, 1) (b, 1)
   \    /
   (a, 2)
     |
   (a, 3)
```

Unlike some other systems, merging is represented *implicitly*. Merges happen whenever an operation (a node) has multiple parents. Before the operation is applied, all parents are first merged together. Then the operation happens.

The current state of a document is named by the set of operations in the time DAG which have no children. This is called the *LocalVersion*. In the example above, the document containing all of those operations has the frontier of `[(a, 3)]`. By convention, the frontier never contains redundant items. Eg, `[(b, 1), (a, 2)]` should be simplified to `[(a, 2)]`.

Given an operation log, the document state at any point in time is well defined and computable.


## Oplog format

The operation log is stored in a highly compact binary format. This file format has been the result of a series of improvements by multiple people:

- Martin Kleppman did the initial set of work to RLE positional data. This [compacted a 146MB JSON format to 100kb](https://youtu.be/x7drE24geUw?t=4075)
- Kevin Jahns improved on this in [Yjs](https://github.com/yjs/yjs), bringing the same data set down to 50kb on disk
- I (Seph Gentle) have made some additional changes, including special handling for backspace events. This brings the same data set down to 21kb.

In diamond types lists, there are (so far) only two types of edits:

- *Insert* (Character X was inserted at position Y)
- *Delete* (1 character at position Y was deleted)

Every operation modifies exactly one character (more on this later).

So for every operation, we need to store:

- The original position (number of preceeding characters in the document)
- Type of edit (*Insert* or *Delete*)
- The item's ID
- The item's parents
- If the operation is an insert, the inserted character

All operations in the oplog are linearized (turned into a list). The list obeys the partial order of edits. Given these edits:

```
    ROOT
   /    \
(a, 1) (b, 1)
   \    /
   (a, 2)
```

The operations could be stored in the order of:

```
(a, 1)
(b, 1)
(a, 2)
```

or

```
(b, 1)
(a, 1)
(a, 2)
```

But thats it. `(a, 2)` can't appear before `(a, 1)` or `(b, 1)` because it comes after both of those operations in time.

Note that any linearization is non-canonical. Different computers on the network will have the same set of operations in a different order.

Diamond types makes heavy use of "local times" - which are essentially just indexes into the array of operations sorted like this.

The operations are stored on disk in a struct of arrays format, rather than an array of structs. This is simply smaller on disk. The oplog file stores a series of *chunks*, separately storing:

- Item IDs
- Item parents
- Operation type, position and length (stored together based on benchmark data)
- The contents of all inserted text

Each of these fields (except the inserted text content) is run-length encoded. This saves massive amounts of space. For example, if a single user makes 1000 consecutive edits to a document, the item IDs will be `(user, 0..1000)`. Item parents are stored in runs of items where each item (except the first) has a parents list of the previous item. So here, the parents data is simply `{ parents: [...], len: 1000 }`.



### Design questions to solve pre 1.0

- Should oplogs store a full version vector?
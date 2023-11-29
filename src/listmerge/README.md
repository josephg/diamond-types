# Merging code

This internal module contains the code for merging changes into a branch. This is a difficult problem because sometimes changes are concurrent.

For example, consider a document with 3 inserts producing the document "abc", but where the `a` and `b` characters were typed concurrently. We end up with a time dag like this:

```
 ROOT
 / \
a   |
|   b
 \ /
  c
```

Internally, the oplog stores this information as a table:

| Time | Content | Position | Parents |
|------|---------|----------|---------|
| 0    | `a`     | 0        | ROOT    |
| 1    | `b`     | **0**    | ROOT    |
| 2    | `c`     | 2        | 0 + 1   |

> *Aside:* This is simplified - DT stores a few more fields, including author (agentID), sequence number and operation type (insert or delete).

This table is interesting:

- Both the `a` and `b` characters have a position of 0. If we replayed this editing trace naively, we'd end up with `bac` instead of `abc`.
- We can detect that those two inserts were concurrent because neither operation is an ancestor of the other change.

The problem when replaying this editing history is figuring out the "merged position". Ie, when replaying these inserts, where does each insert go? We can think of this as needing to fill in another column on this table:

| Time | Content | Position | Parents | **Merge Pos** |
|------|---------|----------|---------|---------------|
| 0    | `a`     | 0        | ROOT    | ???           |
| 1    | `b`     | **0**    | ROOT    | ???           |
| 2    | `c`     | 2        | 0 + 1   | ???           |

Some of these merge position values are easy to fill in. When the parents set contains all previous edits, the merge position is the same as the origin position:

| Time | Content | Position | Parents | Merge Pos |
|------|---------|----------|---------|-----------|
| 0    | `a`     | 0        | ROOT    | **0**     |
| 1    | `b`     | **0**    | ROOT    | ???       |
| 2    | `c`     | 2        | 0 + 1   | **2**     |

But figuring out the merge position of the `b` character, we need to do a lot more work!

This module contains one algorithm to solve this problem.

## The algorithm

There's a few different algorithmic ideas I've considered while working on diamond types. This code contains the first correct algorithm I've implemented, which works as follows.

First, the algorithm depends on a special data structure called a *tracker*. A tracker stores a set of operations (inserts or deletes) in *document order*. That is, in the order in which the changes appear in the final document. The tracker has two magic tricks:

1. Tracked operations can be toggled on and off.
2. The tracker can map positions from the current state (ignoring operations which are toggled off) to the state when all operations are enabled. 


The merging algorithm then essentially does the following steps:

1. Create an empty tracker
2. For each operation, toggle operations in the tracker such that the tracker represents the state of the document when that operation was performed
3. Map the operation's location to the merged location using the tracker.
4. Use the mapped location to modify the document

The tracker is a b-tree of [`YjsSpan`](yjsspan.rs) items. Its implemented using the b-tree implementation in [`content-tree`](../../../../content-tree), with a custom index found in [`metrics.rs`](metrics.rs).

The code in [`txn_trace.rs`](../list/encoding/txn_trace.rs) implements an iterator over all changes in the document. Iteration order is somewhat complicated in order to avoid a few pathological cases.

The merging algorithm itself is in [`merge.rs`](merge.rs).
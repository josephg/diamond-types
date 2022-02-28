# Diamond types binary encoding format

This document describes the diamond types binary file formats.

> WARNING: This format is still flux. There may be some last minute breaking changes to the file format before DT hits 1.0.

As outlined in other documents, diamond types has 2 core data structures:

An **OpLog** is a log of all changes which have happened to a document. Essentially an oplog is a list of operations between two points in time. The list of operations isn't always strictly ordered, because sometimes operations are concurrent.

A **Branch** is a copy of a document at some specific point in time. Essentially, a branch is simply a `(data, version)` tuple.

## Oplog Encoding

The operation log can be thought of as a table, where each row in the table describes a single insert or delete operation. For each operation we store the following fields:

- Parent version(s) (eg *[(mike, 2), (seph, 10)]*)
- Resulting version (eg *(seph, 11)*)
- Operation type (Currently just *Insert* or *Delete*)
- Operation position. Ie, the position in the document where the insert or delete took place.
- Operation contents (optional). For inserts and deletes, this names the item (character) which was inserted or deleted.

Note the operation contents is optional.

- Inserted contents are almost always desirable. But they aren't always needed - particularly in some minimal merging situations.
- Deleted content is only needed in order to make it easier to rewind & replay history. Given the full history of a document, the deleted content can always be regenerated.

The operation log file format stores all of this information, as well as some additional metadata:

- File format version (currently 0)
- Starting version (for the whole file)
- Starting content (Optional)
- Resulting content (optional, Not implemented yet)
- UserData (Optional)
- CRC check (optional)

Encoding the oplog is complex in order to store the oplog in a compact form. And we've achieved a very impressive result on that front, even though the cost is extra code complexity.

Measured in KB:

| Implementation | Automerge-perf |
| Raw JSON       | 16 060         |
| JSON+gzip      | 904            |
| JSON+Brotli    | 461            |
| DT (full)      | 281            |
| DT (patches)   | 23             |

> TODO: Fill out the rest of this table with yjs & automerge implementations, and the other data sets.

At a high level, the file format looks like this:

* Magic bytes (`DMNDTYPS`) (8 bytes)
* Protocol version (Currently 0)
* FileInfo chunk
  * (**TODO**): File type
  * UserData (optional)
  * AgentNames (used below)
* StartBranch (Ie, what the document looks like before the ops below)
  * Frontier (Version / parents of the start of this file)
  * Content (Optional)
* Patches chunk (This contains the operations themselves)
  * Inserted content
  * Deleted content
  * AgentAssignment (Version of each change)
  * PositionalPatches (Type & position of each change)
  * TimeDAG chunk (Parents of each change)
* CRC

This file format is very much optimized for large files. Its not optimized for sending teeny tiny individual changes.


### VarInts

DT makes extensive use of the [Varint encoding](https://developers.google.com/protocol-buffers/docs/encoding#varints) from Google Protobufs. All integers in a DT file (unless otherwise specified) are encoded using the varint format.

### Chunks

A diamond types file is made up of a tree of chunks. Each chunk contains:

- A chunk type (varint, see below)
- The chunk's byte length, not including the chunk header (varint)
- Bytes of data

The format has been designed to allow extra chunk types to be added over time. Unknown chunks should be ignored.

Registered chunk types are below:

| Name | Code | Description |
| ---- | ---- | ----------- |

> TODO: Move into table above

```rust
enum ChunkType {
    /// FileInfo contains optional UserData and AgentNames.
    FileInfo = 1,
    UserData = 2,
    AgentNames = 3,

    /// The StartBranch chunk describes the state of the document before included patches have been
    /// applied.
    StartBranch = 10,
    Frontier = 12,
    Content = 13,

    Patches = 20,
    Version = 21,
    OpTypeAndPosition = 22,
    Parents = 23,
    InsertedContent = 24,
    DeletedContent = 25,

    CRC = 100,
}
```

### Patch encoding

The DT patch encoding makes very heavy use of DT's RLE encoding tricks. Each chunk in the patch block contains one or more fields from the data set, run-length encoded. The data is formatted this way for compactness. For example:

- Its quite common for all changes in the file to be from a single author, with sequential times. This allows the patch `Versions` field to essentially encode a single value.
- Even complex histories often have quite simple time DAGs. The `Parents` field takes advantage of the fact almost all changes simply have the previous item as their parent, and essentially only encodes the parents of patches where this is not the case.
- The patch `OpTypeAndPosition` field run-length encodes adjacent insert & delete operations. In a list with append-only (or prepend-only) edits, this will collapse to a single item! Real-world text editing traces are also compressed very efficiently with this, since users tend to type and delete in runs of characters.
- The actual inserted & deleted content chunks are pulled out for a few reasons:
  - They're optional
  - Having the content itself separate makes it easier for code to adjust based on data type
  - Content - particularly text content - compresses very well. Although compression hasn't been added to DT yet, I'm intending to add LZ4 compression to all content chunks. LZ4's fast compression seems to dramatically reduce file size with almost no cost to performance.


## Branch Encoding

> TODO

Branches store the following fields:

- Version
- Type of content
- Content itself


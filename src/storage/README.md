# Storage engine

This is a simple storage engine which can incrementally persist changes to DT documents on disk.

The storage engine is:

- Atomic (writes have either happened or they haven't)
- Incremental (when data changes, we don't need to re-save the entire history of a document)

It does not yet support:

- Reads in `log(n)` time
- Pruning

Each DT document has its oplog saved as a single file on disk.


## On disk layout

The file on disk is made up of a bunch of fixed size (4k) blocks. Every write overwrites an entire block - which sounds wasteful, but this plays nicely with modern NVMe block devices.

The data set is made of a bunch of data types, each encoded as a separate column. The columns are:

- Agent IDs
- Causal graph (agent assignment & parents information)
- Operations (???)
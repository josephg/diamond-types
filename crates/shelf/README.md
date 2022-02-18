# Shelves

This directory contains an experimental Rust rewrite of [shelf](https://github.com/dglittle/shelf).

A shelf is a value tagged with a version. You can think of them as a tuple in the form `(VALUE, VERSION)`.

The rules for merging two shelves `(A, A#)` and `(B, B#)` are as follows:

1. If `A#` is greater than `B#`, return `(A, A#)`.
2. If `B#` is greater than `A#`, return `(B, B#)`.
3. If `A` and `B` are both objects, recursively merge all keys, return `(X, B#)`
4. Otherwise, pick `(B, B#)`.

# Positional variant

This directory contains an experimental rewrite of diamond types where positional changes are canonical.

This has some benefits:

- ~4x Faster merging of changes when no concurrency occurs.
- ~9x Lower memory footprint (~2mb -> ~300kb) for the same document
- Smaller on-disk or over-the-wire size

And one big drawback:

- Merging long lived branches is much slower

How much slower? That's what I'm trying to figure out!
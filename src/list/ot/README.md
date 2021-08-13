# What is this??

After discussion at one of the braid meetings, we decided it would be useful to allow simple clients which stream positional changes from a live document. I'm imagining web pages here - things like that. These clients should be able to:

- Stream changes (read-only) in a simple way
- Make changes (read-write) using a simple OT system

So to facilitate that, we have the OT (operational transformation) module. This allows clients to fetch the set of changes since some nominated version in time, and ideally parse and process a change made against a previous version.

Its currently pretty experimental and work in progress code.
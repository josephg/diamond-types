#![allow(unused)]
#![allow(unused_imports)]

use std::marker::PhantomData;
use std::mem::replace;
use rle::MergableSpan;
use num_enum::TryFromPrimitive;

/// The encoding module converts the internal data structures to and from a lossless compact binary
/// data format.
///
/// This is modelled after the run-length encoding in Automerge and Yjs.

// Notes for next time I break compatibility:
// - Version in encode::write_local_version - skip second 0 if its ROOT.
pub mod varint;
pub(crate) mod bufparser;
pub(crate) mod parseerror;
pub(crate) mod agent_assignment;
pub(crate) mod tools;
pub(crate) mod parents;
pub(crate) mod op_contents;
pub(crate) mod cg_entry;
// mod agent_assignment;


#[derive(Debug, PartialEq, Eq, Copy, Clone, TryFromPrimitive)]
#[repr(u32)]
pub(crate) enum ChunkType {
    /// Packed bytes storing any data compressed in later parts of the file.
    // CompressedFieldsLZ4 = 5,

    /// FileInfo contains optional UserData and AgentNames.
    FileInfo = 1,
    DocId = 2,
    // AgentNames = 3,
    UserData = 4,

    /// The StartBranch chunk describes the state of the document before included patches have been
    /// applied.
    StartBranch = 10,
    Version = 12,
    // /// StartBranch content is optional.
    // TextContent = 13,
    // TextContentCompressed = 14, // Might make more sense to have a generic compression tag for chunks.

    SetContent = 15,
    SetContentCompressed = 16,

    Patches = 20,
    OpVersions = 21,
    OpTypeAndPosition = 22,
    OpParents = 23,

    PatchContent = 24,
    /// ContentKnown is a RLE expressing which ranges of patches have known content
    ContentIsKnown = 25,

    TransformedPositions = 27, // Currently unused

    // Crc = 100,
}

#[derive(Clone)]
pub(super) struct Merger<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx = ()> {
    last: Option<S>,
    f: F,
    _ctx: PhantomData<Ctx> // Its pretty silly that this is needed.
}

impl<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx> Merger<S, F, Ctx> {
    pub fn new(f: F) -> Self {
        Self { last: None, f, _ctx: PhantomData }
    }

    pub fn push2(&mut self, span: S, ctx: &mut Ctx) {
        if let Some(last) = self.last.as_mut() {
            if last.can_append(&span) {
                last.append(span);
            } else {
                let old = replace(last, span);
                (self.f)(old, ctx);
            }
        } else {
            self.last = Some(span);
        }
    }

    pub fn flush2(mut self, ctx: &mut Ctx) {
        if let Some(span) = self.last.take() {
            (self.f)(span, ctx);
        }
    }

    pub fn flush_iter2<I: Iterator<Item = S>>(mut self, iter: I, ctx: &mut Ctx) {
        for span in iter {
            self.push2(span, ctx);
        }
        self.flush2(ctx);
    }
}

impl<S: MergableSpan, F: FnMut(S, &mut ())> Merger<S, F, ()> {
    pub fn push(&mut self, span: S) {
        self.push2(span, &mut ());
    }
    pub fn flush(self) {
        self.flush2(&mut ());
    }
    pub fn flush_iter<I: Iterator<Item = S>>(mut self, iter: I) {
        self.flush_iter2(iter, &mut ());
    }
}

impl<S: MergableSpan, F: FnMut(S, &mut Ctx), Ctx> Drop for Merger<S, F, Ctx> {
    fn drop(&mut self) {
        if self.last.is_some() && !std::thread::panicking() {
            panic!("Merger dropped with unprocessed data");
        }
    }
}





#[cfg(test)]
mod test {

}

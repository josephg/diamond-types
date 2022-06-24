#![allow(unused)]
#![allow(unused_imports)]

/// The encoding module converts the internal data structures to and from a lossless compact binary
/// data format.
///
/// This is modelled after the run-length encoding in Automerge and Yjs.

// Notes for next time I break compatibility:
// - Version in encode::write_local_version - skip second 0 if its ROOT.
pub mod varint;
mod bufreader;
pub(crate) mod parseerror;
pub(crate) mod agent_assignment;
pub(crate) mod tools;
pub(crate) mod parents;
// mod agent_assignment;

use std::mem::replace;
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::encoding::bufreader::BufReader;
use crate::encoding::parseerror::ParseError;

pub(crate) trait RlePackWriteCursor {
    type Item: SplitableSpan + MergableSpan + HasLength;
    // type Ctx;

    fn write_and_advance(&mut self, item: &Self::Item, dest: &mut Vec<u8>);
    // fn write_and_advance(&mut self, item: &Self::Item, dest: &mut Vec<u8>, ctx: &mut Self::Ctx);
}

pub(crate) trait RlePackReadCursor {
    type Item: SplitableSpan + MergableSpan + HasLength;

    // Read path.
    // /// Returns None when chunk is empty.
    // fn peek(&self, bytes: &[u8]) -> Result<Option<Self::Item>, ParseError>;

    /// Read the next item and update the cursor
    fn read(&mut self, reader: &mut BufReader) -> Result<Option<Self::Item>, ParseError>;
}

trait ToBytes {
    fn write(&self, dest: &mut Vec<u8>);
}

// trait RlePack {
//     type Item: SplitableSpan + MergableSpan + HasLength + ToBytes + Default;
//     type Cursor: RlePackCursor<Self::Item>;
// }


// #[derive(Default, Debug, Eq, PartialEq)]
// struct NullCursor;
//
// impl<I: SplitableSpan + MergableSpan + HasLength + ToBytes> RlePackCursor for NullCursor {
//     type Item = I;
//
//     fn write_and_advance(&mut self, item: &I, dest: &mut Vec<u8>) {
//         item.write(dest)
//     }
// }

#[derive(Debug)]
pub(crate) struct PackWriter<S: RlePackWriteCursor> {
    last: Option<S::Item>,
    cursor: S,
}

impl<S: RlePackWriteCursor + Default> Default for PackWriter<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: RlePackWriteCursor> PackWriter<S> {
    pub fn new(cursor: S) -> Self {
        Self {
            last: None,
            cursor
        }
    }

    pub fn push(&mut self, span: S::Item, out: &mut Vec<u8>) {
        if let Some(last) = self.last.as_mut() {
            if last.can_append(&span) {
                last.append(span);
            } else {
                let old = replace(last, span);
                self.cursor.write_and_advance(&old, out);
                // old.write(out);
            }
        } else {
            self.last = Some(span);
        }
    }

    pub fn flush(mut self, out: &mut Vec<u8>) -> S {
        if let Some(span) = self.last.take() {
            // span.write(out);
            self.cursor.write_and_advance(&span, out);
        }
        self.cursor
    }
}

#[cfg(test)]
mod test {
    // use crate::CRDTSpan;
    // use crate::encoding::{PackWriter, RlePackCursor};
    // use crate::encoding::agent_assignment::AgentAssignmentCursor;

    // #[test]
    // fn foo() {
    //     let mut w: PackWriter<AgentAssignmentCursor> = PackWriter::new(AgentAssignmentCursor::new(10));
    //
    //     let mut result = Vec::new();
    //     w.push(CRDTSpan {
    //         agent: 2,
    //         seq_range: (1..10).into()
    //     }, &mut result);
    //
    //     // w.push(CRDTSpan {
    //     //     agent: 5,
    //     //     seq_range: (1..10).into()
    //     // }, &mut result);
    //
    //     let c = w.flush(&mut result);
    //     dbg!(c);
    //     dbg!(result);
    // }
}

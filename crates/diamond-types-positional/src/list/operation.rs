/// Positional updates are a kind of operation (patch) which is larger than traversals but
/// retains temporal information. So, we know when each change happened relative to all other
/// changes.
///
/// Updates are made up of a series of insert / delete components, each at some position.

use smartstring::alias::{String as SmartString};
use rle::{HasLength, MergableSpan, SplitableSpan};
use InsDelTag::*;
use crate::unicount::{chars_to_bytes, count_chars};
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use crate::list::internal_op::OperationInternal;
use crate::localtime::TimeSpan;
use crate::rev_span::TimeSpanRev;

/// So I might use this more broadly, for all edits. If so, move this out of OT.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub enum InsDelTag { Ins, Del }

impl Default for InsDelTag {
    fn default() -> Self { InsDelTag::Ins } // Arbitrary.
}

/// So the span here is interesting. For inserts, this is the range of positions the inserted
/// characters *will have* after they've been inserted.
///
/// For deletes this is the range of characters in the document *being deleted*.
///
/// The `rev` field specifies if the items being inserted or deleted are doing so in reverse order.
/// For inserts "normal" mode means appending, reverse mode means prepending.
/// For deletes, normal mode means using the delete key. reverse mode means backspacing.
///
/// This has no effect on *what* is deleted. Only the resulting order of the operations. This is
/// totally unnecessary - we could just store extra entries with length 1 when modifying in other
/// orders. But it gives us way better compression for some data sets on disk. And this structure
/// is designed to match the on-disk file format.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct Operation {
    // For now only backspaces are ever reversed.
    pub span: TimeSpanRev,

    // TODO: Remove content_known by making content an Option(...)
    pub content_known: bool,
    pub tag: InsDelTag,
    // pub content_bytes_offset: usize,
    pub content: SmartString,
}

impl HasLength for Operation {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl Operation {
    pub fn new_insert(pos: usize, content: &str) -> Self {
        let len = count_chars(content);
        Operation { span: (pos..pos+len).into(), content_known: true, tag: Ins, content: content.into() }
    }

    pub fn new_delete(pos: usize, len: usize) -> Self {
        Operation { span: (pos..pos+len).into(), content_known: false, tag: Del, content: Default::default() }
    }

    pub fn new_delete_with_content(pos: usize, content: SmartString) -> Self {
        let len = count_chars(&content);
        Operation { span: (pos..pos+len).into(), content_known: true, tag: Del, content }
    }

    // Could just inline this into truncate() below. It won't be used in other contexts.
    // fn split_positions(&self, at: usize) -> (usize, usize) {
    //     let first = self.span.span.start;
    //     match (self.span.fwd, self.tag) {
    //         (true, Ins) => (first, first + at),
    //         (false, Del) => (first + self.len - at, first),
    //         _ => (first, first)
    //     }
    // }

    pub fn range(&self) -> TimeSpan {
        self.span.span
    }

    #[inline]
    pub fn start(&self) -> usize {
        self.span.span.start
    }

    #[inline]
    pub fn end(&self) -> usize {
        self.span.span.end
    }
}

impl SplitableSpan for Operation {
    fn truncate(&mut self, at: usize) -> Self {
        // let (self_span, other_span) = TimeSpanRev::split_op_span(self.span, self.tag, at);
        let other_span = self.span.truncate_tagged_span(self.tag, at);

        let byte_split = if self.content_known {
            chars_to_bytes(&self.content, at)
        } else {
            0
        };

        // TODO: When we split items to a length of 1, consider clearing the reversed flag.
        // This doesn't do anything - but it feels polite.
        let remainder = Self {
            span: TimeSpanRev {
                span: other_span,
                fwd: self.span.fwd
            },
            content_known: self.content_known,
            tag: self.tag,
            content: self.content.split_off(byte_split),
        };
        // if remainder.len == 1 { remainder.reversed = false; }

        // self.span.span = self_span;

        // self.reversed = if self.len == 1 { false } else { self.reversed };

        remainder
    }
}

impl MergableSpan for Operation {
    fn can_append(&self, other: &Self) -> bool {
        if other.tag != self.tag || self.content_known != other.content_known { return false; }

        TimeSpanRev::can_append_ops(self.tag, &self.span, &other.span)
    }

    fn append(&mut self, other: Self) {
        self.span.append_ops(self.tag, other.span);

        if self.content_known {
            self.content.push_str(&other.content);
        }
    }

    // fn prepend(&mut self, mut other: Self) {
    //     // self.reversed = self.pos < other.pos || (other.pos == self.pos && self.tag == Ins);
    //     self.fwd = self.pos >= other.pos && (other.pos != self.pos || self.tag == Del);
    //
    //     if self.tag == Ins || self.fwd {
    //         self.pos = other.pos;
    //     }
    //     self.len += other.len;
    //
    //     if self.tag == Ins && self.content_known {
    //         other.content.push_str(&self.content);
    //         self.content = other.content;
    //     }
    // }
}

impl From<(OperationInternal, Option<&str>)> for Operation {
    fn from((op, content): (OperationInternal, Option<&str>)) -> Self {
        Operation {
            span: op.span,
            content_known: content.is_some(),
            tag: op.tag,
            content: content.map_or_else(|| Default::default(), |str| str.into())
        }
    }
}


#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn test_backspace_merges() {
        // Make sure deletes collapse.
        let a = Operation {
            span: (100..101).into(),
            content_known: true,
            tag: Del,
            content: Default::default()
        };
        let b = Operation {
            span: (99..100).into(),
            content_known: true,
            tag: Del,
            content: Default::default()
        };
        assert!(a.can_append(&b));

        let mut merged = a.clone();
        merged.append(b.clone());
        // dbg!(&a);
        let expect = Operation {
            span: TimeSpanRev {
                span: (99..101).into(),
                fwd: false
            },
            content_known: true,
            tag: Del,
            content: Default::default()
        };
        assert_eq!(merged, expect);

        // And via prepend.
        let mut merged2 = b.clone();
        merged2.prepend(a.clone());
        dbg!(&merged2);
        assert_eq!(merged2, expect);
    }

    #[test]
    fn positional_component_splitable() {
        for fwd in [true, false] {
            for content_known in [true, false] {
                if fwd {
                    test_splitable_methods_valid(Operation {
                        span: TimeSpanRev {
                            span: (10..15).into(),
                            fwd
                        },
                        content_known: true,
                        tag: Ins,
                        content: "abcde".into()
                    });
                }

                test_splitable_methods_valid(Operation {
                    span: TimeSpanRev {
                        span: (10..15).into(),
                        fwd
                    },
                    content_known,
                    tag: Del,
                    content: Default::default()
                });
            }
        }
    }
}
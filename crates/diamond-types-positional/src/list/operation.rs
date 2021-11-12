use jumprope::JumpRope;
/// Positional updates are a kind of operation (patch) which is larger than traversals but
/// retains temporal information. So, we know when each change happened relative to all other
/// changes.
///
/// Updates are made up of a series of insert / delete components, each at some position.

use smartstring::alias::{String as SmartString};
use smallvec::{SmallVec, smallvec};
use rle::{HasLength, MergableSpan, SplitableSpan};
use InsDelTag::*;
use crate::unicount::{chars_to_bytes, count_chars};
use rle::AppendRle;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};

/// So I might use this more broadly, for all edits. If so, move this out of OT.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub enum InsDelTag { Ins, Del }

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
#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct PositionalComponent {
    pub pos: usize,
    pub len: usize,

    pub rev: bool,
    pub content_known: bool,
    pub tag: InsDelTag,
    // pub content_bytes_offset: usize,
    pub content: SmartString,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct PositionalOp(pub SmallVec<[PositionalComponent; 1]>);

// pub struct PositionalOp {
//     pub components: SmallVec<[PositionalComponent; 1]>,
//     pub content: SmartString,
// }

impl Default for InsDelTag {
    fn default() -> Self { InsDelTag::Ins } // Arbitrary.
}

// This is such a dirty hack and I'm not even mad about it.
const XS: &str = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

impl PositionalOp {
    pub fn new() -> Self { Self::default() }

    pub fn new_insert(pos: usize, content: &str) -> Self {
        let len = count_chars(content);
        Self(smallvec![
            PositionalComponent { pos, len, rev: false, content_known: true, tag: Ins, content: content.into() }
        ])
    }
    pub fn new_delete(pos: usize, len: usize) -> Self {
        Self(smallvec![
            PositionalComponent { pos, len, rev: false, content_known: true, tag: Del, content: Default::default() }
        ])
    }

    // pub fn apply_to_rope(&self, rope: &mut JumpRope) {
    //     // let mut new_content = self.content.as_str();
    //
    //     for c in &self.components {
    //         let len = c.len as usize;
    //         let pos = c.pos as usize;
    //         match c.tag {
    //             Ins => {
    //                 if c.content_known {
    //                     // let byte_len = chars_to_bytes(new_content, len);
    //                     // let (here, next) = new_content.split_at(byte_len);
    //                     // new_content = next;
    //                     if c.rev {
    //                         let s = here.chars().rev().collect::<String>();
    //                         rope.insert(pos, &s);
    //                     } else {
    //                         rope.insert(pos, here);
    //                     }
    //                 } else if len < XS.len() {
    //                     rope.insert(pos, &XS[..len]);
    //                 } else {
    //                     // let xs: String = std::iter::repeat('X').take(len).collect();
    //                     let xs: String = "X".repeat(len);
    //                     rope.insert(pos, &xs);
    //                 }
    //             }
    //             Del => {
    //                 rope.remove(pos..pos+len);
    //             }
    //         }
    //     }
    // }

    // pub fn from_components(components: SmallVec<[(usize, PositionalComponent); 10]>, content: Option<&JumpRope>) -> Self {
    //     let mut result = Self::new();
    //     for (post_pos, mut c) in components {
    //         if c.content_known {
    //             if let Some(content) = content {
    //                 let chars = content.slice_chars(post_pos..post_pos + c.len);
    //                 result.content.extend(chars);
    //             } else {
    //                 c.content_known = false;
    //             }
    //         }
    //         result.components.push_rle(c);
    //     }
    //     result
    // }
}

impl HasLength for PositionalComponent {
    fn len(&self) -> usize {
        self.len
    }
}

impl PositionalComponent {
    // Could just inline this into truncate() below. It won't be used in other contexts.
    fn split_positions(&self, at: usize) -> (usize, usize) {
        let first = self.pos;
        if self.rev == false && self.tag == Ins {
            (first, first + at)
        } else if self.rev == true && self.tag == Del {
            (first + self.len - at, first)
        } else {
            (first, first)
        }
    }
}

impl SplitableSpan for PositionalComponent {
    fn truncate(&mut self, at: usize) -> Self {
        let (self_first, rem_first) = self.split_positions(at);
        let byte_split = if self.tag == Ins && self.content_known {
            chars_to_bytes(&self.content, at)
        } else {
            0
        };

        let remainder = Self {
            pos: rem_first,
            len: self.len - at,
            rev: self.rev,
            content_known: self.content_known,
            tag: self.tag,
            content: self.content.split_off(byte_split),
        };

        self.pos = self_first;
        self.len = at;

        remainder
    }
}

impl MergableSpan for PositionalComponent {
    fn can_append(&self, other: &Self) -> bool {
        let tag = self.tag;

        if other.tag != tag || self.content_known != other.content_known { return false; }

        if other.rev != self.rev && self.len > 1 && other.len > 1 { return false; }

        if (self.len == 1 || self.rev == false) && (other.len == 1 || other.rev == false) {
            // Try and append in the forward sort of way.
            if (tag == Ins && other.pos == self.pos + self.len)
                || (tag == Del && other.pos == self.pos) { return true; }
        }
        if (self.len == 1 || self.rev == true) && (other.len == 1 || other.rev == true) {
            // Try an append in a reverse sort of way
            if (tag == Ins && other.pos == self.pos)
                || (tag == Del && other.pos + other.len == self.pos) { return true; }
        }

        false
    }

    fn append(&mut self, other: Self) {
        self.rev = other.pos < self.pos || (other.pos == self.pos && self.tag == Ins);

        self.len += other.len;

        if self.tag == Del && self.rev {
            self.pos = other.pos;
        }

        if self.tag == Ins && self.content_known {
            self.content.push_str(&other.content);
        }
    }

    fn prepend(&mut self, mut other: Self) {
        self.rev = self.pos < other.pos || (other.pos == self.pos && self.tag == Ins);

        if self.tag == Ins || self.rev == false {
            self.pos = other.pos;
        }
        self.len += other.len;

        if self.tag == Ins && self.content_known {
            other.content.push_str(&self.content);
            self.content = other.content;
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
        let a = PositionalComponent {
            pos: 100,
            len: 1,
            rev: false,
            content_known: true,
            tag: Del,
            content: Default::default()
        };
        let b = PositionalComponent {
            pos: 99,
            len: 1,
            rev: false,
            content_known: true,
            tag: Del,
            content: Default::default()
        };
        assert!(a.can_append(&b));

        let mut merged = a.clone();
        merged.append(b.clone());
        // dbg!(&a);
        let expect = PositionalComponent {
            pos: 99,
            len: 2,
            rev: true,
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
        for rev in [true, false] {
            for content_known in [true, false] {
                test_splitable_methods_valid(PositionalComponent {
                    pos: 10,
                    len: 5,
                    rev,
                    content_known: true,
                    tag: Ins,
                    content: "abcde".into()
                });

                test_splitable_methods_valid(PositionalComponent {
                    pos: 10,
                    len: 5,
                    rev,
                    content_known,
                    tag: Del,
                    content: Default::default()
                });
            }
        }
    }
}
use jumprope::{JumpRope, JumpRopeBuf};
/// Positional updates are a kind of operation (patch) which is larger than traversals but
/// retains temporal information. So, we know when each change happened relative to all other
/// changes.
///
/// Updates are made up of a series of insert / delete components, each at some position.

use smartstring::alias::{String as SmartString};
use smallvec::{SmallVec, smallvec};
use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanHelpers};
use InsDelTag::*;
use crate::unicount::{chars_to_bytes, count_chars};
use rle::AppendRle;
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use crate::list::ot::traversal::TraversalOp;
use crate::list::TraversalComponent;

/// So I might use this more broadly, for all edits. If so, move this out of OT.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub enum InsDelTag { Ins, Del }

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct PositionalComponent {
    pub pos: u32,
    pub len: u32,
    pub content_known: bool,
    pub tag: InsDelTag,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct PositionalOp {
    pub components: SmallVec<[PositionalComponent; 1]>,
    pub content: SmartString,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PositionalOpRef<'a> {
    pub components: &'a [PositionalComponent],
    pub content: &'a str,
}

impl Default for InsDelTag {
    fn default() -> Self { InsDelTag::Ins } // Arbitrary.
}

// This is such a dirty hack and I'm not even mad about it.
const XS: &str = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

impl PositionalOp {
    pub fn new() -> Self { Self::default() }

    pub fn new_insert<S: Into<SmartString> + AsRef<str>>(pos: u32, content: S) -> Self {
        let len = count_chars(&content.as_ref()) as u32;
        Self {
            components: smallvec![
                PositionalComponent { pos, len, content_known: true, tag: Ins }
            ],
            content: content.into()
        }
    }
    pub fn new_delete(pos: u32, len: u32) -> Self {
        Self {
            components: smallvec![
                PositionalComponent { pos, len, content_known: true, tag: Del }
            ],
            content: Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.components.iter().map(|c| c.len).sum::<u32>() as usize
    }

    pub fn apply_to_rope(&self, rope: &mut JumpRope) {
        let mut new_content = self.content.as_str();

        for c in &self.components {
            let len = c.len as usize;
            let pos = c.pos as usize;
            match c.tag {
                Ins => {
                    if c.content_known {
                        let byte_len = chars_to_bytes(new_content, len);
                        let (here, next) = new_content.split_at(byte_len);
                        new_content = next;
                        rope.insert(pos, here);
                    } else if len < XS.len() {
                        rope.insert(pos, &XS[..len]);
                    } else {
                        // let xs: String = std::iter::repeat('X').take(len).collect();
                        let xs: String = "X".repeat(len);
                        rope.insert(pos, &xs);
                    }
                }
                Del => {
                    rope.remove(pos..pos+len);
                }
            }
        }
    }

    pub fn from_components(components: SmallVec<[(u32, PositionalComponent); 10]>, content: Option<&JumpRopeBuf>) -> Self {
        let mut result = Self::new();
        for (post_pos, mut c) in components {
            if c.content_known {
                if let Some(content) = content {
                    let borrow = content.borrow();
                    let chars = borrow.slice_chars(post_pos as usize .. (post_pos + c.len) as usize);
                    result.content.extend(chars);
                } else {
                    c.content_known = false;
                }
            }
            result.components.push_rle(c);
        }
        result
    }

    #[allow(unused)]
    pub(crate) fn check(&self) {
        let content_len = self.components.iter().map(|c| {
            if c.content_known && c.tag == Ins { c.len } else { 0 }
        }).sum::<u32>() as usize;

        assert_eq!(content_len, count_chars(&self.content));
    }
}

impl<'a> From<&'a PositionalOp> for PositionalOpRef<'a> {
    fn from(op: &'a PositionalOp) -> Self {
        PositionalOpRef {
            components: op.components.as_slice(),
            content: &op.content
        }
    }
}

impl PositionalComponent {
    pub fn from_traversal_components(traversal: &[TraversalComponent]) -> SmallVec<[PositionalComponent; 1]> {
        let mut result = SmallVec::new();
        let mut pos = 0;
        for c in traversal {
            match c {
                TraversalComponent::Retain(len) => pos += *len,

                TraversalComponent::Ins { len, content_known } => {
                    result.push(PositionalComponent {
                        pos,
                        len: *len,
                        content_known: *content_known,
                        tag: Ins
                    });
                    pos += *len;
                }
                TraversalComponent::Del(len) => {
                    result.push(PositionalComponent {
                        pos,
                        len: *len,
                        content_known: false,
                        tag: Del
                    });
                }
            }
        }
        result
    }
}

impl From<TraversalOp> for PositionalOp {
    fn from(traversal: TraversalOp) -> Self {
        Self {
            components: PositionalComponent::from_traversal_components(traversal.traversal.as_slice()),
            content: traversal.content
        }
    }
}

impl Default for PositionalComponent {
    fn default() -> Self {
        Self { pos: 0, len: 0, content_known: false, tag: InsDelTag::Ins }
    }
}

impl HasLength for PositionalOp {
    fn len(&self) -> usize {
        self.components.iter().map(|c| c.len).sum::<u32>() as usize
    }
}

impl MergableSpan for PositionalOp {
    fn can_append(&self, _other: &Self) -> bool {
        false
    }

    fn append(&mut self, _other: Self) {
        unreachable!()
    }
}

impl HasLength for PositionalComponent {
    fn len(&self) -> usize {
        self.len as usize
    }
}
impl SplitableSpanHelpers for PositionalComponent {
    fn truncate_h(&mut self, at: usize) -> Self {
        let at = at as u32;
        let remainder = PositionalComponent {
            pos: if self.tag == Ins { self.pos + at } else { self.pos },
            len: self.len - at,
            content_known: self.content_known,
            tag: self.tag
        };

        self.len = at;
        remainder
    }
}
impl MergableSpan for PositionalComponent {
    fn can_append(&self, other: &Self) -> bool {
        // Positional components guarantee temporal stability, so we'll only concatenate inserts
        // when the second insert directly follows the first. Any concatenation of deletes throws
        // away information, because the result loses ordering amongst the deleted items. But
        // knowing how I want to use this, I'm kinda ok with it.
        self.content_known == other.content_known && match (self.tag, other.tag) {
            (Ins, Ins) => other.pos == self.pos + self.len,
            (Del, Del) => other.pos == self.pos,
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        self.pos = other.pos;
        self.len += other.len;
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use crate::list::positional::{PositionalComponent, InsDelTag::*};

    #[test]
    fn positional_component_splitable() {
        test_splitable_methods_valid(PositionalComponent {
            pos: 10,
            len: 5,
            content_known: false,
            tag: Ins
        });

        test_splitable_methods_valid(PositionalComponent {
            pos: 10,
            len: 5,
            content_known: false,
            tag: Del
        });

        test_splitable_methods_valid(PositionalComponent {
            pos: 10,
            len: 5,
            content_known: true,
            tag: Ins
        });
    }
}
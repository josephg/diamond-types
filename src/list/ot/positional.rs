/// Positional updates are a kind of operation (patch) which is larger than traversals but
/// retains temporal information. So, we know when each change happened relative to all other
/// changes.
///
/// Updates are made up of a series of insert / delete components, each at some position.

use smartstring::alias::{String as SmartString};
use smallvec::SmallVec;
use crate::splitable_span::SplitableSpan;
use InsDelTag::*;
use ropey::Rope;
use crate::unicount::str_pos_to_bytes;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InsDelTag { Ins, Del }

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PositionalComponent {
    pub pos: u32,
    pub len: u32,
    pub content_known: bool,
    pub tag: InsDelTag,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PositionalOp {
    pub components: SmallVec<[PositionalComponent; 1]>,
    pub content: SmartString,
}

// This is such a dirty hack and I'm not even mad about it.
const XS: &str = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx";

impl PositionalOp {
    pub fn new() -> Self { Self::default() }

    pub fn apply_to_rope(&self, rope: &mut Rope) {
        let mut new_content = self.content.as_str();

        for c in &self.components {
            let len = c.len as usize;
            let pos = c.pos as usize;
            match c.tag {
                Ins => {
                    if c.content_known {
                        let byte_len = str_pos_to_bytes(new_content, len);
                        let (here, next) = new_content.split_at(byte_len);
                        new_content = next;
                        rope.insert(pos, here);
                    } else {
                        if len < XS.len() {
                            rope.insert(pos, &XS[..len]);
                        } else {
                            let xs: String = std::iter::repeat('X').take(len).collect();
                            rope.insert(pos, &xs);
                        }
                    };
                }
                Del => {
                    rope.remove(pos..pos+len);
                }
            }
        }
    }

    pub fn from_components(components: &[(u32, PositionalComponent)], content: &Rope) -> Self {
        let mut result = Self::new();
        for (post_pos, c) in components {
            result.components.push(c.clone());
            if c.content_known {
                let chars = content.chars_at(*post_pos as usize).take(c.len as usize);
                result.content.extend(chars);
            }
        }
        result
    }
}

impl SplitableSpan for PositionalComponent {
    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
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
    use crate::splitable_span::test_splitable_methods_valid;
    use crate::list::ot::positional::{PositionalComponent, InsDelTag::*};

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
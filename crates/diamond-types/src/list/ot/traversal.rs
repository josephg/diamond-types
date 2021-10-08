/// A traversal is a kind of operation (patch) to a document which semantically traverses the whole
/// document, making changes along the way.
///
/// Traversals are generally preferred over using a series of linear positional updates, because
/// any number of single positional updates can be merged into a canonical document traversal.
///
/// For this reason, this is the data structure of choice for
/// [ot-text-unicode](https://github.com/ottypes/text-unicode/).
///
/// The downside of this format is that its not closed under compose. If a remote OT client is
/// merging changes one by one, we *must* transform any changes from that client in the same manner.
/// Otherwise the local and remote state will not converge.
///
/// Details & rough examples are here:
/// https://github.com/ottypes/text-unicode/blob/49b054d275a82a0f3aa2bafa4b77bdc3ee7513b7/NOTES.md

use smartstring::alias::{String as SmartString};
use smallvec::{SmallVec, smallvec};
use rle::{HasLength, MergableSpan, SplitableSpan};
use TraversalComponent::*;
use crate::list::Order;
use crate::list::positional::{PositionalOp, PositionalComponent, InsDelTag};
#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};
use rle::AppendRle;
use crate::unicount::count_chars;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub enum TraversalComponent {
    // Note: I tried making len an i32 and using the sign bit to store content_known, like YjsSpan.
    // The result had no change in memory usage - this enum is already has 8 bytes (because of the
    // enum field and padding). Tucking this bool only really served to drop performance by 1-5% on
    // some benchmarks. The code isn't much different in either case.
    Ins { len: u32, content_known: bool },
    Del(u32), // TODO: Add content_known for del for consistency
    Retain(u32)
}

/// A traversal is a walk over the document which inserts and deletes items.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct TraversalOp {
    pub traversal: SmallVec<[TraversalComponent; 2]>,
    pub content: SmartString,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct TraversalOpSequence {
    pub components: SmallVec<[SmallVec<[TraversalComponent; 2]>; 2]>,
    pub content: SmartString,
}

impl TraversalOp {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_insert(pos: u32, content: &str) -> Self {
        let len = count_chars(content) as u32;
        TraversalOp {
            traversal: if pos == 0 {
                smallvec![Ins { len, content_known: true }]
            } else {
                smallvec![Retain(pos), Ins { len, content_known: true }]
            },
            content: content.into()
        }
    }

    pub fn new_delete(pos: u32, del_len: u32) -> Self {
        TraversalOp {
            traversal: if pos == 0 {
                smallvec![Del(del_len)]
            } else {
                smallvec![Retain(pos), Del(del_len)]
            },
            content: "".into()
        }
    }

    pub(crate) fn append_insert(&mut self, content: &str) {
        self.traversal.push_rle(Ins { len: count_chars(content) as _, content_known: true });
        self.content.push_str(content);
    }

    fn body_from_positional(positional: PositionalComponent) -> SmallVec<[TraversalComponent; 2]> {
        let body = match positional.tag {
            InsDelTag::Ins => TraversalComponent::Ins {
                len: positional.len,
                content_known: positional.content_known
            },
            InsDelTag::Del => TraversalComponent::Del(positional.len)
        };
        if positional.pos == 0 {
            smallvec![body]
        } else {
            smallvec![Retain(positional.pos), body]
        }
    }

    pub fn apply_to_string(&self, val: &str) -> String {
        let mut old_chars = val.chars();
        let mut content_chars = self.content.chars();

        let mut result = String::new();
        for c in &self.traversal {
            match c {
                TraversalComponent::Ins { len, content_known } => {
                    // This would be much cleaner with take, except it consumes the whole iterator.
                    if *content_known {
                        for _i in 0..*len {
                            result.push(content_chars.next().unwrap());
                        }
                        // result.extend(content_chars.take(*len as usize));
                    } else {
                        result.extend(std::iter::repeat('X').take(*len as usize));
                    }
                }
                Del(len) => {
                    for _i in 0..*len { old_chars.next(); }
                    // old_chars.take(*len as usize);
                    // old_chars.advance_by(len as usize).unwrap();
                }
                Retain(len) => {
                    for _i in 0..*len {
                        result.push(old_chars.next().unwrap());
                    }
                    // result.extend(old_chars.take(*len as usize));
                }
            }
        }
        result
    }

    pub(crate) fn check(&self) {
        let mut content_len = 0;

        for c in &self.traversal {
            if let TraversalComponent::Ins { len, content_known: true } = c {
                content_len += *len;
            }

            // Components are not allowed to be no-ops.
            assert!(!c.is_noop());
        }

        assert_eq!(content_len as usize, count_chars(&self.content));
    }
}

impl From<PositionalOp> for TraversalOpSequence {
    fn from(positional: PositionalOp) -> Self {
        Self {
            components: positional.components
                .into_iter()
                .map(TraversalOp::body_from_positional)
                .collect(),
            content: positional.content
        }
    }
}

impl TraversalComponent {
    pub(super) fn pre_len(&self) -> Order {
        match self {
            Retain(len) | Del(len) => *len,
            Ins{..} => 0,
        }
    }

    pub(super) fn post_len(&self) -> Order {
        match self {
            Retain(len) => *len,
            Del(_) => 0,
            Ins { len, .. } => *len,
        }
    }

    // TODO: This function is pretty useless. Consider replacing it with is_empty().
    pub fn len(&self) -> u32 {
        match self {
            Retain(len) | Del(len) => *len,
            Ins { len, .. } => *len,
        }
    }
}

impl Default for TraversalComponent {
    fn default() -> Self {
        // For range tree
        Retain(u32::MAX)
    }
}

impl HasLength for TraversalComponent {
    fn len(&self) -> usize {
        match self {
            Ins { len, .. } | Del(len) | Retain(len) => *len as usize,
        }
    }
}
impl SplitableSpan for TraversalComponent {
    // Might be able to write this cleaner with .len_mut_ref() method and such.
    fn truncate(&mut self, at: usize) -> Self {
        match self {
            Ins { len, content_known } => {
                let remainder = Ins {
                    len: *len - at as Order,
                    content_known: *content_known,
                };
                *len = at as Order;
                remainder
            }
            Del(len) => {
                let remainder = Del(*len - at as Order);
                *len = at as Order;
                remainder
            }
            Retain(len) => {
                let remainder = Retain(*len - at as Order);
                *len = at as _;
                remainder
            }
        }
    }
}
impl MergableSpan for TraversalComponent {
    fn can_append(&self, other: &Self) -> bool {
        match (self, other) {
            (Ins { content_known: true, .. }, Ins { content_known: true, .. }) => true,
            (Ins { content_known: false, .. }, Ins { content_known: false, .. }) => true,
            (Del(_), Del(_)) => true,
            (Retain(_), Retain(_)) => true,
            (_, _) => false
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (Ins { len, .. }, Ins { len: len2, .. }) => *len += len2,
            (Del(len), Del(len2)) => *len += len2,
            (Retain(len), Retain(len2)) => *len += len2,
            (_, _) => unreachable!()
        }
    }

    fn prepend(&mut self, other: Self) {
        // We're symmetric.
        self.append(other);
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use crate::list::ot::traversal::*;
    use std::mem::size_of;
    use crate::list::positional::PositionalOp;

    #[test]
    fn traverse_op_checks() {
        test_splitable_methods_valid(Ins { len: 5, content_known: false });
        test_splitable_methods_valid(Ins { len: 5, content_known: true });
        test_splitable_methods_valid(Del(5));
        test_splitable_methods_valid(Retain(5));
    }

    #[test]
    fn print_sizes() {
        dbg!(size_of::<TraversalComponent>());
        dbg!(size_of::<TraversalOpSequence>());
        dbg!(size_of::<TraversalOp>());
        dbg!(size_of::<PositionalOp>());
    }
}
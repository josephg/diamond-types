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
use smallvec::SmallVec;
use crate::splitable_span::SplitableSpan;
use TraversalComponent::*;
use crate::list::Order;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TraversalComponent {
    Ins { len: u32, content_known: bool },
    Del(u32), // TODO: Add content_known for del for consistency
    Retain(u32)
}

/// A traversal is a walk over the document which inserts and deletes items.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct TraversalOp {
    pub traversal: SmallVec<[TraversalComponent; 2]>,
    pub content: SmartString,
}

impl TraversalOp {
    pub fn new() -> Self {
        Self::default()
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
}

impl Default for TraversalComponent {
    fn default() -> Self {
        // For range tree
        Retain(u32::MAX)
    }
}

impl SplitableSpan for TraversalComponent {
    fn len(&self) -> usize {
        match self {
            Ins { len, .. } | Del(len) | Retain(len) => *len as usize,
        }
    }

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
    use crate::splitable_span::test_splitable_methods_valid;
    use super::TraversalComponent::*;

    #[test]
    fn traverse_op_checks() {
        test_splitable_methods_valid(Ins { len: 5, content_known: false });
        test_splitable_methods_valid(Ins { len: 5, content_known: true });
        test_splitable_methods_valid(Del(5));
        test_splitable_methods_valid(Retain(5));
    }
}
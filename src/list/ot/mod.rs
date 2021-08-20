mod positionmap;

use smartstring::alias::{String as SmartString};
use smallvec::SmallVec;
use crate::splitable_span::SplitableSpan;
use TraversalComponent::*;
use crate::list::Order;
use crate::range_tree::{EntryTraits};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TraversalComponent {
    Ins { len: u32, content_known: bool },
    Del(u32),
    Retain(u32)
}

/// A traversal is a walk over the document which inserts and deletes items.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct TraversalOp {
    traversal: SmallVec<[TraversalComponent; 2]>,
    content: SmartString,
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
    fn pre_len(&self) -> Order {
        match self {
            Retain(len) | Del(len) => *len,
            Ins{..} => 0,
        }
    }

    fn post_len(&self) -> Order {
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

impl EntryTraits for TraversalComponent {
    fn is_valid(&self) -> bool {
        self.len() != u32::MAX as usize
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
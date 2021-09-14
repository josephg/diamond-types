use rle::splitable_span::SplitableSpan;
use crate::{Toggleable, ContentLength};
use rle::Searchable;

/// This is a simple span object for testing.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TestRange {
    pub id: u32,
    pub len: u32,
    pub is_activated: bool,
}

impl Default for TestRange {
    fn default() -> Self {
        Self {
            id: u32::MAX,
            len: u32::MAX,
            is_activated: false
        }
    }
}

impl SplitableSpan for TestRange {
    fn len(&self) -> usize { self.len as usize }
    fn truncate(&mut self, at: usize) -> Self {
        assert!(at > 0 && at < self.len as usize);
        let other = Self {
            id: self.id + at as u32,
            len: self.len - at as u32,
            is_activated: self.is_activated
        };
        self.len = at as u32;
        other
    }

    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let mut other = *self;
        *self = other.truncate(at);
        other
    }

    fn can_append(&self, other: &Self) -> bool {
        other.id == self.id + self.len && other.is_activated == self.is_activated
    }

    fn append(&mut self, other: Self) {
        assert!(self.can_append(&other));
        self.len += other.len;
    }

    fn prepend(&mut self, other: Self) {
        assert!(other.can_append(&self));
        self.len += other.len;
        self.id = other.id;
    }
}

impl Toggleable for TestRange {
    fn is_activated(&self) -> bool {
        self.is_activated
    }

    fn mark_activated(&mut self) {
        assert!(!self.is_activated);
        self.is_activated = true;
    }

    fn mark_deactivated(&mut self) {
        assert!(self.is_activated);
        self.is_activated = false;
    }
}

impl ContentLength for TestRange {
    fn content_len(&self) -> usize {
        if self.is_activated { self.len() } else { 0 }
    }
}

impl Searchable for TestRange {
    type Item = (u32, bool);

    fn contains(&self, loc: Self::Item) -> Option<usize> {
        if self.is_activated == loc.1 && loc.0 >= self.id && loc.0 < (self.id + self.len) {
            Some((loc.0 - self.id) as usize)
        } else { None }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        (offset as u32 + self.id, self.is_activated)
    }
}
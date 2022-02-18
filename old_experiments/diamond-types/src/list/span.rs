use std::fmt::{Debug, DebugStruct, Formatter};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpan};

use content_tree::ContentLength;
use content_tree::Toggleable;
use crate::list::{ROOT_TIME, Time};

#[cfg(feature = "serde")]
use serde_crate::{Deserialize, Serialize};

/// This is exposed for diamond-wasm's vis output. The internal fields here should not be considered
/// part of the public API and are not to be relied on.
#[derive(Copy, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(crate="serde_crate"))]
pub struct YjsSpan {
    /// The ID of this entry. Well, the ID of the first entry in this span.
    pub time: Time,

    /**
     * The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: Time,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: Time,

    pub len: i32, // negative if deleted.
}

impl YjsSpan {
    pub fn origin_left_at_offset(&self, at: u32) -> Time {
        if at == 0 { self.origin_left }
        else { self.time + at - 1 }
    }

    pub fn activated(mut self) -> Self {
        self.len = self.len.abs();
        self
    }

    pub fn order_len(&self) -> Time {
        self.len.abs() as _
    }
}

impl HasLength for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.len.abs() as usize }
}

impl SplitableSpan for YjsSpan {

    fn truncate(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        let at_signed = at as i32 * self.len.signum();
        let other = YjsSpan {
            time: self.time + at as Time,
            origin_left: self.time + at as u32 - 1,
            origin_right: self.origin_right,
            len: self.len - at_signed
        };

        self.len = at_signed;
        other
    }
}

impl MergableSpan for YjsSpan {
    // Could have a custom truncate_keeping_right method here - I once did. But the optimizer
    // does a great job flattening the generic implementation anyway.

    // This method gets inlined all over the place.
    // TODO: Might be worth tagging it with inline(never) and seeing what happens.
    fn can_append(&self, other: &Self) -> bool {
        let len = self.len.abs() as u32;
        (self.len > 0) == (other.len > 0)
            && other.time == self.time + len
            && other.origin_left == other.time - 1
            && other.origin_right == self.origin_right
    }

    #[inline(always)]
    fn append(&mut self, other: Self) {
        self.len += other.len
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(other.can_append(self));
        self.time = other.time;
        self.len += other.len;
        self.origin_left = other.origin_left;
    }
}

impl Searchable for YjsSpan {
    type Item = Time;

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        if (loc >= self.time) && (loc < self.time + self.len.abs() as u32) {
            Some((loc - self.time) as usize)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.time + offset as Time
    }
}

impl ContentLength for YjsSpan {
    #[inline(always)]
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }

    fn content_len_at_offset(&self, offset: usize) -> usize {
        // let mut e = *self;
        // e.truncate(offset);
        // e.content_len()
        self.len.clamp(0, offset as i32) as usize
    }
}

impl Toggleable for YjsSpan {
    fn is_activated(&self) -> bool {
        self.len > 0
    }

    fn mark_activated(&mut self) {
        debug_assert!(self.len < 0);
        self.len = -self.len;
    }

    fn mark_deactivated(&mut self) {
        debug_assert!(self.len > 0);
        self.len = -self.len
    }
}


#[derive(Debug)]
struct RootTime;

pub(crate) fn debug_time(fmt: &mut DebugStruct, name: &str, val: Time) {
    match val {
        ROOT_TIME => {
            fmt.field(name, &RootTime);
        },
        start => {
            fmt.field(name, &start);
        }
    }
}

impl Debug for YjsSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("YjsSpan");
        s.field("time", &self.time);
        debug_time(&mut s, "origin_left", self.origin_left);
        debug_time(&mut s, "origin_right", self.origin_right);
        s.field("len", &self.len);
        s.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use rle::test_splitable_methods_valid;

    use crate::list::span::YjsSpan;

    #[test]
    fn print_span_sizes() {
        // Last I checked, 16 bytes.
        println!("size of YjsSpan {}", size_of::<YjsSpan>());
    }

    #[test]
    fn yjsspan_entry_valid() {
        test_splitable_methods_valid(YjsSpan {
            time: 10,
            origin_left: 20,
            origin_right: 30,
            len: 5
        });

        test_splitable_methods_valid(YjsSpan {
            time: 10,
            origin_left: 20,
            origin_right: 30,
            len: -5
        });
    }
}
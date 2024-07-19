use std::fmt::{Debug, DebugStruct, Formatter};
use rle::{HasLength, MergableSpan, Searchable, SplitableSpanHelpers};

use crate::list::{ROOT_LV, LV};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use crate::ost::content_tree::Content;

/// This is exposed for diamond-wasm's vis output. The internal fields here should not be considered
/// part of the public API and are not to be relied on.
#[derive(Copy, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct YjsSpan {
    /// The ID of this entry. Well, the ID of the first entry in this span.
    pub lv: LV,

    /**
     * The origin_left is only for the first item in the span. Each subsequent item has an
     * origin_left of order+offset
     */
    pub origin_left: LV,

    /**
     * Each item in the span has the same origin_right.
     */
    pub origin_right: LV,

    pub len: isize, // negative if deleted.
}

impl YjsSpan {
    pub fn origin_left_at_offset(&self, at: usize) -> LV {
        if at == 0 { self.origin_left }
        else { self.lv + at - 1 }
    }

    pub fn activated(mut self) -> Self {
        self.len = self.len.abs();
        self
    }

    pub fn order_len(&self) -> LV {
        self.len.abs() as _
    }

    pub fn contains(&self, time: LV) -> bool {
        self.lv <= time && time < self.lv + self.len.abs() as LV
    }
    
    pub fn is_activated(&self) -> bool {
        self.len > 0
    }
}

impl HasLength for YjsSpan {
    #[inline(always)]
    fn len(&self) -> usize { self.len.abs() as usize }
}

impl SplitableSpanHelpers for YjsSpan {

    fn truncate_h(&mut self, at: usize) -> Self {
        debug_assert!(at > 0);
        let at_signed = at as isize * self.len.signum();
        let other = YjsSpan {
            lv: self.lv + at,
            origin_left: self.lv + at - 1,
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
        let len = self.len.abs() as LV;
        (self.len > 0) == (other.len > 0)
            && other.lv == self.lv + len
            && other.origin_left == other.lv - 1
            && other.origin_right == self.origin_right
    }

    #[inline(always)]
    fn append(&mut self, other: Self) {
        self.len += other.len
    }

    fn prepend(&mut self, other: Self) {
        debug_assert!(other.can_append(self));
        self.lv = other.lv;
        self.len += other.len;
        self.origin_left = other.origin_left;
    }
}

impl Searchable for YjsSpan {
    type Item = LV;

    fn get_offset(&self, loc: Self::Item) -> Option<usize> {
        if (loc >= self.lv) && (loc < self.lv + self.len.abs() as LV) {
            Some((loc - self.lv) as usize)
        } else {
            None
        }
    }

    fn at_offset(&self, offset: usize) -> Self::Item {
        self.lv + offset as LV
    }
}

// impl ContentLength for YjsSpan {
//     #[inline(always)]
//     fn content_len(&self) -> usize {
//         self.len.max(0) as usize
//     }
//
//     fn content_len_at_offset(&self, offset: usize) -> usize {
//         // let mut e = *self;
//         // e.truncate(offset);
//         // e.content_len()
//         self.len.clamp(0, offset as isize) as usize
//     }
// }
//
// impl Toggleable for YjsSpan {
//     fn is_activated(&self) -> bool {
//         self.len > 0
//     }
//
//     fn mark_activated(&mut self) {
//         debug_assert!(self.len < 0);
//         self.len = -self.len;
//     }
//
//     fn mark_deactivated(&mut self) {
//         debug_assert!(self.len > 0);
//         self.len = -self.len
//     }
// }

impl Content for YjsSpan {
    fn content_len(&self) -> usize {
        self.len.max(0) as usize
    }

    fn exists(&self) -> bool {
        self.len != 0
    }

    fn takes_up_space(&self) -> bool {
        self.len > 0
    }

    fn none() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
struct RootTime;

pub(crate) fn debug_time(fmt: &mut DebugStruct, name: &str, val: LV) {
    match val {
        ROOT_LV => {
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
        s.field("time", &self.lv);
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
            lv: 10,
            origin_left: 20,
            origin_right: 30,
            len: 5
        });

        test_splitable_methods_valid(YjsSpan {
            lv: 10,
            origin_left: 20,
            origin_right: 30,
            len: -5
        });
    }
}
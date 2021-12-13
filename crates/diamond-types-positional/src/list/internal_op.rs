use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::list::operation::InsDelTag;
use crate::list::operation::InsDelTag::*;
use crate::localtime::TimeSpan;
use crate::rev_span::TimeSpanRev;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OperationInternal {
    pub span: TimeSpanRev,

    pub tag: InsDelTag,

    /// Offset into self.ins_content or del_content. This is essentially a poor man's pointer.
    ///
    /// Note this number is a *byte offset*.
    pub content_pos: Option<usize>,
}

impl HasLength for OperationInternal {
    fn len(&self) -> usize {
        self.span.len()
    }
}


// pub(crate) fn truncate_tagged_span(range: &mut TimeSpanRev, tag: InsDelTag, at: usize) -> TimeSpan {
//     let len = range.len();
//
//     let start2 = if range.fwd && tag == Ins {
//         range.span.start + at
//     } else {
//         range.span.start
//     };
//
//     if !range.fwd && tag == Del {
//         range.span.start = range.span.end - at;
//     }
//
//     TimeSpan { start: start2, end: start2 + len - at }
// }

impl TimeSpanRev {
    // TODO: Consider rewriting this as some form of truncate().
    #[inline]
    pub(crate) fn split_op_span(range: TimeSpanRev, tag: InsDelTag, at: usize) -> (TimeSpan, TimeSpan) {
        let (start1, start2) = match (range.fwd, tag) {
            (true, Ins) => (range.span.start, range.span.start + at),
            (false, Del) => (range.span.end - at, range.span.start),
            _ => (range.span.start, range.span.start)
        };

        (
            TimeSpan { start: start1, end: start1 + at },
            TimeSpan { start: start2, end: start2 + range.len() - at },
        )
    }

    // TODO: Move this method. I'd like to put it in TimeSpanRev's file, but we only define
    // InsDelTag locally so that doesn't make sense. Eh.
    #[inline]
    pub(crate) fn can_append_ops(tag: InsDelTag, a: &TimeSpanRev, b: &TimeSpanRev) -> bool {
        // This logic can be simplified to a single expression, but godbolt says the compiler still
        // produces branchy code anyway so eh.

        if (a.len() == 1 || a.fwd) && (b.len() == 1 || b.fwd)
            && ((tag == Ins && b.span.start == a.span.end)
            || (tag == Del && b.span.start == a.span.start)) {
            // Append in the forward sort of way.
            return true;
        }

        // TODO: Handling reversed items is currently limited to Del. Undo this.
        if tag == Del && (a.len() == 1 || !a.fwd) && (b.len() == 1 || !b.fwd)
            && ((tag == Ins && b.span.start == a.span.start)
            || (tag == Del && b.span.end == a.span.start)) {
            // We can append in a reverse sort of way
            return true;
        }

        false
    }

    pub(crate) fn append_ops(&mut self, tag: InsDelTag, other: TimeSpanRev) {
        debug_assert!(Self::can_append_ops(tag, self, &other));

        self.fwd = other.span.start >= self.span.start && (other.span.start != self.span.start || tag == Del);

        // self.span.end += other.span.len(); // I bet there's a cleaner way to do this.
        // self.len += other.len;

        if tag == Del && !self.fwd {
            self.span.start = other.span.start;
        } else {
            self.span.end += other.span.len();
        }
    }
}

impl SplitableSpan for OperationInternal {
    fn truncate(&mut self, at: usize) -> Self {
        // Note we can't use self.span.truncate() because it assumes the span is absolute, but
        // actually how the span splits depends on the tag (and some other stuff).
        let (a, b) = TimeSpanRev::split_op_span(self.span, self.tag, at);
        self.span.span = a;

        OperationInternal {
            span: TimeSpanRev { span: b, fwd: self.span.fwd },
            tag: self.tag,
            content_pos: self.content_pos.map(|p| p + at),
        }
    }
}

// impl MergableSpan for OperationInternal {
//     fn can_append(&self, other: &Self) -> bool {
//         let content_can_append = match (self.content_pos, other.content_pos) {
//             (None, None) => true,
//             (Some(a), Some(b)) => {
//
//             },
//         };
//
//         self.tag == other.tag
//             && self.span.can_append(&other.span)
//             && content_can_append
//     }
//
//     fn append(&mut self, other: Self) {
//         self.span.append(other.span);
//     }
// }
use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::list::operation::{InsDelTag, Operation};
use crate::list::operation::InsDelTag::*;
use crate::list::OpLog;
use crate::localtime::TimeSpan;
use crate::rev_span::TimeSpanRev;
use crate::unicount::chars_to_bytes;

/// This is an internal structure for passing around information about a change. Notably the content
/// of the change is not stored here - but is instead stored in a contiguous array in the oplog
/// itself. This has 2 benefits:
///
/// - Speed / size improvements. The number of items each operation references varies wildly, and
///   storing the content itself in a block in the oplog keeps fragmentation down.
/// - This makes supporting other data types much easier - because there's a lot less code which
///   needs to adapt to the content type itself.
///
/// Note that OperationInternal can't directly implement
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct OperationInternal {
    pub span: TimeSpanRev,

    pub tag: InsDelTag,

    /// Byte range in self.ins_content or del_content where our content is being held. This is
    /// essentially a poor man's pointer.
    ///
    /// Note this stores a *byte offset*.
    pub content_pos: Option<TimeSpan>,
}

impl OperationInternal {
    #[inline]
    pub fn start(&self) -> usize {
        self.span.span.start
    }

    #[inline]
    pub fn end(&self) -> usize {
        self.span.span.end
    }

    // Note we can't implement SplitableSpan because we can't adjust content_pos correctly without
    // reference to the contained data.
    pub(crate) fn truncate(&mut self, at: usize, content: &str) -> Self {
        // Note we can't use self.span.truncate() because it assumes the span is absolute, but
        // actually how the span splits depends on the tag (and some other stuff).
        // let (a, b) = TimeSpanRev::split_op_span(self.span, self.tag, at);
        // self.span.span = a;
        let span = self.span.truncate_tagged_span(self.tag, at);

        let content_pos = if let Some(p) = &mut self.content_pos {
            let byte_offset = chars_to_bytes(&content[p.start..p.end], at);
            Some(p.truncate(byte_offset))
        } else { None };

        OperationInternal {
            span,
            tag: self.tag,
            content_pos,
        }
    }

    #[inline]
    pub(crate) fn truncate_keeping_right(&mut self, at: usize, content: &str) -> Self {
        let mut other = self.clone();
        *self = other.truncate(at, content);
        other
    }

    #[allow(unused)]
    pub(crate) fn get_content<'a>(&self, oplog: &'a OpLog) -> Option<&'a str> {
        self.content_pos.map(|span| {
            let c = oplog.content_str(self.tag);
            &c[span.start..span.end]
        })
    }

    #[allow(unused)]
    pub(crate) fn to_operation(&self, oplog: &OpLog) -> Operation {
        let content = self.get_content(oplog);
        (self, content).into()
    }
}

impl HasLength for OperationInternal {
    fn len(&self) -> usize {
        self.span.len()
    }
}

impl SplitableSpan for OperationInternal {
    fn truncate(&mut self, at: usize) -> Self {
        Self {
            span: self.span.truncate_tagged_span(self.tag, at),
            tag: self.tag,
            content_pos: self.content_pos.truncate(at)
        }
    }
}

impl TimeSpanRev {
    // These are 3 versions of essentially the same function. TODO: decide which version of this
    // logic to keep. (Only keep 1!).
    //
    // In godbolt these variants all look pretty similar.
    #[inline]
    pub(crate) fn truncate_tagged_span(&mut self, tag: InsDelTag, at: usize) -> TimeSpanRev {
        let len = self.len();

        let start2 = if self.fwd && tag == Ins {
            self.span.start + at
        } else {
            self.span.start
        };

        if !self.fwd && tag == Del {
            self.span.start = self.span.end - at;
        }
        self.span.end = self.span.start + at;

        TimeSpanRev {
            span: TimeSpan { start: start2, end: start2 + len - at },
            fwd: self.fwd
        }
    }

    // pub(crate) fn truncate_tagged_span(&mut self, tag: InsDelTag, at: usize) -> TimeSpan {
    //     let len = self.len();
    //     let (start1, start2) = match (self.fwd, tag) {
    //         (true, Ins) => (self.span.start, self.span.start + at),
    //         (false, Del) => (self.span.end - at, self.span.start),
    //         _ => (self.span.start, self.span.start)
    //     };
    //
    //     self.span = TimeSpan { start: start1, end: start1 + at };
    //     TimeSpan { start: start2, end: start2 + len - at }
    // }

    // This logic is interchangable with truncate_tagged_span above.
    #[inline]
    #[allow(unused)] // FOR NOW...
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

impl MergableSpan for OperationInternal {
    fn can_append(&self, other: &Self) -> bool {
        let can_append_content = match (&self.content_pos, &other.content_pos) {
            (Some(a), Some(b)) => a.can_append(b),
            (None, None) => true,
            _ => false
        };

        self.tag == other.tag
            && can_append_content
            && TimeSpanRev::can_append_ops(self.tag, &self.span, &other.span)
    }

    fn append(&mut self, other: Self) {
        self.span.append_ops(self.tag, other.span);
        if let (Some(a), Some(b)) = (&mut self.content_pos, other.content_pos) {
            a.append(b);
        }
    }
}

#[cfg(test)]
mod test {
    use rle::{SplitableSpan, test_splitable_methods_valid};
    use crate::list::internal_op::OperationInternal;
    use crate::list::operation::InsDelTag;
    use crate::localtime::TimeSpan;
    use crate::rev_span::TimeSpanRev;

    #[test]
    fn internal_op_splitable() {
        test_splitable_methods_valid(OperationInternal {
            span: (10..20).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((1000..1010).into()),
        });

        // I can't test the other splitablespan variants like this because they don't support
        // appending.
    }

    #[test]
    fn truncate_fwd_delete() {
        // Regression.
        let mut op = OperationInternal {
            span: (10..15).into(),
            tag: InsDelTag::Del,
            content_pos: Some((0..5).into()),
        };

        // let rem = op.truncate(2, "abcde");
        let rem = SplitableSpan::truncate(&mut op, 2);

        assert_eq!(op, OperationInternal {
            span: (10..12).into(),
            tag: InsDelTag::Del,
            content_pos: Some((0..2).into())
        });

        assert_eq!(rem, OperationInternal {
            span: (10..13).into(),
            tag: InsDelTag::Del,
            content_pos: Some((2..5).into())
        });

        dbg!(op, rem);
    }

    #[test]
    #[ignore]
    #[allow(dead_code)] // Don't complain about unused fields.
    fn print_sizes() {
        struct V1 {
            span: TimeSpanRev,
            tag: InsDelTag,
            content_pos: Option<TimeSpan>,
        }
        struct V2 {
            span: TimeSpan,
            rev: bool,
            tag: InsDelTag,
            content_pos: Option<TimeSpan>,
        }
        struct V3 {
            span: TimeSpan,
            rev: bool,
            tag: InsDelTag,
            content_pos: TimeSpan,
        }

        dbg!(std::mem::size_of::<V1>());
        dbg!(std::mem::size_of::<V2>());
        dbg!(std::mem::size_of::<V3>());
    }
}
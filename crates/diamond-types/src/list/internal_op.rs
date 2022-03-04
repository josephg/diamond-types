use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanCtx};
use crate::list::operation::{InsDelTag, Operation};
use crate::list::operation::InsDelTag::*;
use crate::list::{OpLog, switch};
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
    /// The span of content which is inserted or deleted.
    ///
    /// For inserts, this describes the resulting location (span) of the new characters.
    /// For deletes, this names the range of the set of characters deleted.
    ///
    /// This span is reversible. The span.rev tag specifies if the span is reversed chronologically.
    /// That is, characters are inserted or deleted in the reverse order chronologically.
    pub span: TimeSpanRev,

    /// Is this an insert or delete?
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

    pub(crate) fn get_content<'a>(&self, oplog: &'a OpLog) -> Option<&'a str> {
        self.content_pos.map(|span| {
            let c = oplog.operation_ctx.switch(self.tag);
            &c[span.start..span.end]
        })
    }

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

#[derive(Debug, Clone)]
pub(crate) struct OperationCtx {
    pub ins_content: String,
    pub del_content: String,
}

impl OperationCtx {
    pub fn new() -> Self {
        Self {
            ins_content: String::new(),
            del_content: String::new()
        }
    }

    pub(crate) fn switch(&self, tag: InsDelTag) -> &str {
        switch(tag, self.ins_content.as_str(), self.del_content.as_str())
    }
}

impl SplitableSpanCtx for OperationInternal {
    type Ctx = OperationCtx;

    // Note we can't implement SplitableSpan because we can't adjust content_pos correctly without
    // reference to the contained data.
    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        debug_assert!(self.span.span.start + at <= self.span.span.end);

        // Note we can't use self.span.truncate() because it assumes the span is absolute, but
        // actually how the span splits depends on the tag (and some other stuff).
        // let (a, b) = TimeSpanRev::split_op_span(self.span, self.tag, at);
        // self.span.span = a;
        let span = self.span.truncate_tagged_span(self.tag, at);

        let content_pos = if let Some(p) = &mut self.content_pos {
            let content = switch(self.tag, &ctx.ins_content, &ctx.del_content);
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
    fn truncate_keeping_right_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        let mut other = self.clone();
        *self = other.truncate_ctx(at, ctx);
        other
    }

    // fn truncate(&mut self, at: usize) -> Self {
    //     // panic!("This is")
    //     Self {
    //         span: self.span.truncate_tagged_span(self.tag, at),
    //         tag: self.tag,
    //         content_pos: self.content_pos.truncate(at)
    //     }
    // }
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
    use rle::{SplitableSpanCtx, test_splitable_methods_valid_ctx};
    use crate::list::internal_op::{OperationCtx, OperationInternal};
    use crate::list::operation::InsDelTag;
    use crate::localtime::TimeSpan;
    use crate::rev_span::TimeSpanRev;

    #[test]
    fn internal_op_splitable() {
        test_splitable_methods_valid_ctx(OperationInternal {
            span: (10..20).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((0..10).into()),
        }, &OperationCtx {
            ins_content: "0123456789".to_string(),
            del_content: "".to_string()
        });

        let s2 = "↯1↯3↯5↯7↯9";
        test_splitable_methods_valid_ctx(OperationInternal {
            span: (10..20).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((0..s2.len()).into()),
        }, &OperationCtx {
            ins_content: s2.to_string(), // too easy? Maybe..
            del_content: "".to_string()
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
        let rem = op.truncate_ctx(2, &OperationCtx {
            ins_content: "".to_string(),
            del_content: "abcde".to_string()
        });

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
    fn split_around_unicode() {
        // The ¥ symbol is a 2-byte encoding. And ↯ is 3 bytes.
        let ctx = OperationCtx {
            ins_content: "¥123↯".to_string(),
            del_content: "¥123↯".to_string()
        };

        let op = OperationInternal {
            span: (10..15).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((0..ctx.ins_content.len()).into())
        };

        let (a, b) = op.split_ctx(1, &ctx);
        assert_eq!(a, OperationInternal {
            span: (10..11).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((0..2).into())
        });
        assert_eq!(b, OperationInternal {
            span: (11..15).into(),
            tag: InsDelTag::Ins,
            content_pos: Some((2..ctx.ins_content.len()).into())
        });
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
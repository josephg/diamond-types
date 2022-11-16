use std::borrow::Borrow;
use smallvec::SmallVec;
use rle::{HasLength, SplitableSpanCtx};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::ListOpLog;
use crate::list::operation::TextOperation;
use crate::dtrange::DTRange;
use crate::rle::{KVPair, RleVec};
use crate::LV;

#[derive(Debug)]
pub(crate) struct OpMetricsIter<'a> {
    list: &'a RleVec<KVPair<ListOpMetrics>>,
    pub(crate) ctx: &'a ListOperationCtx,

    idx: usize,
    range: DTRange,
}

/// Wrapper around OpMetricsIter which yields (metrics, content) instead of just metrics.
#[derive(Debug)]
pub(crate) struct OpIterFast<'a>(OpMetricsIter<'a>);

impl<'a> From<OpMetricsIter<'a>> for OpIterFast<'a> {
    fn from(inner: OpMetricsIter<'a>) -> Self {
        OpIterFast(inner)
    }
}

impl<'a> Iterator for OpMetricsIter<'a> {
    type Item = KVPair<ListOpMetrics>;

    fn next(&mut self) -> Option<Self::Item> {
        // I bet there's a more efficient way to write this function.
        if self.idx >= self.list.0.len() { return None; }

        let KVPair(mut time, mut c) = self.list[self.idx].clone();
        if time >= self.range.end { return None; }

        if time + c.len() > self.range.end {
            c.truncate_ctx(self.range.end - time, self.ctx);
        }

        if time < self.range.start {
            c.truncate_keeping_right_ctx(self.range.start - time, self.ctx);
            time = self.range.start;
        }

        self.idx += 1;
        Some(KVPair(time, c))
    }
}

impl<'a> Iterator for OpIterFast<'a> {
    type Item = (KVPair<ListOpMetrics>, Option<&'a str>);

    fn next(&mut self) -> Option<Self::Item> {
        let metrics = self.0.next()?;
        let content = self.0.get_content(&metrics);
        Some((metrics, content))
    }
}

impl<'a> OpMetricsIter<'a> {
    pub(crate) fn new(list: &'a RleVec<KVPair<ListOpMetrics>>, ctx: &'a ListOperationCtx, range: DTRange) -> Self {
        let mut iter = OpMetricsIter {
            list,
            ctx,
            idx: 0,
            range
        };
        iter.prime(range);
        iter
    }

    fn prime(&mut self, range: DTRange) {
        self.range = range;
        self.idx = if range.is_empty() { 0 } else { self.list.find_next_index(range.start) };
    }

    #[allow(unused)]
    pub(crate) fn is_empty(&self) -> bool {
        self.idx >= self.list.0.len() || self.range.is_empty()
    }

    pub(crate) fn get_content(&self, metrics: &KVPair<ListOpMetrics>) -> Option<&'a str> {
        metrics.1.content_pos.map(|pos| {
            self.ctx.get_str(metrics.1.kind, pos)
        })
    }
}

impl<'a> OpIterFast<'a> {
    fn new(oplog: &'a ListOpLog, range: DTRange) -> Self {
        Self(OpMetricsIter::new(&oplog.operations, &oplog.operation_ctx, range))
    }
}

/// This is a variant on OpIterFast which returns operations since some (complex) point in time in
/// a document.
#[derive(Debug)]
struct OpIterRanges<'a> {
    ranges: SmallVec<[DTRange; 4]>, // We own this. This is in descending order.
    current: OpIterFast<'a>
}

impl<'a> OpIterRanges<'a> {
    fn new(oplog: &'a ListOpLog, mut r: SmallVec<[DTRange; 4]>) -> Self {
        let last = r.pop().unwrap_or_else(|| (0..0).into());
        Self {
            ranges: r,
            current: OpIterFast::new(oplog, last)
        }
    }
}

impl<'a> Iterator for OpIterRanges<'a> {
    // type Item = KVPair<OperationInternal>;
    type Item = (KVPair<ListOpMetrics>, Option<&'a str>);

    fn next(&mut self) -> Option<Self::Item> {
        let inner_next = self.current.next();
        if inner_next.is_some() { return inner_next; }

        if let Some(range) = self.ranges.pop() {
            debug_assert!(!range.is_empty());
            self.current.0.prime(range);
            let inner_next = self.current.next();
            if inner_next.is_some() { return inner_next; }
        }

        None
    }
}

impl ListOpLog {
    // TODO: Consider removing these functions if they're never used.
    #[allow(unused)]
    pub(crate) fn iter_metrics_range(&self, range: DTRange) -> OpMetricsIter {
        OpMetricsIter::new(&self.operations, &self.operation_ctx, range)
    }

    #[allow(unused)]
    pub(crate) fn iter_metrics(&self) -> OpMetricsIter {
        self.iter_metrics_range((0..self.len()).into())
    }

    pub(crate) fn iter_range_simple(&self, range: DTRange) -> OpIterFast {
        OpIterFast::new(self, range)
    }

    pub fn iter_range_since(&self, local_version: &[LV]) -> impl Iterator<Item = TextOperation> + '_ {
        let (only_a, only_b) = self.cg.graph.diff(local_version, self.cg.version.as_ref());
        assert!(only_a.is_empty());

        OpIterRanges::new(self, only_b)
            .map(|pair| (pair.0.1, pair.1).into())
    }

    pub(crate) fn iter_fast(&self) -> OpIterFast {
        OpIterFast::new(self, (0..self.len()).into())
    }

    pub fn iter(&self) -> impl Iterator<Item = TextOperation> + '_ {
        self.iter_fast().map(|pair| (pair.0.1, pair.1).into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::list::operation::ListOpKind;
    use crate::rle::{KVPair, RleVec};
    use ListOpKind::*;

    #[test]
    fn iter_smoke() {
        let mut ops: RleVec<KVPair<ListOpMetrics>> = RleVec::new();

        ops.push(KVPair(0, ListOpMetrics {
            loc: (100..110).into(),
            kind: Ins,
            content_pos: Some((0..10).into()),
        }));
        ops.push(KVPair(10, ListOpMetrics {
            loc: (200..220).into(),
            kind: Del,
            content_pos: None,
        }));

        let ctx = ListOperationCtx {
            ins_content: "0123456789".to_string().into_bytes(),
            del_content: "".to_string().into_bytes()
        };

        assert_eq!(OpMetricsIter::new(&ops, &ctx, (0..30).into()).collect::<Vec<_>>(), ops.0.as_slice());
        
        assert_eq!(OpMetricsIter::new(&ops, &ctx, (1..5).into()).collect::<Vec<_>>(), &[KVPair(1, ListOpMetrics {
            loc: (101..105).into(),
            kind: Ins,
            content_pos: Some((1..5).into()),
        })]);

        assert_eq!(OpMetricsIter::new(&ops, &ctx, (6..16).into()).collect::<Vec<_>>(), &[
            KVPair(6, ListOpMetrics {
                loc: (106..110).into(),
                kind: Ins,
                content_pos: Some((6..10).into()),
            }),
            KVPair(10, ListOpMetrics {
                loc: (200..206).into(),
                kind: Del,
                content_pos: None,
            }),
        ]);
    }
}
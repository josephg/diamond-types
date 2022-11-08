use rle::{HasLength, MergableSpan, SplitableSpan, SplitableSpanCtx};
use crate::{CRDTKind, ListOperationCtx, Op, OpContents};

impl HasLength for OpContents {
    fn len(&self) -> usize {
        match self {
            OpContents::Text(metrics) => metrics.len(),
            _ => 1,
        }
    }
}

impl MergableSpan for OpContents {
    fn can_append(&self, other: &Self) -> bool {
        match (self, other) {
            (OpContents::Text(a), OpContents::Text(b)) => a.can_append(b),
            _ => false,
        }
    }

    fn append(&mut self, other: Self) {
        match (self, other) {
            (OpContents::Text(a), OpContents::Text(b)) => a.append(b),
            _ => panic!("Cannot append"),
        }
    }
}

impl SplitableSpanCtx for OpContents {
    type Ctx = ListOperationCtx;

    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        match self {
            OpContents::Text(metrics) => {
                let remainder = metrics.truncate_ctx(at, ctx);
                OpContents::Text(remainder)
            }
            _ => {
                panic!("Cannot truncate op");
            }
        }
    }
}

impl OpContents {
    pub fn kind(&self) -> CRDTKind {
        match self {
            OpContents::RegisterSet(_) => CRDTKind::Register,
            OpContents::MapSet(_, _) | OpContents::MapDelete(_) => CRDTKind::Map,
            OpContents::Collection(_) => CRDTKind::Collection,
            OpContents::Text(_) => CRDTKind::Text,
        }
    }
}

impl HasLength for Op {
    fn len(&self) -> usize { self.contents.len() }
}

impl MergableSpan for Op {
    fn can_append(&self, other: &Self) -> bool {
        self.target_id == other.target_id && self.contents.can_append(&other.contents)
    }

    fn append(&mut self, other: Self) {
        self.contents.append(other.contents)
    }
}

impl SplitableSpanCtx for Op {
    type Ctx = ListOperationCtx;

    fn truncate_ctx(&mut self, at: usize, ctx: &Self::Ctx) -> Self {
        let remainder = self.contents.truncate_ctx(at, ctx);
        Self {
            target_id: self.target_id,
            contents: remainder
        }
    }
}
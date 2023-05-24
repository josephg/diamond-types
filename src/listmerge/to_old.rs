use rle::{HasLength, MergableSpan, merge_items, MergeableIterator, SplitableSpan, SplitableSpanHelpers};
use crate::{DTRange, SmartString, LV, Frontier};
use crate::causalgraph::agent_assignment::remote_ids::RemoteVersionSpan;
use crate::list::ListOpLog;
use crate::list::operation::ListOpKind;
use crate::listmerge::M2Tracker;
use crate::rev_range::RangeRev;
use crate::unicount::{chars_to_bytes, split_at_char};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OldCRDTOp {
    Ins {
        id: DTRange,
        // id: DTRange,
        origin_left: LV,
        origin_right: LV,
        content: SmartString,
        // content_pos: DTRange,
    },

    Del {
        start_v: LV,
        target: RangeRev
    },
}

impl SplitableSpanHelpers for OldCRDTOp {
    fn truncate_h(&mut self, at: usize) -> Self {
        debug_assert!(at > 0 && at < self.len());

        match self {
            OldCRDTOp::Ins { id, origin_right, content, .. } => {
                let byte_split_pos = chars_to_bytes(content, at);
                let rem_str: SmartString = content[byte_split_pos..].into();
                content.truncate(at);

                Self::Ins {
                    id: id.truncate(at),
                    origin_left: id.start + at - 1,
                    origin_right: *origin_right,
                    content: rem_str,
                }
            }

            OldCRDTOp::Del { start_v: start_time, target } => {
                let t = target.truncate_tagged_span(ListOpKind::Del, at);

                Self::Del {
                    start_v: *start_time + at,
                    target: t,
                }
            }
        }
    }
}

impl MergableSpan for OldCRDTOp {
    fn can_append(&self, other: &Self) -> bool {
        use OldCRDTOp::*;

        match (self, other) {
            (Ins { id: id1, origin_right: origin_right1, .. }, Ins { id: id2, origin_left: origin_left2, origin_right: origin_right2, .. }) => {
                id1.can_append(id2)
                    && *origin_left2 == id2.start - 1
                    && *origin_right1 == *origin_right2
            },
            (Del { start_v: v1, target: target1 }, Del { start_v: v2, target: target2 }) => {
                *v1 + target1.len() == *v2
                    && RangeRev::can_append_ops(ListOpKind::Del, target1, target2)
            },
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        use OldCRDTOp::*;

        match (self, other) {
            (Ins { id: id1, content: content1, .. }, Ins { id: id2, content: content2, .. }) => {
                id1.append(id2);
                content1.push_str(&content2);
            }
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.append_ops(ListOpKind::Del, target2);
            },
            _ => panic!("Append not supported")
        }
    }
}

impl HasLength for OldCRDTOp {
    fn len(&self) -> usize {
        match self {
            OldCRDTOp::Ins { id, .. } => id.len(),
            OldCRDTOp::Del { target, .. } => target.len(),
        }
    }
}

impl OldCRDTOp {
    pub fn lv_span(&self) -> DTRange {
        match self {
            OldCRDTOp::Ins { id, .. } => {
                *id
            },
            OldCRDTOp::Del { start_v: start_time, target } => {
                (*start_time .. *start_time + target.len()).into()
            },
        }
    }

    pub fn remote_span<'a>(&self, oplog: &'a ListOpLog) -> RemoteVersionSpan<'a> {
        oplog.cg.agent_assignment.local_to_remote_version_span(self.lv_span())
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OldCRDTOpInternal {
    Ins {
        id: DTRange,
        // id: DTRange,
        origin_left: LV,
        origin_right: LV,
        content_pos: DTRange,
        // content_pos: DTRange,
    },

    Del {
        start_v: LV,
        target: RangeRev
    },
}

impl MergableSpan for OldCRDTOpInternal {
    fn can_append(&self, other: &Self) -> bool {
        use OldCRDTOpInternal::*;

        match (self, other) {
            (Ins { id: id1, origin_right: origin_right1, content_pos: cp1, .. }, Ins { id: id2, origin_left: origin_left2, origin_right: origin_right2, content_pos: cp2, .. }) => {
                id1.can_append(id2)
                    && *origin_left2 == id2.start - 1
                    && *origin_right1 == *origin_right2
                    && cp1.end == cp2.start
            },
            (Del { start_v: v1, target: target1 }, Del { start_v: v2, target: target2 }) => {
                *v1 + target1.len() == *v2
                    && RangeRev::can_append_ops(ListOpKind::Del, target1, target2)
            },
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        use OldCRDTOpInternal::*;

        match (self, other) {
            (Ins { id: id1, content_pos: cp1, .. }, Ins { id: id2, content_pos: cp2, .. }) => {
                id1.append(id2);
                cp1.end = cp2.end;
            }
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.append_ops(ListOpKind::Del, target2);
            },
            _ => panic!("Append not supported")
        }
    }
}

impl ListOpLog {
    #[cfg(feature = "ops_to_old")]
    pub fn dbg_items(&self) -> Vec<OldCRDTOp> {
        self.cg.version.debug_check_sorted();
        let mut tracker = M2Tracker::new();
        tracker.walk(&self.cg.graph, &self.cg.agent_assignment, &self.operation_ctx, &self.operations,
                     Frontier::root(), &[(0..self.len()).into()], None);
        tracker.dbg_ops.into_iter().merge_spans().map(|op_i| {
            match op_i {
                OldCRDTOpInternal::Ins { id, origin_left, origin_right, content_pos } => {
                    OldCRDTOp::Ins {
                        id,
                        origin_left,
                        origin_right,
                        content: self.operation_ctx.get_str(ListOpKind::Ins, content_pos).into()
                    }
                }
                OldCRDTOpInternal::Del { start_v: start_time, target } => {
                    OldCRDTOp::Del { start_v: start_time, target }
                }
            }
        }).collect()
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use crate::listmerge::to_old::{OldCRDTOp, OldCRDTOpInternal};
    use crate::rev_range::RangeRev;

    #[test]
    fn splitable_mergable() {
        test_splitable_methods_valid(OldCRDTOp::Ins {
            id: (10..20).into(),
            origin_left: 100,
            origin_right: 200,
            content: "0123456789".into(),
        });

        test_splitable_methods_valid(OldCRDTOp::Del {
            start_v: 1000,
            target: RangeRev { span: (10..20).into(), fwd: true },
        });
        test_splitable_methods_valid(OldCRDTOp::Del {
            start_v: 1000,
            target: RangeRev { span: (10..20).into(), fwd: false },
        });
    }

    // I can't use test_splitable on OldCRDTOpInternal because I haven't implemented truncate.
}
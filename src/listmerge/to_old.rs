use rle::{HasLength, MergableSpan};
use crate::{DTRange, SmartString, LV, Frontier};
use crate::causalgraph::agent_assignment::remote_ids::RemoteVersionSpan;
use crate::list::ListOpLog;
use crate::list::operation::ListOpKind;
use crate::listmerge::M2Tracker;
use crate::rev_range::RangeRev;

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
        start_time: LV,
        target: RangeRev
    },
}

impl MergableSpan for OldCRDTOp {
    fn can_append(&self, other: &Self) -> bool {
        use OldCRDTOp::*;

        match (self, other) {
            (Ins { id: _id1, .. }, Ins { id: _id2, .. }) => {
                // Could implement this I guess?
                false
            },
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.can_append(target2)
            },
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        use OldCRDTOp::*;

        match (self, other) {
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.append(target2);
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
    pub fn time_span(&self) -> DTRange {
        match self {
            OldCRDTOp::Ins { id, .. } => {
                *id
            },
            OldCRDTOp::Del { start_time, target } => {
                (*start_time .. *start_time + target.len()).into()
            },
        }
    }

    pub fn remote_span<'a>(&self, oplog: &'a ListOpLog) -> RemoteVersionSpan<'a> {
        oplog.cg.agent_assignment.local_to_remote_version_span(self.time_span())
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
        start_time: LV,
        target: RangeRev
    },
}

impl MergableSpan for OldCRDTOpInternal {
    fn can_append(&self, other: &Self) -> bool {
        use OldCRDTOpInternal::*;

        match (self, other) {
            (Ins { id: _id1, .. }, Ins { id: _id2, .. }) => {
                // Could implement this I guess?
                false
            },
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.can_append(target2)
            },
            _ => false
        }
    }

    fn append(&mut self, other: Self) {
        use OldCRDTOpInternal::*;

        match (self, other) {
            (Del { target: target1, .. }, Del { target: target2, .. }) => {
                target1.append(target2);
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
        tracker.dbg_ops.into_iter().map(|op_i| {
            match op_i {
                OldCRDTOpInternal::Ins { id, origin_left, origin_right, content_pos } => {
                    OldCRDTOp::Ins {
                        id,
                        origin_left,
                        origin_right,
                        content: self.operation_ctx.get_str(ListOpKind::Ins, content_pos).into()
                    }
                }
                OldCRDTOpInternal::Del { start_time, target } => {
                    OldCRDTOp::Del { start_time, target }
                }
            }
        }).collect()
    }
}
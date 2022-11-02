use rle::{HasLength, MergableSpan};
use crate::{DTRange, SmartString, LV};
use crate::causalgraph::remote_ids::RemoteVersionSpan;
use crate::list::ListOpLog;
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
            (Ins { id: id1, .. }, Ins { id: id2, .. }) => {
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
        oplog.cg.local_to_remote_version_span(self.time_span())
    }
}
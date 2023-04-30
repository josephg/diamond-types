/// TODO:
///
/// This export script works to export data sets to something cross-compatible with other CRDTs.
///
/// But if we want *identical* DT documents, this isn't valid for 2 reasons:
///
/// 1. The exported data is missing user agents. (Or should be missing user agents)
/// 2. The exported data is missing `fwd: bool` for operations.
///
/// Write a second export script which outputs the data to some dt-json style format (making this a
/// non-issue). Or just add these fields in and demand people ignore them.

use std::collections::HashMap;
use serde::Serialize;
use smallvec::SmallVec;
use diamond_types::list::ListOpLog;
use diamond_types::list::operation::{ListOpKind, TextOperation};
use smartstring::alias::{String as SmartString};
use diamond_types::HasLength;

// Note this discards the fwd/backwards direction of the changes. This shouldn't matter in
// practice given the whole operation is unitary.
#[derive(Clone, Debug, Serialize)]
pub struct SimpleTextOp(usize, usize, SmartString); // pos, del_len, ins_content.

impl From<TextOperation> for SimpleTextOp {
    fn from(op: TextOperation) -> Self {
        match op.kind {
            ListOpKind::Ins => {
                if !op.loc.fwd {
                    // If inserts are reversed, we should emit a series of operations for each
                    // (reversed) keystroke.
                    todo!("Not reversing op");
                }
                SimpleTextOp(op.start(), 0, op.content.unwrap())
            },
            ListOpKind::Del => SimpleTextOp(op.start(), op.len(), SmartString::new()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportedEditHistory {
    start_content: SmartString,
    end_content: String,

    txns: Vec<ExportEntry>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportEntry {
    id: usize,
    parents: SmallVec<[usize; 2]>,
    num_children: usize, // TODO: Consider taking this out.
    agent: String, // TODO: Take this out.
    // op: TextOperation,
    ops: SmallVec<[SimpleTextOp; 2]>,
}

pub fn export_to_json(oplog: &ListOpLog) -> ExportedEditHistory {
    let mut txns = vec![];

    // TODO: A hashmap is overkill here. A vec + binary search would be fine. Eh.
    // Each chunk of operations has an ID so other ops can refer to it.
    let mut idx_for_v = HashMap::new();

    for entry in oplog.iter_full_2() {
        let agent_name = oplog.cg.agent_assignment.get_agent_name(entry.agent_id);

        let id = txns.len();

        txns.push(ExportEntry {
            id,
            parents: entry.parents.iter().map(|v| *idx_for_v.get(v).unwrap()).collect(),
            num_children: 0,
            agent: agent_name.into(),
            ops: entry.ops.into_iter().map(|op| op.into()).collect(),
        });

        for p in entry.parents.iter() {
            let parent_idx = *idx_for_v.get(p).unwrap();
            txns[parent_idx].num_children += 1;
        }

        let old_entry = idx_for_v.insert(entry.span.last(), id);
        assert!(old_entry.is_none());
    }

    // for r in result {
    //     // println!("{:?}", r);
    //     println!("{:?}", serde_json::to_string(&r).unwrap());
    // }

    let end_content = oplog.checkout_tip().into_inner().to_string();
    ExportedEditHistory {
        start_content: Default::default(),
        end_content,
        txns,
    }
}
// pub fn export_to_json(oplog: &ListOpLog) -> Vec<ExportEntry> {
//     let mut result = vec![];
//
//     // TODO: A hashmap is overkill here. A vec + binary search would be fine. Eh.
//     // Each chunk of operations has an ID so other ops can refer to it.
//     let mut id_for_v = HashMap::new();
//     let mut next_id = 0usize;
//
//     let simple_graph = oplog.cg.make_simple_graph();
//
//     for (entry, agent_span, op) in oplog.iter_full(&simple_graph) {
//         let agent_name = oplog.cg.agent_assignment.get_agent_name(agent_span.agent);
//
//         let id = next_id;
//         next_id += 1;
//
//         result.push(ExportEntry {
//             id,
//             parents: entry.parents.iter().map(|v| *id_for_v.get(v).unwrap()).collect(),
//             agent: agent_name.into(),
//             op: op.into(),
//         });
//
//         // println!("agent '{agent_name}' entry {:?} op {:?}", entry, op);
//
//         for &p in entry.parents.iter() {
//             assert!(id_for_v.contains_key(&p));
//         }
//
//         let old_entry = id_for_v.insert(entry.span.last(), id);
//         assert!(old_entry.is_none());
//     }
//
//     // for r in result {
//     //     // println!("{:?}", r);
//     //     println!("{:?}", serde_json::to_string(&r).unwrap());
//     // }
//
//     result
// }
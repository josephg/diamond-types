use std::cmp::Ordering;
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
use smallvec::{SmallVec, smallvec};
use diamond_types::list::ListOpLog;
use diamond_types::list::operation::{ListOpKind, TextOperation};
use smartstring::alias::{String as SmartString};
use diamond_types::{DTRange, HasLength};

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
pub struct TraceExportData {
    end_content: String,
    num_agents: usize,

    txns: Vec<TraceExportTxn>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceExportTxn {
    parents: SmallVec<[usize; 2]>,
    num_children: usize, // TODO: Consider taking this out.
    agent: usize,
    // op: TextOperation,
    patches: SmallVec<[SimpleTextOp; 2]>,
}

pub fn export_trace_to_json(oplog: &ListOpLog, force: bool) -> TraceExportData {
    let mut txns = vec![];

    // TODO: A hashmap is overkill here. A vec + binary search would be fine. Eh.
    // Each chunk of operations has an ID so other ops can refer to it.
    let mut idx_for_v = HashMap::new();
    let mut last_version_from_agent = HashMap::new();
    let mut agent_map = vec![None; oplog.cg.num_agents()];
    let mut num_agents: usize = 0;

    for (i, entry) in oplog.as_chunked_operation_vec().into_iter().enumerate() {
        if let Some(last_v) = last_version_from_agent.get(&entry.agent_span.agent) {
            if !force {
                assert_eq!(Some(Ordering::Less), oplog.cg.graph.version_cmp(*last_v, entry.span.start), "Operations are not fully ordered from each agent");
            }
        }
        last_version_from_agent.insert(entry.agent_span.agent, entry.span.last());

        if !force {
            assert_eq!(i == 0, entry.parents.is_empty(), "Cannot export trace: ROOT entry has multiple children");
            // I'm not sure how this can happen, but its cheap to check just in case.
            assert_eq!(entry.ops.is_empty(), false, "Transaction cannot have empty op list");
        }

        let oplog_agent = entry.agent_span.agent as usize;
        let agent = if let Some(a) = agent_map[oplog_agent] { a }
        else {
            let a = num_agents;
            agent_map[oplog_agent] = Some(a);
            num_agents += 1;
            a
        };

        txns.push(TraceExportTxn {
            parents: entry.parents.iter().map(|v| *idx_for_v.get(v).unwrap()).collect(),
            num_children: 0,
            agent,
            patches: entry.ops.into_iter().map(|op| op.into()).collect(),
        });

        for p in entry.parents.iter() {
            let parent_idx = *idx_for_v.get(p).unwrap();
            txns[parent_idx].num_children += 1;
        }

        let old_entry = idx_for_v.insert(entry.span.last(), i);
        assert!(old_entry.is_none());
    }

    if let Some((_, rest)) = txns.split_last_mut() {
        if rest.iter().any(|r| r.num_children == 0) {
            // The transaction list contains multiple items with no children. These items need to
            // be merged together in the final result. We will produce a "dummy" txn which merges
            // all previously un-merged children.
            let mut txn = TraceExportTxn {
                parents: smallvec![],
                num_children: 0,
                agent: 0,
                patches: smallvec![],
            };

            for (i, r) in rest.iter_mut().enumerate() {
                if r.num_children == 0 {
                    r.num_children += 1;
                    txn.parents.push(i);
                }
            }

            txns.push(txn);
        }
    }


    let end_content = oplog.checkout_tip().into_inner().to_string();
    TraceExportData {
        end_content,
        num_agents,
        txns,
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DTExportTxn {
    span: DTRange,
    parents: SmallVec<[usize; 2]>,
    agent: SmartString,
    // op: TextOperation,
    ops: SmallVec<[SimpleTextOp; 2]>,
}

pub fn export_full_to_json(oplog: &ListOpLog) -> Vec<DTExportTxn> {
    let mut txns = vec![];

    for entry in oplog.as_chunked_operation_vec().into_iter() {
        txns.push(DTExportTxn {
            span: entry.span,
            parents: entry.parents.0.clone(),
            agent: oplog.get_agent_name(entry.agent_span.agent).into(),
            ops: entry.ops.into_iter().map(|op| op.into()).collect(),
        });
    }

    txns
}


#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSimpleExportData {
    start_content: SmartString,
    end_content: String,
    txns: Vec<TraceSimpleExportTxn>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSimpleExportTxn {
    time: SmartString,
    patches: SmallVec<[SimpleTextOp; 2]>,
}

pub fn export_transformed(oplog: &ListOpLog, timestamp: String) -> TraceSimpleExportData {
    let mut txns = vec![];

    let timestamp: SmartString = timestamp.into();
    for (_, op) in oplog.iter_xf_operations() {
        if let Some(op) = op {
            txns.push(TraceSimpleExportTxn {
                time: timestamp.clone(),
                patches: smallvec![op.into()],
            });
        }
    }

    let end_content = oplog.checkout_tip().into_inner().to_string();
    TraceSimpleExportData {
        start_content: Default::default(),
        end_content,
        txns,
    }
}
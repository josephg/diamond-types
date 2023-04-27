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
#[serde(tag = "type")]
pub enum SimpleTextOp {
    Ins { pos: usize, content: SmartString },
    Del { start: usize, len: usize },
}

impl From<TextOperation> for SimpleTextOp {
    fn from(op: TextOperation) -> Self {
        match op.kind {
            ListOpKind::Ins => SimpleTextOp::Ins {
                pos: op.start(),
                content: op.content.unwrap()
            },
            ListOpKind::Del => SimpleTextOp::Del { start: op.start(), len: op.len() },
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ExportEntry {
    id: usize,
    parents: SmallVec<[usize; 2]>,
    agent: String,
    // op: TextOperation,
    op: SimpleTextOp,
}

pub fn export_to_json(oplog: &ListOpLog) -> Vec<ExportEntry> {
    // TODO: A hashmap is overkill here. A vec + binary search would be fine. Eh.
    let mut names = HashMap::new();
    let mut next_name = 0usize;

    let simple_graph = oplog.cg.make_simple_graph();

    let mut result = vec![];

    for (entry, agent_span, op) in oplog.iter_full(&simple_graph) {
        let agent_name = oplog.cg.agent_assignment.get_agent_name(agent_span.agent);

        let id = next_name;
        next_name += 1;

        result.push(ExportEntry {
            id,
            parents: entry.parents.iter().map(|v| *names.get(v).unwrap()).collect(),
            agent: agent_name.into(),
            op: op.into(),
        });

        // println!("agent '{agent_name}' entry {:?} op {:?}", entry, op);

        for &p in entry.parents.iter() {
            assert!(names.contains_key(&p));
        }

        let old_entry = names.insert(entry.span.last(), id);
        assert!(old_entry.is_none());
    }

    // for r in result {
    //     // println!("{:?}", r);
    //     println!("{:?}", serde_json::to_string(&r).unwrap());
    // }

    result
}
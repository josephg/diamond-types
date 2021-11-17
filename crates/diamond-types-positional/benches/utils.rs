use diamond_types_positional::list::*;
use crdt_testdata::{TestTxn, TestPatch};
use diamond_types_positional::list::operation::{InsDelTag, Operation};

pub fn apply_edits_local(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut positional: Vec<Operation> = Vec::with_capacity(3);
    // let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            positional.clear();
            // content.clear();

            if *del_span > 0 {
                positional.push(Operation {
                    pos: *pos,
                    len: *del_span,
                    rev: false,
                    content_known: false,
                    tag: InsDelTag::Del,
                    content: Default::default()
                });
            }

            if !ins_content.is_empty() {
                positional.push(Operation {
                    pos: *pos,
                    len: ins_content.chars().count(),
                    rev: false,
                    content_known: true,
                    tag: InsDelTag::Ins,
                    content: ins_content.into(),
                });
                // content.push_str(ins_content.as_str());
            }

            doc.apply_local_operation(id, positional.as_slice());
        }
    }
}


pub fn apply_edits_push_merge(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut last_parent = doc.branch.frontier[0];

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            // content.clear();

            if *del_span > 0 {
                last_parent = doc.ops.push_delete(id, &[last_parent], *pos, *del_span);
            }

            if !ins_content.is_empty() {
                last_parent = doc.ops.push_insert(id, &[last_parent], *pos, ins_content);
            }
        }
    }

    doc.branch.merge(&doc.ops, &[last_parent]);
}
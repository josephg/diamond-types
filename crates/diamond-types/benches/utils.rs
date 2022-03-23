use diamond_types::list::*;
use crdt_testdata::{TestTxn, TestPatch};
use diamond_types::list::operation::Operation;

pub fn apply_edits_local(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut positional: Vec<Operation> = Vec::with_capacity(3);
    // let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            positional.clear();
            // content.clear();

            if *del_span > 0 {
                positional.push(Operation::new_delete(*pos, *del_span));
            }

            if !ins_content.is_empty() {
                positional.push(Operation::new_insert(*pos, ins_content));
                // content.push_str(ins_content.as_str());
            }

            doc.apply_local_operation(id, positional.as_slice());
        }
    }
}


pub fn apply_edits_push_merge(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut last_parent = doc.branch.local_version()[0];

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            // content.clear();

            if *del_span > 0 {
                last_parent = doc.oplog.add_delete_at(id, &[last_parent], *pos, *del_span);
            }

            if !ins_content.is_empty() {
                last_parent = doc.oplog.add_insert_at(id, &[last_parent], *pos, ins_content);
            }
        }
    }

    doc.branch.merge(&doc.oplog, &[last_parent]);
}
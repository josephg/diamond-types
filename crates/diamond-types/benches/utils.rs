use diamond_types::list::*;
use crdt_testdata::{TestTxn, TestPatch};
use diamond_types::list::operation::Operation;
use rle::AppendRle;

pub fn apply_edits_local(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut positional: Vec<Operation> = Vec::with_capacity(3);
    // let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            positional.clear();
            // content.clear();

            if *del_span > 0 {
                positional.push(Operation::new_delete(*pos .. *pos + *del_span));
            }

            if !ins_content.is_empty() {
                positional.push(Operation::new_insert(*pos, ins_content));
                // content.push_str(ins_content.as_str());
            }

            doc.apply_local_operations(id, positional.as_slice());
        }
    }
}

pub fn apply_edits_push_merge(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            let pos = *pos;
            let del_span = *del_span;

            if del_span > 0 {
                doc.oplog.add_delete_without_content(id, pos..pos + del_span);
            }

            if !ins_content.is_empty() {
                doc.oplog.add_insert(id, pos, ins_content);
            }
        }
    }

    doc.branch.merge(&doc.oplog, &doc.oplog.local_version());
}


pub fn apply_grouped(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut ops: Vec<Operation> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            if *del_span > 0 {
                ops.push(Operation::new_delete(*pos .. *pos + *del_span));
            }

            if !ins_content.is_empty() {
                ops.push(Operation::new_insert(*pos, ins_content));
            }
        }
    }

    doc.apply_local_operations(id, &ops);
    // doc.branch.merge(&doc.oplog, &doc.oplog.local_version());
}

pub fn as_grouped_ops_rle(txns: &Vec<TestTxn>) -> Vec<Operation> {
    let mut ops: Vec<Operation> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {

            if *del_span > 0 {
                ops.push_rle(Operation::new_delete(*pos .. *pos + *del_span));
            }

            if !ins_content.is_empty() {
                ops.push_rle(Operation::new_insert(*pos, ins_content));
            }
        }
    }

    ops
}

pub fn apply_ops(doc: &mut ListCRDT, ops: &[Operation]) {
    let id = doc.get_or_create_agent_id("jeremy");
    doc.apply_local_operations(id, &ops);
}

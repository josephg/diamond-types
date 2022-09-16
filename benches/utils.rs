use diamond_types::list::*;
use crdt_testdata::{TestTxn, TestPatch};
use diamond_types::list::operation::TextOperation;
use rle::AppendRle;

pub fn apply_edits_direct(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            if *del_span > 0 {
                // doc.delete(id, *pos .. *pos + *del_span);
                doc.delete_without_content(id, *pos .. *pos + *del_span);
            }

            if !ins_content.is_empty() {
                doc.insert(id, *pos, ins_content);
            }
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

    doc.branch.merge(&doc.oplog, &doc.oplog.local_version_ref());
}


pub fn apply_grouped(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut ops: Vec<TextOperation> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            if *del_span > 0 {
                ops.push(TextOperation::new_delete(*pos .. *pos + *del_span));
            }

            if !ins_content.is_empty() {
                ops.push(TextOperation::new_insert(*pos, ins_content));
            }
        }
    }

    doc.apply_local_operations(id, &ops);
    // doc.branch.merge(&doc.oplog, &doc.oplog.local_version());
}

pub fn as_grouped_ops_rle(txns: &Vec<TestTxn>) -> Vec<TextOperation> {
    let mut ops: Vec<TextOperation> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {

            if *del_span > 0 {
                ops.push_rle(TextOperation::new_delete(*pos .. *pos + *del_span));
            }

            if !ins_content.is_empty() {
                ops.push_rle(TextOperation::new_insert(*pos, ins_content));
            }
        }
    }

    ops
}

pub fn apply_ops(doc: &mut ListCRDT, ops: &[TextOperation]) {
    let id = doc.get_or_create_agent_id("jeremy");
    doc.apply_local_operations(id, &ops);
}

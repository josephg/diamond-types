use diamond_types::list::{ListCRDT, TraversalComponent};
use crdt_testdata::{TestTxn, TestPatch};

use TraversalComponent::*;

pub fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut traversal: Vec<TraversalComponent> = Vec::with_capacity(3);
    let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            traversal.clear();
            content.clear();
            traversal.push(Retain(*pos as u32));

            if *del_span > 0 {
                traversal.push(Del(*del_span as u32));
            }

            if !ins_content.is_empty() {
                traversal.push(Ins {
                    len: ins_content.chars().count() as u32,
                    content_known: true
                });
                content.push_str(ins_content.as_str());
            }

            doc.apply_local_txn(id, traversal.as_slice(), content.as_str());
        }
    }
}
use diamond_types::list::ListCRDT;
use crdt_testdata::{TestTxn, TestPatch};
use diamond_types::LocalOp;
use smartstring::alias::{String as SmartString};

pub fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut local_ops: Vec<LocalOp> = Vec::new();

    for (_i, txn) in txns.iter().enumerate() {
        local_ops.clear();
        local_ops.extend(txn.patches.iter().map(|TestPatch(pos, del_span, ins_content)| {
            assert!(*pos <= doc.len());
            LocalOp {
                pos: *pos,
                del_span: *del_span,
                ins_content: SmartString::from(ins_content.as_str())
            }
        }));

        doc.apply_local_txn(id, local_ops.as_slice());
    }
}
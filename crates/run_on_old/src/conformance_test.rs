use std::fs::File;
use std::io::BufReader;
use serde::Deserialize;
use smallvec::SmallVec;
use diamond_types::DTRange;
use diamond_types::list::operation::{ListOpKind, TextOperation};
use smartstring::alias::{String as SmartString};
use diamond_types::list::ListOpLog;
use crate::get_txns_from_oplog;

#[derive(Clone, Debug, Deserialize)]
pub struct SimpleTextOp(usize, usize, SmartString); // pos, del_len, ins_content.

impl Into<TextOperation> for &SimpleTextOp {
    fn into(self) -> TextOperation {
        let SimpleTextOp(pos, del_len, ins_content) = self;
        assert_ne!((*del_len == 0), ins_content.is_empty());
        if *del_len > 0 {
            TextOperation {
                kind: ListOpKind::Del,
                loc: (*pos..*pos + *del_len).into(),
                content: None,
            }
        } else {
            let content_len = ins_content.chars().count();
            TextOperation {
                kind: ListOpKind::Ins,
                loc: (*pos..*pos + content_len).into(),
                content: Some(ins_content.clone()),
            }
        }
    }
}


#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DTExportTxn {
    /// The LV span of the txn. Note the agent seq span is not exported.
    span: DTRange,
    parents: SmallVec<[usize; 2]>,
    agent: SmartString,
    seq_start: usize,
    // op: TextOperation,
    ops: SmallVec<[SimpleTextOp; 2]>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DTExport {
    txns: Vec<DTExportTxn>,
    end_content: String,
}

fn export_to_oplog(data: &DTExport) -> ListOpLog {
    // First make an oplog from the exported data.
    let mut oplog = ListOpLog::new();
    for txn in &data.txns {
        let ops: Vec<TextOperation> = txn.ops.iter().map(|op| op.into()).collect();
        let agent = oplog.get_or_create_agent_id(txn.agent.as_str());
        // oplog.a
        oplog.add_operations_at(agent, txn.parents.as_slice(), &ops);
    }

    debug_assert_eq!(oplog.checkout_tip().content(), data.end_content);

    oplog
}

#[test]
fn conformance_tests() {
    // Runs in crates/run_on_old.
    // println!("working dir {:?}", std::env::current_dir().unwrap());

    // Generated with:
    // dt gen-conformance -n1000 -s10 --seed 30 -o test_data/conformance.json
    let name = "../../test_data/conformance.json";
    let reader = BufReader::new(File::open(name).unwrap());
    // let contents = std::fs::read(name).unwrap();
    let data: Vec<DTExport> = serde_json::from_reader(reader).unwrap();
    println!("Loaded conformance testing data from {} ({} entries)", name, data.len());

    for (i, d) in data.iter().enumerate() {
        // println!("i {i}");
        let oplog = export_to_oplog(d);
        let old_txns = get_txns_from_oplog(&oplog);
        // dbg!(&old_txns);

        let mut old_oplog = diamond_types_old::list::ListCRDT::new();
        for txn in &old_txns {
            old_oplog.apply_remote_txn(txn);
        }
        let result = old_oplog.to_string();

        // old_oplog.debug_print_ids();

        // After applying the edits, the results should match!
        assert_eq!(result, d.end_content);
    }

    println!("Conformance tests pass!");
}

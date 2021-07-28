use crdt_testdata::{load_testing_data, TestPatch, TestTxn, TestData};
use smartstring::alias::{String as SmartString};
use diamond_types::*;
use diamond_types::list::*;

fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
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

fn load_into_doc(test_data: TestData) -> ListCRDT {

    let mut doc = ListCRDT::new();
    apply_edits(&mut doc, &test_data.txns);
    // apply_edits_fast(&mut state, &patches);
    // println!("len {}", state.len());
    assert_eq!(doc.len(), test_data.end_content.len());

    doc.check(true);
    if doc.has_content() {
        assert_eq!(doc.to_string(), test_data.end_content.as_str());
    }

    doc
}

#[test]
fn txn_real_world_data() {
    let test_data = load_testing_data("benchmark_data/sveltecomponent.json.gz");

    assert_eq!(test_data.start_content.len(), 0);
    println!("final length: {}, txns {} patches {}", test_data.end_content.len(), test_data.txns.len(),
             test_data.txns.iter().fold(0, |x, i| x + i.patches.len()));

    let start_alloc = get_thread_memory_usage();
    load_into_doc(test_data);

    println!("alloc {}", get_thread_memory_usage() - start_alloc);
    println!("alloc count {}", get_thread_num_allocations());
}

#[test]
fn replicate() {
    let test_data = load_testing_data("benchmark_data/sveltecomponent.json.gz");
    let local_doc = load_into_doc(test_data);

    let mut remote_doc = ListCRDT::new();
    local_doc.replicate_into(&mut remote_doc);
    assert_eq!(local_doc, remote_doc);
}
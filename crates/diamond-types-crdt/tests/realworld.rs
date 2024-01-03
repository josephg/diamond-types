use crdt_testdata::{load_testing_data, TestPatch, TestTxn, TestData};
use diamond_types_crdt::list::*;
use diamond_types_crdt::list::positional::PositionalOpRef;

pub fn apply_edits(doc: &mut ListCRDT, txns: &Vec<TestTxn>) {
    let id = doc.get_or_create_agent_id("jeremy");

    let mut positional: Vec<PositionalComponent> = Vec::with_capacity(3);
    let mut content = String::new();

    for (_i, txn) in txns.iter().enumerate() {
        for TestPatch(pos, del_span, ins_content) in &txn.patches {
            positional.clear();
            content.clear();

            if *del_span > 0 {
                positional.push(PositionalComponent {
                    pos: *pos,
                    len: *del_span,
                    content_known: false,
                    tag: InsDelTag::Del
                });
            }

            if !ins_content.is_empty() {
                positional.push(PositionalComponent {
                    pos: *pos,
                    len: ins_content.chars().count(),
                    content_known: true,
                    tag: InsDelTag::Ins
                });
                content.push_str(ins_content.as_str());
            }

            doc.apply_local_txn(id, PositionalOpRef {
                components: &positional,
                content: content.as_str(),
            });
        }
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
#[ignore]
fn txn_real_world_data() {
    let test_data = load_testing_data("../../benchmark_data/rustcode.json.gz");
    // let test_data = load_testing_data("../../benchmark_data/sveltecomponent.json.gz");
    load_into_doc(test_data);
}

#[test]
fn replicate() {
    let test_data = load_testing_data("../../benchmark_data/sveltecomponent.json.gz");
    let local_doc = load_into_doc(test_data);

    let mut remote_doc = ListCRDT::new();
    local_doc.replicate_into(&mut remote_doc);
    assert_eq!(local_doc, remote_doc);
}

#[ignore]
#[test]
fn doc_to_position_updates() {
    // let test_data = load_testing_data("../../benchmark_data/seph-blog1.json.gz");
    let test_data = load_testing_data("../../benchmark_data/sveltecomponent.json.gz");
    let local_doc = load_into_doc(test_data);
    let patches = local_doc.iter_original_patches().collect::<Vec<_>>();
    dbg!(patches.len());
}
use crdt_testdata::nonlinear::{load_nl_testing_data, NLId};
use diamond_types::list::external_txn::RemoteId;
use diamond_types::list::{InsDelTag, ListCRDT, PositionalComponent};
// use smartstring::alias::String as SmartString;
use crdt_testdata::TestPatch;
use diamond_types::list::positional::PositionalOpRef;
use diamond_types::root_id;

// #[test]
// fn foo() {
//     let d = load_nl_testing_data("/home/seph/src/crdt-benchmarks/xml/out/G1-3.json");
//     dbg!(&d);
// }

// fn convert_id(id: &NLId) -> RemoteId {
//     RemoteId {
//         agent: id.agent.to_string().into(),
//         seq: id.seq,
//     }
// }

#[test]
#[ignore]
fn test_xml_trace_data() {
    let mut doc = ListCRDT::new();
    // let d = load_nl_testing_data("/home/seph/src/crdt-benchmarks/xml/out/G1-3.json");
    // let d = load_nl_testing_data("/home/seph/src/crdt-benchmarks/xml/out/Serie-1.json");
    let d = load_nl_testing_data("/home/seph/src/crdt-benchmarks/xml/out/G1-1.json");

    let mut positional: Vec<PositionalComponent> = Vec::with_capacity(3);
    let mut content = String::new();

    // Sooooo the sequence numbers in the file don't line up with the way I use sequence numbers in
    // DT. In the file they're linear from 1-n. Here they count from 0 and go up by the size of the
    // change.
    let mut seq_map: Vec<Vec<u32>> = vec![];

    let convert_id = |id: &NLId, seq_map: &mut Vec<Vec<u32>>| -> RemoteId {
        RemoteId {
            agent: id.agent.to_string().into(),
            seq: seq_map[id.agent as usize][id.seq as usize - 1]
        }
    };

    for op in d.ops {
        let agent_str = op.id.agent.to_string();
        let agent_id = doc.get_or_create_agent_id(&agent_str);
        let seq = doc.get_next_agent_seq(agent_id);
        // doc.ge

        // dbg!(&op);
        let id = RemoteId {
            agent: agent_str.into(),
            seq
        };
        while seq_map.len() <= op.id.agent as usize {
            seq_map.push(vec![]);
        }
        assert_eq!(seq_map[op.id.agent as usize].len(), op.id.seq as usize - 1);
        let op_len = (op.patch.1 + op.patch.2.chars().count()) as u32;
        seq_map[op.id.agent as usize].push(id.seq + op_len - 1);

        // dbg!(&id);

        let mut parents = op.parents.iter().map(|p| convert_id(p, &mut seq_map)).collect::<Vec<_>>();
        if parents.len() == 0 {
            // The root operation(s).
            parents.push(root_id());
        }
        // dbg!(&parents);

        positional.clear();
        content.clear();

        let TestPatch(pos, del_span, ins_content) = op.patch;
        if del_span > 0 {
            positional.push(PositionalComponent {
                pos: pos as u32,
                len: del_span as u32,
                content_known: false,
                tag: InsDelTag::Del
            });
        }

        if !ins_content.is_empty() {
            positional.push(PositionalComponent {
                pos: pos as u32,
                len: ins_content.chars().count() as u32,
                content_known: true,
                tag: InsDelTag::Ins
            });
            content.push_str(ins_content.as_str());
        }

        doc.apply_remote_patch_at_version(&id, &parents, PositionalOpRef {
            components: &positional,
            content: content.as_str(),
        });
    }

    println!("{}", doc.to_string());
}
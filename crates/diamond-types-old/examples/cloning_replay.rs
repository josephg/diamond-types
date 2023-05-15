//! This file implements a simple routine to replay complex positional editing histories using
//! deep cloning.
//!
//! Build / run with:
//! cargo build -p diamond-types-old --example cloning_replay --features=serde,serde_json --release

#![allow(unused_imports)]

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use criterion::{black_box, Criterion};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;
use diamond_core_old::AgentId;
use diamond_types_old::list::ListCRDT;
use std::fmt::Write;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EditHistory {
    start_content: SmartString,
    end_content: String,

    txns: Vec<HistoryEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SimpleTextOp(usize, usize, SmartString); // pos, del_len, ins_content.

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    id: usize,
    parents: SmallVec<[usize; 2]>,
    num_children: usize,
    agent: String,
    // op: TextOperation,
    ops: SmallVec<[SimpleTextOp; 2]>,
}


fn gen_main() -> Result<(), Box<dyn Error>> {
    let mut doc = ListCRDT::new();

    // let filename = "example_trace.json";
    let filename = "node_nodecc.json";
    // let filename = "git_makefile.json";

    let file = BufReader::new(File::open(filename)?);
    let history: EditHistory = serde_json::from_reader(file)?;
    // dbg!(data);

    assert!(history.start_content.is_empty()); // 'cos I'm not handling this for now.

    // There should be exactly one entry with no parents.
    let num_roots = history.txns.iter().filter(|e| e.parents.is_empty()).count();
    // assert_eq!(num_roots, 1);

    // The last item should be the output.
    let num_final = history.txns.iter().filter(|e| e.num_children == 0).count();
    assert_eq!(num_final, 1);

    let mut doc_at_idx: HashMap<usize, (ListCRDT, AgentId, usize)> = HashMap::new();

    let agent = doc.get_or_create_agent_id("origin");
    doc_at_idx.insert(usize::MAX, (doc, agent, num_roots));

    fn take_doc(doc_at_idx: &mut HashMap<usize, (ListCRDT, AgentId, usize)>, idx: usize, need_agent: bool) -> (ListCRDT, AgentId) {
        let (parent_doc, agent, retains) = doc_at_idx.get_mut(&idx).unwrap();
        if *retains == 1 {
            // We'll just take the document.
            let agent = *agent;
            (doc_at_idx.remove(&idx).unwrap().0, agent)
        } else {
            // Fork it and take the fork.
            let mut doc = parent_doc.clone();

            let agent = if need_agent {
                let mut agent_name = SmartString::new();
                write!(agent_name, "{idx}-{retains}").unwrap();
                doc.get_or_create_agent_id(agent_name.as_str())
            } else { 0 };
            *retains -= 1;
            (doc, agent)
        }
    }

    // let mut next_seq_for_agent: HashMap<SmartString, usize> = Default::default();

    // doc_at_idx.insert(usize::MAX)

    // let mut root = Some(doc);
    for (_i, entry) in history.txns.iter().enumerate() {
        // println!("Iteration {_i} / {:?}", entry);

        // First we need to get the doc we're editing.
        let (&first_p, rest_p) = entry.parents.split_first().unwrap_or((&usize::MAX, &[]));

        let (mut doc, agent) = take_doc(&mut doc_at_idx, first_p, true);

        // If there's any more parents, merge them together.
        for p in rest_p {
            let (doc2, _) = take_doc(&mut doc_at_idx, *p, false);
            doc2.replicate_into(&mut doc);
        }

        // Gross - actor IDs are fixed 16 byte arrays.
        // let actor = ActorId::from()
        // let mut actor_bytes = [0u8; 16];
        // let copied_bytes = actor_bytes.len().min(entry.agent.len());
        // actor_bytes[..copied_bytes].copy_from_slice(&entry.agent.as_bytes()[..copied_bytes]);
        // actor_bytes[12..16].copy_from_slice(&(entry.id as u32).to_be_bytes());
        // let actor = ActorId::from(actor_bytes);
        // doc.set_actor(actor);

        // let a = format!("{_i}");
        // let agent = doc.get_or_create_agent_id(&a);
        // let agent = doc.get_or_create_agent_id(&entry.agent);

        // Ok, now modify the document.
        for op in &entry.ops {
            let pos = op.0;
            let del_len = op.1;
            let ins_content = op.2.as_str();

            if del_len > 0 {
                // Delete.
                doc.local_delete(agent, pos, del_len);
            }
            if !ins_content.is_empty() {
                doc.local_insert(agent, pos, ins_content);
            }
        }

        // And deposit the result back into doc_at_idx.
        if entry.num_children > 0 {
            doc_at_idx.insert(entry.id, (doc, agent, entry.num_children));
        } else {
            println!("done!");

            let result = doc.to_string();
            // println!("result: '{result}'");
            // let saved = doc.save();
            // println!("automerge document saves to {} bytes", saved.len());

            // let out_filename = format!("{filename}.am");
            // std::fs::write(&out_filename, saved).unwrap();
            // println!("Saved to {out_filename}");

            assert_eq!(result, history.end_content);
        }
    }

    Ok(())
}

// fn bench_process(c: &mut Criterion) {
//     let name = "node_nodecc";
//     let filename = format!("{name}.json.am");
//
//     c.bench_function(&format!("process_remote_edits/{name}"), |b| {
//         let bytes = std::fs::read(&filename).unwrap();
//         b.iter(|| {
//             let doc = AutoCommit::load(&bytes).unwrap();
//             let (_, text_id) = doc.get(automerge::ROOT, "text").unwrap().unwrap();
//             let result = doc.text(text_id).unwrap();
//             // black_box(doc);
//             black_box(result);
//         })
//     });
// }

// fn bench_main() {
//     // benches();
//     let mut c = Criterion::default()
//         .configure_from_args();
//
//     bench_process(&mut c);
//     c.final_summary();
// }

fn main() {
    gen_main().unwrap();
    // bench_main();
}
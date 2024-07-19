#[cfg(feature = "memusage")]
use std::collections::HashMap;
use std::hint::black_box;

#[cfg(feature = "bench")]
use criterion::{BenchmarkId, Criterion};
#[cfg(feature = "memusage")]
use serde::Serialize;
use smallvec::{smallvec, SmallVec};

use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionOwned as NewRemoteVersion;
use diamond_types::DTRange;
use diamond_types::list::ListOpLog;
use diamond_types::listmerge::to_old::OldCRDTOp;
use diamond_types_crdt::list::external_txn::{RemoteId as OldRemoteId, RemoteIdSpan as OldRemoteIdSpan, RemoteTxn};
use diamond_types_crdt::root_id;
use rle::{AppendRle, HasLength, SplitableSpan};
#[cfg(feature = "memusage")]
use trace_alloc::measure_memusage;

#[cfg(test)]
mod conformance_test;

fn time_to_remote_id(time: usize, oplog: &ListOpLog) -> OldRemoteId {
    if time == usize::MAX {
        root_id()
    } else {
        new_to_old_remote_id(oplog.cg.agent_assignment.local_to_remote_version(time).into())
    }
}

fn new_to_old_remote_id(new: NewRemoteVersion) -> OldRemoteId {
    OldRemoteId {
        agent: new.0.into(),
        seq: new.1 as _
    }
}

// // NOTE: Not guaranteed to cover incoming span.
// fn agent_to_remote_span(span: AgentSpan, oplog: &ListOpLog) -> OldRemoteIdSpan {
//     // let span = oplog.cg.agent_assignment.agent_span_to_remote(span);
//     OldRemoteIdSpan {
//         id: OldRemoteId {
//             agent: oplog.cg.agent_assignment.get_agent_name(span.agent).into(),
//             seq: span.seq_range.start as _
//         },
//         len: span.seq_range.len() as _
//     }
// }

fn lv_to_remote_span(range: DTRange, oplog: &ListOpLog) -> OldRemoteIdSpan {
    if range.start == usize::MAX { panic!("Cannot convert a root timespan"); }

    // TODO: Feels gross & redundant having both of these methods.
    let span = oplog.cg.agent_assignment.local_to_remote_version_span(range);
    OldRemoteIdSpan {
        id: OldRemoteId {
            agent: span.0.into(),
            seq: span.1.start as _
        },
        len: span.1.len() as _
    }
}

// fn new_to_old_remote_span(new: diamond_types::list::remote_ids::RemoteIdSpan) -> RemoteIdSpan {
//     RemoteIdSpan {
//         // agent: new.agent,
//         // seq: new.seq as _
//         id: RemoteId {
//             agent: new.agent,
//             seq: new.seq_range.start as _
//         },
//         len: new.seq_range.len() as _
//     }
// }


pub fn get_txns_from_file(name: &str) -> Vec<RemoteTxn> {
    let contents = std::fs::read(name).unwrap();
    println!("\n\nLoaded testing data from {} ({} bytes)", name, contents.len());
    let oplog = ListOpLog::load_from(&contents).unwrap();

    get_txns_from_oplog(&oplog)
}

pub fn get_txns_from_oplog(oplog: &ListOpLog) -> Vec<RemoteTxn> {
    let items = oplog.dbg_items();

    // dbg!(&items);
    // for e in oplog.cg.iter() {
    //     println!("e {:?}", e);
    // }

    let mut result = vec![];
    for mut item in items {
        loop {
            // println!();

            let entry = oplog.cg.simple_entry_at(item.lv_span());
            // println!("TEMP {:?} -> {:?}", span, id);

            // assert_eq!(id.len as usize, item.len(), "Split items is unimplemented!");

            let entry_len = entry.len();
            let rem = if entry_len < item.len() {
                Some(item.truncate(entry_len))
            } else {
                assert_eq!(entry_len, item.len());
                None
            };

            // println!("{:?} -> {:?}", item.lv_span(), &entry);

            // println!("Item {:?}", &item);
            // if let Some(r) = rem.as_ref() {
            //     println!("Rem {:?}", &r);
            // }

            let mut ops = smallvec![];
            let ins_content = match item {
                OldCRDTOp::Ins {
                    id, origin_left, origin_right, content
                } => {
                    ops.push_rle(diamond_types_crdt::list::external_txn::RemoteCRDTOp::Ins {
                        origin_left: time_to_remote_id(origin_left, &oplog),
                        origin_right: time_to_remote_id(origin_right, &oplog),
                        len: id.len() as _,
                        content_known: true
                    });
                    content
                }
                OldCRDTOp::Del { mut target, .. } => {
                    // It would be nice to do something nice and RLE-optimized here, but
                    // unfortunately target may be reversed. In that case, its really quite tricky
                    // to get all the items and append them properly. And this code doesn't have to
                    // be that fast. So I'll just iterate through target by hand.
                    while !target.is_empty() {
                        // Carve off the first delete. This will get the deletes in reverse order
                        // if target is in reverse order.
                        let first_item = target.truncate_keeping_right(1);
                        let t_here = lv_to_remote_span(first_item.span, &oplog);

                        ops.push_rle(diamond_types_crdt::list::external_txn::RemoteCRDTOp::Del {
                            id: t_here.id,
                            len: t_here.len // always 1.
                        });
                    }
                    "".into()
                }
            };

            let parents: SmallVec<OldRemoteId, 2> = entry.parents.iter().map(|p| {
                time_to_remote_id(*p, &oplog)
            }).collect();

            // println!("Parents {:?} -> {:?}", entry.parents, &parents);

            result.push(RemoteTxn {
                id: OldRemoteId {
                    agent: oplog.cg.agent_assignment.get_agent_name(entry.span.agent).into(),
                    seq: entry.span.seq_range.start as _
                },
                parents,
                ops,
                ins_content
            });

            if let Some(rem) = rem {
                item = rem;
            } else { break; }
        }
    }

    // dbg!(&result);
    result
}

// const DATASETS: &[&str] = &["automerge-paper", "seph-blog1", "friendsforever", "clownschool", "node_nodecc", "git-makefile", "egwalker"];
// const DATASETS: &[&str] = &["automerge-paperx3", "seph-blog1x3", "node_nodeccx1", "friendsforeverx25", "clownschoolx25", "egwalkerx1", "git-makefilex2"];
const DATASETS: &[&str] = & ["S1", "S2", "S3", "C1", "C2", "A1", "A2"];

#[cfg(feature = "bench")]
fn bench_process(c: &mut Criterion) {
    // let name = "benchmark_data/node_nodecc.dt";
    // let name = "benchmark_data/friendsforever.dt";
    // let name = "benchmark_data/git-makefile.dt";
    // let name = "benchmark_data/data.dt";

    for &name in DATASETS {
        let txns = get_txns_from_file(&format!("../reg-paper/datasets/{}.dt", name));
        // let txns = get_txns_from_file(&format!("benchmark_data/{}.dt", name));

        let mut group = c.benchmark_group("dt-crdt");
        group.bench_function(BenchmarkId::new("process_remote_edits", name), |b| {
            // let old_str = old_oplog.to_string();
            // let new_str = oplog.checkout_tip().content().to_string();
            // assert_eq!(old_str, new_str);
            b.iter(|| {
                let mut old_oplog = diamond_types_crdt::list::ListCRDT::new();
                for txn in txns.iter() {
                    old_oplog.apply_remote_txn(txn);
                }
                black_box(old_oplog.to_string());
            })
        });

        // DIRTY!!!

        // let mut old_oplog = diamond_types_crdt::list::ListCRDT::new();
        // for txn in txns.iter() {
        //     old_oplog.apply_remote_txn(txn);
        // }
        // old_oplog.encode_small(true);

    }
}

#[cfg(feature = "stats")]
fn stats() {
    // for &name in DATASETS {
    // for &name in &["S1"] {
    for &name in &["S1", "S2", "S3"] {
        let txns = get_txns_from_file(&format!("../reg-paper/datasets/{}.dt", name));
        // dbg!(txns.len());
        // dbg!(txns.iter().map(|t| t.ops.len()).sum::<usize>());
        // dbg!(txns.iter()
        //     .map(|t| t.ops.iter().map(|o| o.len()).sum::<usize>())
        //     .sum::<usize>());

        // dbg!(txns.iter().take(10).collect::<Vec<_>>());

        // diamond_types_crdt::take_stats();
        let mut old_oplog = diamond_types_crdt::list::ListCRDT::new();
        for txn in txns.iter() {
            old_oplog.apply_remote_txn(txn);
        }

        let (hits, misses) = diamond_types_crdt::take_stats();
        println!("Trace {name}: Hits: {hits} misses {misses} / total {}", hits + misses);
    }
}

#[cfg(feature = "memusage")]
#[derive(Debug, Clone, Copy, Serialize)]
struct MemUsage {
    steady_state: usize,
    peak: usize,
}

// Run with:
// $ cargo run -p run_on_old --release --features memusage
#[cfg(feature = "memusage")]
fn measure_memory() {
    let mut usage = HashMap::new();

    for &name in DATASETS {
        print!("{name}...");
        let txns = get_txns_from_file(&format!("../reg-paper/datasets/{name}.dt"));

        let (peak, steady_state, _) = measure_memusage(|| {
            let mut old_oplog = diamond_types_crdt::list::ListCRDT::new();
            for txn in txns.iter() {
                old_oplog.apply_remote_txn(txn);
            }
            // Make sure there's no silly business.
            black_box(old_oplog.to_string());

            old_oplog
        });

        println!(" peak memory: {peak} / steady state {steady_state}");
        usage.insert(name.to_string(), MemUsage { peak, steady_state });
    }

    let json_out = serde_json::to_vec_pretty(&usage).unwrap();
    // let filename = "../../results/ot_memusage.json";
    let filename = "../reg-paper/results/dtcrdt_memusage.json";
    std::fs::write(filename, json_out).unwrap();
    println!("JSON written to {filename}");
}

fn main() {
    #[cfg(feature = "memusage")]
    measure_memory();

    #[cfg(feature = "bench")]
    {
        let mut c = Criterion::default()
            .configure_from_args();

        bench_process(&mut c);
        c.final_summary();
    }

    #[cfg(feature = "stats")]
    stats();
}

// fn main() {
//     // let name = "benchmark_data/node_nodecc.dt";
//     let name = "benchmark_data/git-makefile.dt";
//     // let name = "benchmark_data/data.dt";
//
//     let txns = get_txns(name);
//     println!("Applying changes to oplog");
//     let mut old_oplog = diamond_types_crdt::list::ListCRDT::new();
//     for (_i, txn) in txns.iter().enumerate() {
//         old_oplog.apply_remote_txn(txn);
//     }
//     println!("OK! Applied cleanly.");
// }

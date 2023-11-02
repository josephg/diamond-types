#[cfg(test)]
mod conformance_test;

use criterion::{black_box, Criterion};
use smallvec::{smallvec, SmallVec};
use diamond_types::causalgraph::agent_assignment::remote_ids::{RemoteVersionOwned as NewRemoteVersion};
use diamond_types::DTRange;
use diamond_types::list::ListOpLog;
use diamond_types::listmerge::to_old::OldCRDTOp;
use diamond_types_old::list::external_txn::{RemoteId as OldRemoteId, RemoteIdSpan as OldRemoteIdSpan, RemoteTxn};
use diamond_types_old::root_id;
use rle::{AppendRle, HasLength, SplitableSpan};

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

// NOTE: Not guaranteed to cover incoming span.
fn lv_to_remote_span(range: DTRange, oplog: &ListOpLog) -> OldRemoteIdSpan {
    if range.start == usize::MAX {
        panic!("Cannot convert a root timespan");
    } else {
        let span = oplog.cg.agent_assignment.local_to_remote_version_span(range);
        OldRemoteIdSpan {
            id: OldRemoteId {
                agent: span.0.into(),
                seq: span.1.start as _
            },
            len: span.1.len() as _
        }
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

    let mut result = vec![];
    for mut item in items {
        loop {
            let span = item.lv_span();
            let id = lv_to_remote_span(span, &oplog);
            // println!("{i}: id span {:?}", id);

            // assert_eq!(id.len as usize, item.len(), "Split items is unimplemented!");

            let id_len = id.len as usize;
            let rem = if id_len < item.len() {
                Some(item.truncate(id_len))
            } else {
                assert_eq!(id_len, item.len());
                None
            };

            let id = id.id;
            // println!("id {:?} item {:?}", id, item);
            let span = item.lv_span();

            let mut ops = smallvec![];
            let ins_content = match item {
                OldCRDTOp::Ins {
                    id, origin_left, origin_right, content
                } => {
                    ops.push_rle(diamond_types_old::list::external_txn::RemoteCRDTOp::Ins {
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

                        ops.push_rle(diamond_types_old::list::external_txn::RemoteCRDTOp::Del {
                            id: t_here.id,
                            len: t_here.len // always 1.
                        });
                    }
                    "".into()
                }
            };

            let mut parents: SmallVec<[OldRemoteId; 2]> = oplog.parents_at_version(span.start).iter().map(|p| {
                time_to_remote_id(*p, &oplog)
            }).collect();
            if parents.is_empty() {
                parents.push(root_id());
            }

            result.push(RemoteTxn {
                id,
                parents,
                ops,
                ins_content
            });

            if let Some(rem) = rem {
                item = rem;
            } else { break; }
        }
    }

    result
}

fn bench_process(c: &mut Criterion) {
    // let name = "benchmark_data/node_nodecc.dt";
    let name = "benchmark_data/friendsforever.dt";
    // let name = "benchmark_data/git-makefile.dt";
    // let name = "benchmark_data/data.dt";

    let txns = get_txns_from_file(name);

    c.bench_function(&format!("process_remote_edits/{name}"), |b| {
        // let old_str = old_oplog.to_string();
        // let new_str = oplog.checkout_tip().content().to_string();
        // assert_eq!(old_str, new_str);
        b.iter(|| {
            let mut old_oplog = diamond_types_old::list::ListCRDT::new();
            for txn in txns.iter() {
                old_oplog.apply_remote_txn(txn);
            }
            black_box(old_oplog.to_string());
        })
    });

    // DIRTY!!!

    let mut old_oplog = diamond_types_old::list::ListCRDT::new();
    for txn in txns.iter() {
        old_oplog.apply_remote_txn(txn);
    }
    old_oplog.encode_small(true);
}

fn main() {
    // benches();
    let mut c = Criterion::default()
        .configure_from_args();

    bench_process(&mut c);
    c.final_summary();
}

// fn main() {
//     // let name = "benchmark_data/node_nodecc.dt";
//     let name = "benchmark_data/git-makefile.dt";
//     // let name = "benchmark_data/data.dt";
//
//     let txns = get_txns(name);
//     println!("Applying changes to oplog");
//     let mut old_oplog = diamond_types_old::list::ListCRDT::new();
//     for (_i, txn) in txns.iter().enumerate() {
//         old_oplog.apply_remote_txn(txn);
//     }
//     println!("OK! Applied cleanly.");
// }

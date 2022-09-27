use criterion::{BenchmarkId, black_box, Criterion, criterion_group, criterion_main};
use smallvec::{smallvec, SmallVec};
use diamond_types::list::ListOpLog;
use diamond_types::list::merge::to_old::OldCRDTOp;
use diamond_types_old::list::external_txn::{RemoteId, RemoteIdSpan, RemoteTxn};
use diamond_types_old::root_id;
use rle::HasLength;

fn time_to_remote_id(time: usize, oplog: &ListOpLog) -> RemoteId {
    if time == usize::MAX {
        root_id()
    } else {
        new_to_old_remote_id(oplog.local_to_remote_time(time))
    }
}

fn new_to_old_remote_id(new: diamond_types::list::remote_ids::RemoteId) -> RemoteId {
    RemoteId {
        agent: new.agent,
        seq: new.seq as _
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

fn bench_process(c: &mut Criterion) {
    let name = "benchmark_data/node_nodecc.dt";
    // let name = "benchmark_data/git-makefile.dt";
    // let name = "benchmark_data/data.dt";
    let contents = std::fs::read(name).unwrap();
    println!("\n\nLoaded testing data from {} ({} bytes)", name, contents.len());
    let oplog = ListOpLog::load_from(&contents).unwrap();

    let items = oplog.dbg_items();
    dbg!(items.len());

    let txns: Vec<RemoteTxn> = items.into_iter().map(|item| {
        // let id = new_to_old_remote_span(item.remote_span(&oplog));
        let span = item.time_span();

        let (op, ins_content) = match item {
            OldCRDTOp::Ins {
                id, origin_left, origin_right, content
            } => {
                (diamond_types_old::list::external_txn::RemoteCRDTOp::Ins {
                    origin_left: time_to_remote_id(origin_left, &oplog),
                    origin_right: time_to_remote_id(origin_right, &oplog),
                    len: id.len() as _,
                    content_known: true
                }, content)
            }
            OldCRDTOp::Del { target, .. } => {
                (diamond_types_old::list::external_txn::RemoteCRDTOp::Del {
                    id: time_to_remote_id(target.span.start, &oplog),
                    len: target.len() as _
                }, "".into())
            }
        };

        // oplog.

        let mut parents: SmallVec<[RemoteId; 2]> = oplog.parents_at_time(span.start).iter().map(|p| {
            time_to_remote_id(*p, &oplog)
        }).collect();
        if parents.is_empty() {
            parents.push(root_id());
        }

        RemoteTxn {
            id: time_to_remote_id(span.start, &oplog),
            parents,
            ops: smallvec![op],
            ins_content
        }
    }).collect();

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

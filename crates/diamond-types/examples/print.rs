#![allow(unused)]

use std::env;
use diamond_types::list::{OpLog, encoding::EncodeOptions};
use rle::zip::rle_zip;

fn print_stats_for_file(name: &str) {
    let contents = std::fs::read(name).unwrap();
    println!("\n\nLoaded testing data from {} ({} bytes)", name, contents.len());

    let oplog = OpLog::load_from(&contents).unwrap();

    println!("\nOperations:");
    for op in oplog.iter() {
        println!("{:?}", op);
    }

    println!("\nHistory:");
    for hist in oplog.iter_history() {
        println!("{:?}", hist);
    }

    println!("\nAgent assignment mappings:");
    for m in oplog.iter_mappings() {
        println!("{:?} ('{}')", m, oplog.get_agent_name(m.agent));
    }

    // println!("\nTransformed operations:");
    // for (origin_op, (span, xf_op)) in rle_zip(oplog.iter(), oplog.get_all_transformed_operations()) {
    //     if let Some(xf_op) = xf_op {
    //         if origin_op.span != xf_op.span {
    //             let amt = xf_op.start() as isize - origin_op.start() as isize;
    //             println!("{:?} moved by {} ({:?} {:?})", span, amt, origin_op.tag, origin_op.content);
    //             // println!("{:?} moved {:?} -> {:?} {:?} {:?}", span, origin_op.span, xf_op.span, origin_op.tag, origin_op.content);
    //         }
    //     } else {
    //         println!("{:?} double deleted", span);
    //     }
    // }

    // for (span, op) in oplog.get_all_transformed_operations() {
    //     println!("{:?} {:?}", span, op);
    // }

    // for c in oplog.

    // for x in rle_zip3(
    //     oplog.iter_mappings(),
    //     oplog.iter_history(),
    //     oplog.iter()
    // ) {
    //     println!("{:?}", x);
    // }

    // for x in rle_zip(
    //     oplog.iter_history(),
    //     oplog.iter()
    // ) {
    //     println!("{:?}", x);
    // }

    println!();
    oplog.encode(EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: true,
        verbose: true,
    });
}


fn main() {
    let args = env::args();
    let filename = args.last().unwrap_or_else(|| "node_nodecc.dt".into());
    print_stats_for_file(&filename);
}
use std::collections::HashSet;
/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::Write as _;
use std::fs::File;
use std::io::{stderr, stdout, Write as _};
use std::process::Command;
use smallvec::{smallvec, SmallVec};
use rle::{HasLength, SplitableSpan};
use crate::list::ListOpLog;
use crate::dtrange::DTRange;
use crate::{CausalGraph, Frontier, LV};
use crate::causalgraph::graph::{Graph, GraphEntrySimple};
use crate::rle::KVPair;

pub fn name_of(time: LV) -> String {
    if time == LV::MAX { panic!("Should not see ROOT_TIME here"); }

    format!("{}", time)
}

#[derive(Debug, Clone, Copy)]
pub enum DotColor {
    Red, Green, Blue, Grey, Black
}

impl ToString for DotColor {
    fn to_string(&self) -> String {
        match self {
            DotColor::Red => "red".into(),
            DotColor::Green => "\"#98ea79\"".into(),
            DotColor::Blue => "\"#84a7e8\"".into(),
            DotColor::Grey => "\"#eeeeee\"".into(),
            DotColor::Black => "black".into(),
        }
    }
}

impl CausalGraph {
    pub fn make_time_dag_graph(&self, filename: &str) {
        // Same as above, but each merge creates a new dot item.
        let mut merges_touched = HashSet::new();

        fn key_for_parents(p: &[usize]) -> String {
            p.iter().map(|t| format!("{t}"))
                .collect::<Vec<_>>().join("x")
        }

        let mut out = String::new();
        out.push_str("strict digraph {\n");
        out.push_str("\trankdir=\"BT\"\n");
        // out.write_fmt(format_args!("\tlabel=<Starting string:<b>'{}'</b>>\n", starting_content));
        out.push_str("\tlabelloc=\"t\"\n");
        out.push_str("\tnode [shape=box style=filled]\n");
        out.push_str("\tedge [color=\"#333333\" dir=none]\n");

        write!(&mut out, "\tROOT [fillcolor={} label=<ROOT>]\n", DotColor::Red.to_string()).unwrap();
        let entries = self.make_conflict_graph::<()>();
        for (index, entry) in entries.ops.into_iter().enumerate() {
            // dbg!(txn);
            let range = entry.span;

            let parent_item = match entry.parents.len() {
                0 => "ROOT".to_string(),
                1 => format!("{}", entry.parents[0]),
                _ => {
                    let key = key_for_parents(entry.parents.as_ref());
                    // dbg!(&key);
                    if merges_touched.insert(key.clone()) {
                        // Emit the merge item.
                        write!(&mut out, "\t\"{key}\" [fillcolor={} label=\"\" shape=point]\n", DotColor::Blue.to_string()).unwrap();
                        for &p in entry.parents.iter() {
                            write!(&mut out, "\t\"{key}\" -> {} [color={}]\n", p, DotColor::Blue.to_string()).unwrap();
                            // write!(&mut out, "\t\"{key}\" -> {} [label={} color={}]\n", p, p, DotColor::Blue.to_string()).unwrap();
                        }
                    }

                    key
                }
            };

            if range.is_empty() {
                write!(&mut out, "\t{index} [label=<{index}>]\n").unwrap();
            } else {
                write!(&mut out, "\t{index} [label=<{index} {} (Len {})>]\n", range.start, range.len()).unwrap();
            }
            write!(&mut out, "\t{index} -> \"{parent_item}\"\n").unwrap();
        }

        out.push_str("}\n");

        let mut f = File::create("out.dot").unwrap();
        f.write_all(out.as_bytes()).unwrap();
        f.flush().unwrap();
        drop(f);

        let out = Command::new("dot")
            // .arg("-Tpng")
            .arg("-Tsvg")
            .stdin(File::open("out.dot").unwrap())
            .output().unwrap();

        // dbg!(out.status);
        // stdout().write_all(&out.stdout);
        // stderr().write_all(&out.stderr);

        let mut f = File::create(filename).unwrap();
        f.write_all(&out.stdout).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use crate::list::ListOpLog;
    use super::DotColor::*;

    // #[test]
    // #[ignore]
    // fn test1() {
    //     let mut ops = ListOpLog::new();
    //     ops.get_or_create_agent_id("seph");
    //     ops.get_or_create_agent_id("mike");
    //     ops.add_insert_at(0, &[], 0, "a");
    //     ops.add_insert_at(1, &[], 0, "b");
    //     ops.add_delete_at(0, &[0, 1], 0..2);
    //
    //     ops.make_merge_graph("test.svg", "asdf", [((0..ops.len()).into(), Red)].iter().copied());
    // }

    #[test]
    #[ignore]
    fn test2() {
        let mut ops = ListOpLog::new();
        ops.get_or_create_agent_id("seph");
        ops.get_or_create_agent_id("mike");
        let _a = ops.add_insert_at(0, &[], 0, "aaa");
        let b = ops.add_insert_at(1, &[], 0, "b");
        ops.add_delete_at(0, &[1, b], 0..2);
        // dbg!(&ops);

        ops.cg.make_time_dag_graph("dag.svg");
    }

    #[test]
    #[ignore]
    fn dot_of_node_cc() {
        let name = "benchmark_data/node_nodecc.dt";
        // let name = "benchmark_data/git-makefile.dt";
        let contents = fs::read(name).unwrap();
        let oplog = ListOpLog::load_from(&contents).unwrap();

        oplog.cg.make_time_dag_graph("node_graph.svg");
        println!("Graph written to node_graph.svg");
    }
}
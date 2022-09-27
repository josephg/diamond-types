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
use crate::rle::KVPair;
use crate::{Parents, ROOT_TIME, Time};
use crate::causalgraph::parents::ParentsEntrySimple;

pub fn name_of(time: Time) -> String {
    if time == ROOT_TIME { "ROOT".into() }
    else { format!("{}", time) }
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

impl Parents {
    /// This is a helper method to iterate through the time DAG, but such that there's nothing in
    /// the time DAG which splits the returned range via its parents.
    ///
    /// This method could be made more public - but right now its only used in this one place.
    fn iter_atomic_chunks(&self) -> impl Iterator<Item = ParentsEntrySimple> + '_ {
        self.entries.iter().flat_map(|e| {
            let mut split_points: SmallVec<[usize; 4]> = smallvec![e.span.last()];

            // let mut children = e.child_indexes.clone();
            for &child_idx in &e.child_indexes {
                let child = &self.entries[child_idx];
                for &p in &child.parents {
                    if e.span.contains(p) {
                        split_points.push(p);
                    }
                }
            }

            split_points.sort_unstable();

            // let mut last = None;
            let mut start = e.span.start;
            split_points.iter().flat_map(|&s| {
                // Filter duplicates.
                if s < start { return None; }

                let next = s + 1;
                let span = DTRange::from(start..next);

                assert!(!span.is_empty());
                assert!(next <= e.span.end);

                let parents = if start == e.span.start {
                    e.parents.clone()
                } else {
                    smallvec![start - 1]
                };

                start = next;

                Some(ParentsEntrySimple {
                    span,
                    parents
                })
            }).collect::<SmallVec<[ParentsEntrySimple; 4]>>()
        })
    }
}

impl ListOpLog {
    pub fn make_time_dag_graph(&self, filename: &str) {
        // for e in self.history.iter_atomic_chunks() {
        //     dbg!(e);
        // }

        let mut out = String::new();
        out.push_str("strict digraph {\n");
        out.push_str("\trankdir=\"BT\"\n");
        // out.write_fmt(format_args!("\tlabel=<Starting string:<b>'{}'</b>>\n", starting_content));
        out.push_str("\tlabelloc=\"t\"\n");
        out.push_str("\tnode [shape=box style=filled]\n");
        out.push_str("\tedge [color=\"#333333\" dir=none]\n");

        write!(&mut out, "\tROOT [fillcolor={} label=<ROOT>]\n", DotColor::Red.to_string()).unwrap();
        for txn in self.cg.parents.iter_atomic_chunks() {
            // dbg!(txn);
            let range = txn.span;

            write!(&mut out, "\t{} [label=<{} (Len {})>]\n", range.last(), range.start, range.len()).unwrap();

            if txn.parents.is_empty() {
                write!(&mut out, "\t{} -> ROOT\n", range.last()).unwrap();
            } else {
                for &p in txn.parents.iter() {
                    // let parent_entry = self.history.entries.find_packed(*p);
                    // write!(&mut out, "\t{} -> {} [headlabel={}]\n", txn.span.last(), parent_entry.span.start, *p);

                    write!(&mut out, "\t{} -> {} [taillabel={}]\n", range.last(), p, p).unwrap();
                }
            }
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

    pub fn make_time_dag_graph_with_merge_bubbles(&self, filename: &str) {
        // Same as above, but each merge creates a new dot item.
        let mut merges_touched = HashSet::new();

        fn key_for_parents(p: &[Time]) -> String {
            p.iter().map(|t| format!("{t}"))
                .collect::<Vec<_>>().join("0")
        }

        let mut out = String::new();
        out.push_str("strict digraph {\n");
        out.push_str("\trankdir=\"BT\"\n");
        // out.write_fmt(format_args!("\tlabel=<Starting string:<b>'{}'</b>>\n", starting_content));
        out.push_str("\tlabelloc=\"t\"\n");
        out.push_str("\tnode [shape=box style=filled]\n");
        out.push_str("\tedge [color=\"#333333\" dir=none]\n");

        write!(&mut out, "\tROOT [fillcolor={} label=<ROOT>]\n", DotColor::Red.to_string()).unwrap();
        for txn in self.cg.parents.iter_atomic_chunks() {
            // dbg!(txn);
            let range = txn.span;

            let parent_item = match txn.parents.len() {
                0 => "ROOT".to_string(),
                1 => format!("{}", txn.parents[0]),
                _ => {
                    let key = key_for_parents(&txn.parents);
                    if merges_touched.insert(key.clone()) {
                        // Emit the merge item.
                        write!(&mut out, "\t{key} [fillcolor={} label=\"\" shape=point]\n", DotColor::Blue.to_string()).unwrap();
                        for &p in txn.parents.iter() {
                            write!(&mut out, "\t{key} -> {} [label={} color={}]\n", p, p, DotColor::Blue.to_string()).unwrap();
                        }
                    }

                    key
                }
            };

            write!(&mut out, "\t{} [label=<{} (Len {})>]\n", range.last(), range.start, range.len()).unwrap();
            write!(&mut out, "\t{} -> {}\n", range.last(), parent_item).unwrap();
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

    pub fn make_merge_graph<I: Iterator<Item=(DTRange, DotColor)>>(&self, filename: &str, _starting_content: &str, iter: I) {
        let mut out = String::new();
        out.push_str("strict digraph {\n");
        out.push_str("\trankdir=\"BT\"\n");
        // out.write_fmt(format_args!("\tlabel=<Starting string:<b>'{}'</b>>\n", starting_content));
        out.push_str("\tlabelloc=\"t\"\n");
        out.push_str("\tnode [shape=box style=filled]\n");
        out.push_str("\tedge [color=\"#333333\" dir=back]\n");

        for (span, color) in iter {
            for time in span.iter() {
                let name = name_of(time);

                // This is horribly inefficient but I don't care.
                let (KVPair(_, op), offset) = self.operations.find_packed_with_offset(time);
                let mut op = op.to_operation(self);
                op.truncate_keeping_right(offset);
                op.truncate(1);

                let txn = self.cg.parents.entries.find_packed(time);

                // let label = if op.tag == Ins {
                // let label = if op.content_known {
                let label = if let Some(s) = &op.content {
                    // <b>72</b><br align="left"/>  Del 7 <s>'n'</s>
                    format!("<b>{}</b><br align=\"left\"/>{:?} {} '{}'", time, op.kind, op.start(), s)
                    // format!("{}: {:?} {} '{}'", time, op.tag, op.pos, &op.content)
                } else {
                    format!("{}: {:?} {}", time, op.kind, op.start())
                };
                out.write_fmt(format_args!("\t{} [fillcolor={} label=<{}>]\n", name, color.to_string(), label)).unwrap();

                txn.with_parents(time, |parents| {
                    // let color = if parents.len() == 1 { DotColor::Black } else { DotColor::Blue };

                    if parents.is_empty() {
                        out.write_fmt(format_args!("\t{} -> {} [arrowtail=none]\n", name, name_of(ROOT_TIME))).unwrap();
                    } else {
                        for p in parents {
                            out.write_fmt(format_args!("\t{} -> {} [color=\"#6b2828\" arrowtail=diamond]\n", name, name_of(*p))).unwrap();
                            // out.write_fmt(format_args!("\t{} -> {} [color=\"#6b2828\" arrowtail=ediamond penwidth=1.5]\n", name, name_of(*p))).unwrap();
                        }
                    }


                });
            }
        }

        out.push_str("}\n");

        let mut f = File::create("out.dot").unwrap();
        f.write_all(out.as_bytes()).unwrap();
        f.flush().unwrap();
        drop(f);

        let out = Command::new("dot")
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
    use crate::list::merge::dot::DotColor::*;
    use crate::list::ListOpLog;

    #[test]
    #[ignore]
    fn test1() {
        let mut ops = ListOpLog::new();
        ops.get_or_create_agent_id("seph");
        ops.get_or_create_agent_id("mike");
        ops.add_insert_at(0, &[], 0, "a");
        ops.add_insert_at(1, &[], 0, "b");
        ops.add_delete_at(0, &[0, 1], 0..2);

        ops.make_merge_graph("test.svg", "asdf", [((0..ops.len()).into(), Red)].iter().copied());
    }

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

        ops.make_time_dag_graph_with_merge_bubbles("dag.svg");
    }

    #[test]
    #[ignore]
    fn dot_of_node_cc() {
        let name = "node_nodecc.dt";
        let contents = fs::read(name).unwrap();
        let oplog = ListOpLog::load_from(&contents).unwrap();

        oplog.make_time_dag_graph("node_graph.svg");
        println!("Graph written to node_graph.svg");
    }
}
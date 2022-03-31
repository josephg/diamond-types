/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::{Write as _};
use std::fs::File;
use std::io::{Write as _};
use std::process::Command;
use rle::{HasLength, SplitableSpan};
use crate::list::{OpLog, Time};
use crate::dtrange::DTRange;
use crate::rle::KVPair;
use crate::ROOT_TIME;

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

impl OpLog {
    pub fn make_time_dag_graph(&self, filename: &str) {
        let mut out = String::new();
        out.push_str("strict digraph {\n");
        out.push_str("\trankdir=\"BT\"\n");
        // out.write_fmt(format_args!("\tlabel=<Starting string:<b>'{}'</b>>\n", starting_content));
        out.push_str("\tlabelloc=\"t\"\n");
        out.push_str("\tnode [shape=box style=filled]\n");
        out.push_str("\tedge [color=\"#333333\" dir=back]\n");

        write!(&mut out, "\tROOT [fillcolor={} label=<ROOT>]\n", DotColor::Red.to_string()).unwrap();
        for txn in self.history.entries.iter() {
            // dbg!(txn);
            // Each txn needs to be split so we can actually connect children to parents.
            // let mut children = txn.child_indexes.clone();
            // children.sort_unstable();
            // let mut iter = children.iter();
            let mut range = txn.span;
            let mut prev = None;
            // dbg!(range);

            let mut processed_parents = false;

            loop {
                // Look through our children to find the next split point.
                let mut earlist_parent = range.last();

                for idx in &txn.child_indexes {
                    let child_txn = &self.history.entries[*idx];
                    for p in &child_txn.parents {
                        if range.contains(*p) && *p < earlist_parent {
                            earlist_parent = *p;
                        }
                    }
                }

                // dbg!(earlist_parent, range, (earlist_parent - range.start + 1));
                let next = range.truncate(earlist_parent - range.start + 1);

                write!(&mut out, "\t{} [label=<{} (Len {})>]\n", range.last(), range.start, range.len()).unwrap();
                if let Some(prev) = prev {
                    write!(&mut out, "\t{} -> {}\n", range.last(), prev).unwrap();
                }

                if !processed_parents {
                    processed_parents = true;

                    if txn.parents.is_empty() {
                        write!(&mut out, "\t{} -> ROOT\n", range.last()).unwrap();
                    } else {
                        for p in txn.parents.iter() {
                            // let parent_entry = self.history.entries.find_packed(*p);
                            // write!(&mut out, "\t{} -> {} [headlabel={}]\n", txn.span.last(), parent_entry.span.start, *p);

                            write!(&mut out, "\t{} -> {} [headlabel={}]\n", range.last(), *p, *p).unwrap();
                        }
                    }
                }

                if !next.is_empty() {
                    prev = Some(range.last());
                    range = next;
                } else { break; }
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

                let txn = self.history.entries.find_packed(time);

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
    use crate::list::merge::dot::DotColor::*;
    use crate::list::OpLog;

    #[test]
    #[ignore]
    fn test1() {
        let mut ops = OpLog::new();
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
        let mut ops = OpLog::new();
        ops.get_or_create_agent_id("seph");
        ops.get_or_create_agent_id("mike");
        let _a = ops.add_insert_at(0, &[], 0, "aaa");
        let b = ops.add_insert_at(1, &[], 0, "b");
        ops.add_delete_at(0, &[1, b], 0..2);
        // dbg!(&ops);

        ops.make_time_dag_graph("dag.svg");
    }
}
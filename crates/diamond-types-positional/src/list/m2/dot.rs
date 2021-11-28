/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::{Write as _};
use std::fs::File;
use std::io::{Write as _};
use std::process::Command;
use rle::SplitableSpan;
use crate::list::{OpLog, Time};
use crate::localtime::TimeSpan;
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
    pub fn make_graph<I: Iterator<Item=(TimeSpan, DotColor)>>(&self, filename: &str, _starting_content: &str, iter: I) {
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
                let mut op = op.clone();
                op.truncate_keeping_right(offset);
                op.truncate(1);

                let txn = self.history.entries.find_packed(time);

                // let label = if op.tag == Ins {
                let label = if op.content_known {
                    // <b>72</b><br align="left"/>  Del 7 <s>'n'</s>
                    format!("<b>{}</b><br align=\"left\"/>{:?} {} '{}'", time, op.tag, op.pos, &op.content)
                    // format!("{}: {:?} {} '{}'", time, op.tag, op.pos, &op.content)
                } else {
                    format!("{}: {:?} {}", time, op.tag, op.pos)
                };
                out.write_fmt(format_args!("\t{} [fillcolor={} label=<{}>]\n", name, color.to_string(), label)).unwrap();

                txn.with_parents(time, |parents| {
                    // let color = if parents.len() == 1 { DotColor::Black } else { DotColor::Blue };

                    if parents.len() == 1 {
                        out.write_fmt(format_args!("\t{} -> {} [arrowtail=none]\n", name, name_of(parents[0]))).unwrap();
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
    use crate::list::m2::dot::DotColor::*;
    use crate::list::OpLog;
    use crate::ROOT_TIME;

    #[test]
    fn foo() {
        let mut ops = OpLog::new();
        ops.get_or_create_agent_id("seph");
        ops.get_or_create_agent_id("mike");
        ops.push_insert(0, &[ROOT_TIME], 0, "a");
        ops.push_insert(1, &[ROOT_TIME], 0, "b");
        ops.push_delete(0, &[0, 1], 0, 2);

        ops.make_graph("test.svg", "asdf", [((0..ops.len()).into(), Red)].iter().copied());
    }
}
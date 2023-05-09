use std::collections::HashSet;
/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::Write as _;
use std::fs::File;
use std::io::{stderr, stdout, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use smallvec::{smallvec, SmallVec};
use rle::{HasLength, SplitableSpan};
use crate::list::ListOpLog;
use crate::dtrange::DTRange;
use crate::{CausalGraph, Frontier, LV};
use crate::causalgraph::dot::render_dot_string;
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

impl ListOpLog {
    pub fn make_merge_graph<I: Iterator<Item=(DTRange, DotColor)>>(&self, filename: &Path, _starting_content: &str, iter: I) {
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
                let mut op = op.to_operation(&self.operation_ctx);
                op.truncate_keeping_right(offset);
                op.truncate(1);

                let txn = self.cg.graph.entries.find_packed(time);

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
                        out.write_fmt(format_args!("\t{} -> {} [arrowtail=none]\n", name, "ROOT")).unwrap();
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

        render_dot_string(out, filename);
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::Path;
    use crate::list::ListOpLog;
    use crate::listmerge::dot::DotColor::*;

    #[test]
    #[ignore]
    fn test1() {
        let mut ops = ListOpLog::new();
        ops.get_or_create_agent_id("seph");
        ops.get_or_create_agent_id("mike");
        ops.add_insert_at(0, &[], 0, "a");
        ops.add_insert_at(1, &[], 0, "b");
        ops.add_delete_at(0, &[0, 1], 0..2);

        ops.make_merge_graph(Path::new("test.svg"), "asdf", [((0..ops.len()).into(), Red)].iter().copied());
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

        ops.cg.generate_dot_svg(Path::new("dag.svg"));
    }

    #[test]
    #[ignore]
    fn dot_of_node_cc() {
        let name = "benchmark_data/node_nodecc.dt";
        let contents = fs::read(name).unwrap();
        let oplog = ListOpLog::load_from(&contents).unwrap();

        oplog.cg.generate_dot_svg(Path::new("node_graph.svg"));
        println!("Graph written to node_graph.svg");
    }
}
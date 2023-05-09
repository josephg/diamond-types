use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use rle::HasLength;
use crate::{CausalGraph, LV};

#[derive(Debug, Clone, Copy)]
#[allow(unused)]
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
    pub fn to_dot_graph(&self) -> String {
        // Same as above, but each merge creates a new dot item.
        let mut merges_touched = HashSet::new();

        fn key_for_parents(p: &[LV]) -> String {
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
        for txn in self.make_simple_graph().iter() {
            // dbg!(txn);
            let range = txn.span;

            let parent_item = match txn.parents.len() {
                0 => "ROOT".to_string(),
                1 => format!("{}", txn.parents[0]),
                _ => {
                    let key = key_for_parents(txn.parents.as_ref());
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

        out
    }

    pub(crate) fn generate_dot_svg(&self, out_filename: &Path) {
        render_dot_string(self.to_dot_graph(), out_filename);
    }
}

// This is for debugging.
pub(crate) fn render_dot_string(dot_content: String, out_filename: &Path) {
    let out_file = File::create(&out_filename).expect("Could not create output file");
    let dot_path = "dot";
    let mut child = Command::new(dot_path)
        // .arg("-Tpng")
        .arg("-Tsvg")
        .stdin(Stdio::piped())
        .stdout(out_file)
        .stderr(Stdio::piped())
        .spawn()
        .expect("Could not run dot");

    let mut stdin = child.stdin.take().unwrap();
    // Spawn is needed here to prevent a potential deadlock. See:
    // https://doc.rust-lang.org/std/process/index.html#handling-io
    std::thread::spawn(move || {
        stdin.write_all(dot_content.as_bytes()).unwrap();
    });

    let out = child.wait_with_output().unwrap();

    // Pipe stderr.
    std::io::stderr().write_all(&out.stderr).unwrap();

    println!("Wrote DOT output to {}", out_filename.display());
}

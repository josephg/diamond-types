use std::collections::HashSet;
use std::error::Error;
use std::ffi::OsString;
/// This file contains some helper code to create SVG images from time DAGs to show whats going on
/// in a document.
///
/// It was mostly made as an aide to debugging. Compilation is behind a feature flag (dot_export)

use std::fmt::{Display, Formatter, Write as _};
use std::io::Write as _;
use std::process::{Command, Stdio};
use diamond_types::{CausalGraph, HasLength, LV};

// pub fn name_of(time: LV) -> String {
//     if time == LV::MAX { panic!("Should not see ROOT_TIME here"); }
//
//     format!("{}", time)
// }

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


pub fn make_time_dag_graph(cg: &CausalGraph) -> String {
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
    for txn in cg.make_simple_graph().iter() {
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

#[derive(Debug)]
struct DotError;

impl Display for DotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("dot command failed with an error")
    }
}

impl Error for DotError {}

pub fn generate_svg_with_dot(dot_content: String, dot_path: Option<OsString>) -> Result<String, Box<dyn Error>> {
    let dot_path = dot_path.unwrap_or_else(|| "dot".into());
    let mut child = Command::new(dot_path)
        // .arg("-Tpng")
        .arg("-Tsvg")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();
    // Spawn is needed here to prevent a potential deadlock. See:
    // https://doc.rust-lang.org/std/process/index.html#handling-io
    std::thread::spawn(move || {
        stdin.write_all(dot_content.as_bytes()).unwrap();
    });

    let out = child.wait_with_output()?;

    // Pipe stderr.
    std::io::stderr().write_all(&out.stderr)?;

    if out.status.success() {
        Ok(String::from_utf8(out.stdout)?)
    } else {
        // May as well pipe stdout too.
        std::io::stdout().write_all(&out.stdout)?;
        Err(DotError.into())
    }
}

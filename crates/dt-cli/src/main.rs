mod export;
mod dot;
mod git;

use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use anyhow::Error;
use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use serde::Serialize;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionOwned;
use diamond_types::Frontier;
use diamond_types::list::{gen_oplog, ListBranch, ListOpLog};
use diamond_types::list::encoding::{ENCODE_FULL, EncodeOptions};
use crate::dot::{generate_svg_with_dot};
use crate::export::{check_trace_invariants, export_full_to_json, export_trace_to_json, export_transformed};
use crate::git::extract_from_git;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new diamond types file on disk
    Create {
        filename: PathBuf,

        /// Initialize the DT file with contents from here.
        ///
        /// Equivalent to calling create followed by set.
        #[arg(short)]
        input: Option<String>,

        /// Agent name for edits. If not specified, a random name is chosen.
        ///
        /// This is only relevant when content is provided. Empty files need no agent ID.
        #[arg(short, long)]
        agent: Option<String>,

        /// Create a new file, even if a file already exists with the given name
        #[arg(short, long)]
        force: bool,
    },

    /// Dump (cat) the contents of a diamond-types file to stdout or to a file
    Cat {
        /// Diamond types file to read
        #[arg(value_name = "filename", value_parser = parse_dt_oplog)]
        oplog: ListOpLog,

        /// Output contents to the named file instead of stdout
        #[arg(short, long)]
        output: Option<OsString>,

        /// Checkout at the specified (requested) version
        ///
        /// If not specified, the version defaults to the latest version, printing the result of
        /// merging all changes.
        #[arg(short, long)]
        version: Option<Version>,
    },

    Stats {
        /// Diamond types file to read
        #[arg(value_name = "filename", value_parser = parse_dt_oplog)]
        oplog: ListOpLog,

        // #[arg(short, long)]
        // json: bool,

        /// Output contents to the named file instead of stdout
        #[arg(short, long)]
        output: Option<OsString>,
    },

    // /// Dump the file at a series of versions to test conformance
    // Splat {
    //     /// Diamond types file to read
    //     #[arg(value_name = "filename", value_parser = parse_dt_oplog)]
    //     oplog: ListOpLog,
    //
    //     /// Output contents to the named file instead of stdout
    //     #[arg(short, long)]
    //     output: Option<OsString>,
    // },

    /// Print the operations contained within a diamond types file
    Log {
        /// Diamond types file to read
        #[arg(value_name = "filename", value_parser = parse_dt_oplog)]
        oplog: ListOpLog,

        /// Output the changes in a form where they can be applied directly (in order)
        #[arg(short, long)]
        transformed: bool,

        /// Output the changes in JSON format
        #[arg(short, long)]
        json: bool,

        /// Output the history instead (time DAG)
        #[arg(long)]
        history: bool,
    },

    /// Get (print) the current version of a DT file
    Version {
        /// Diamond types file to read
        #[arg(value_name = "filename", value_parser = parse_dt_oplog)]
        oplog: ListOpLog,
    },

    /// Set the contents of a DT file by applying a diff
    Set {
        /// Diamond types file to modify
        dt_filename: OsString,

        /// The file containing the new content
        target_content_file: OsString,

        /// Set the new content with this version as the named parent.
        ///
        /// If not specified, the version defaults to the latest version (including all changes)
        #[arg(short, long)]
        version: Option<Version>,

        /// Suppress output to stdout
        #[arg(short, long)]
        quiet: bool,

        /// Agent name for edits. If not specified, a random name is chosen.
        ///
        /// Be very careful overriding the default random agent name. If an (agent, seq) is ever
        /// reused to describe two *different* edits, weird & bad things happen.
        #[arg(short, long)]
        agent: Option<String>,
    },

    /// Re-save a diamond types file with different options. This method can:
    ///
    /// - Compress / uncompress the file's contents
    /// - Trim or prune the operations the file contains, to create a patch
    /// - Remove inserted / deleted content
    Repack {
        /// File to edit
        dt_filename: PathBuf,

        /// Save the resulting content to this file. If not specified, the original file will be
        /// overwritten.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Force overwrite the file which exists with the same name.
        #[arg(short, long)]
        force: bool,

        /// Disable internal LZ4 compression on the file when saving.
        #[arg(long)]
        uncompressed: bool,

        /// Trim the file to only contain changes from the specified point in time onwards.
        #[arg(short, long)]
        version: Option<Version>,

        #[arg(long)]
        truncate: Option<usize>,

        /// Save a patch. Patch files do not contain the base snapshot state. They must be merged
        /// with an existing DT file.
        #[arg(short, long)]
        patch: bool,

        /// Do not store inserted content. This prevents the editing trace being replayed, but an
        /// oplog with no inserted content can still have changes merged into it.
        ///
        /// Note: Support for this in Diamond types is still a work in progress.
        #[arg(long)]
        no_inserted_content: bool,

        /// Do not store deleted content. Deleted content can (usually) be reconstructed from the
        /// inserted content anyway, but its helpful if you want to skim back and forth through the
        /// file's history.
        #[arg(long)]
        no_deleted_content: bool,

        /// Suppress all output to stdout
        #[arg(short, long)]
        quiet: bool,
    },

    /// Export a diamond types file to raw JSON. This outputs the raw data stored in a diamond types
    /// file in a simplified JSON format.
    Export {
        /// File to export
        dt_filename: OsString,

        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Use pretty JSON output
        #[arg(short, long)]
        pretty: bool,
    },

    /// Export a diamond types file to raw JSON. This produces an editing log which can be processed
    /// by other compatible CRDT libraries for benchmarking and testing.
    ///
    /// See https://github.com/josephg/editing-traces for detail.
    ExportTrace {
        /// File to export
        dt_filename: OsString,

        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Use pretty JSON output
        #[arg(short, long)]
        pretty: bool,

        /// The file containing timestamps to merge
        #[arg(short)]
        timestamp_filename: Option<OsString>,

        // /// Force generation of output even if trace breaks some of the rules for well defined
        // /// shared traces.
        // #[arg(short, long)]
        // force: bool,

        /// When there is no timestamp file, shatter the trace such that every keystroke gets its
        /// own transaction
        #[arg(short, long)]
        shatter: bool,
    },

    ExportTraceSimple {
        /// File to edit
        dt_filename: OsString,

        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Use pretty JSON output
        #[arg(short, long)]
        pretty: bool,

        /// The file containing timestamps to merge
        #[arg(short)]
        timestamp_filename: Option<OsString>,

        /// When there is no timestamp file, shatter the trace such that every keystroke gets its
        /// own transaction
        #[arg(short, long)]
        shatter: bool
    },

    /// Generate and export testing data for multi-implementation conformance testing.
    GenConformance {
        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Number of example test cases to generate
        #[arg(short, long)]
        num: Option<usize>,

        /// Number of steps for each example
        #[arg(short, long)]
        steps: Option<usize>,

        /// RNG seed for the generated data
        #[arg(long)]
        seed: Option<u64>,

        /// Use non-ascii characters
        #[arg(short, long)]
        unicode: bool,

        /// Use pretty JSON output. Note: Steps must be 1.
        #[arg(short, long)]
        pretty: bool,

        /// Generate "simple" operation traces. Simple traces enforce a strict total order over
        /// all changes coming from each user agent. Diamond types proper doesn't require this
        /// constraint internally.
        #[arg(long)]
        simple: bool,
    },

    /// Generate a diagram of the causal graph contained in a diamond types' file.
    ///
    /// This depends on having the `dot` tool from [graphviz](https://graphviz.org/download/)
    /// installed on your computer.
    ///
    /// By default, we will execute `dot` in the system path to render graphs. But this can be
    /// overridden using `--dot-path="xxx/dot"`.
    Dot {
        /// File to edit
        dt_filename: PathBuf,

        #[arg(short, long)]
        no_render: bool,

        /// Output the result to the specified filename. If missing, output is saved to
        /// (dt file).svg / .dot.
        ///
        /// Use -o- to output to stdout instead.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Path to `dot` command
        #[arg(long)]
        dot_path: Option<OsString>,

        #[arg(short, long)]
        truncate: Option<usize>
    },

    /// Import & convert the editing history for a file from git to diamond types.
    GitImport {
        /// Path to the file being read. Must be inside a git repository.
        path: PathBuf,

        /// branch to be read. Defaults to 'master'.
        #[arg(short, long)]
        branch: Option<String>,

        /// Quiet mode
        #[arg(short, long)]
        quiet: bool,

        /// Output filename
        #[arg(short, long)]
        out: Option<PathBuf>,

        /// Output an extra file containing mapping from git commits <-> DT versions.
        #[arg(short, long)]
        map_out: Option<PathBuf>,
    },

    /// Duplicate an operation log some integer number of times.
    BenchDuplicate {
        /// File
        path: PathBuf,

        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Force overwrite the file which exists with the same name.
        #[arg(short, long)]
        force: bool,

        /// The number of times to duplicate it
        #[arg(short)]
        number: u32,

        /// Suppress all output to stdout
        #[arg(short, long)]
        quiet: bool,
    }
}

#[derive(Clone, Debug)]
struct Version(Box<[RemoteVersionOwned]>);

impl FromStr for Version {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Version(serde_json::from_str(s)?))
    }
}

fn parse_dt_oplog(filename: &str) -> Result<ListOpLog, anyhow::Error> {
    let data = fs::read(filename)?;
    let oplog = ListOpLog::load_from(&data)?;
    Ok(oplog)
}

// fn checkout_version_or_tip(oplog: OpLog, version: Option<&[RemoteVersionOwned]>) -> Branch {
fn checkout_version_or_tip(oplog: &ListOpLog, version: Option<Box<[RemoteVersionOwned]>>) -> ListBranch {
    let v = if let Some(version) = version {
        oplog.cg.agent_assignment.try_remote_to_local_frontier(version.iter()).unwrap()
    } else {
        oplog.local_frontier()
    };

    oplog.checkout(v.as_ref())
}

fn main() -> Result<(), anyhow::Error> {
    let cli: Cli = Cli::parse();
    match cli.command {
        Commands::Create { filename, input: content_file, agent, force } => {
            let mut oplog = ListOpLog::new();

            if let Some(content_file) = content_file {
                let content = fs::read_to_string(content_file)?;
                let agent_name = agent.unwrap_or_else(random_agent_name);
                let agent = oplog.get_or_create_agent_id(&agent_name);
                oplog.add_insert(agent, 0, &content);
            }

            let data = oplog.encode(&ENCODE_FULL);

            maybe_overwrite(&filename, &data, force)?;
        }

        Commands::Cat { oplog, output, version } => {
            // let data = fs::read(filename)?;
            // Using custom oplog / branch here to support custom versions
            // let oplog = OpLog::load_from(&data).unwrap();

            // let branch = checkout_version_or_tip(oplog, version.map(|v| &v));
            let branch = checkout_version_or_tip(&oplog, version.map(|v| v.0));
            let content = branch.content();

            // There's probably some fancy way to switch and share code here - either write to a
            // File or stdout. But eh.
            if let Some(output) = output {
                let mut file = fs::File::create(output)?;
                write!(&mut file, "{content}")?;
            } else {
                print!("{}", content);
            }
        }

        Commands::Stats { oplog, output } => {
            let stats = oplog.get_stats();
            let json = serde_json::to_string_pretty(&stats).unwrap();

            if let Some(output) = output {
                let mut file = File::create(output)?;
                write!(&mut file, "{json}")?;
            } else {
                print!("{}", json);
            }
        }

        // Commands::Splat { oplog, output } => {
        //     #[derive(Debug, Serialize)]
        //     #[serde(rename_all = "camelCase")]
        //     struct OutData {
        //         num_ops: usize,
        //         f: Vec<usize>,
        //         r: Vec<RemoteVersionOwned>,
        //         result: String,
        //     }
        //
        //     let mut result = vec![];
        //     // for num_ops in (0..oplog.len()).step_by(100) {
        //     for num_ops in (13130..13140).step_by(1) {
        //         // This is dirty.
        //         let all_versions: Vec<usize> = (0..num_ops).collect();
        //         let f = oplog.cg.graph.find_dominators(&all_versions);
        //         // let f = Frontier::new_1(v - 1);
        //
        //         let branch = oplog.checkout(f.as_ref());
        //         let r = oplog.cg.agent_assignment.local_to_remote_frontier_owned(f.as_ref()).into_vec();
        //         result.push(OutData {
        //             num_ops,
        //             f: f.0.into_vec(),
        //             r,
        //             result: branch.content().to_string()
        //         });
        //     }
        //
        //     write_serde_data(output, true, &result)?;
        // },

        Commands::Log { oplog, transformed, json, history: history_mode } => {
            if history_mode {
                for hist in oplog.iter_history() {
                    if json {
                        let s = serde_json::to_string(&hist).unwrap();
                        println!("{s}");
                    } else {
                        println!("{:?}", hist);
                    }
                }
            } else if transformed {
                    for (_, op) in oplog.iter_xf_operations() {
                        if let Some(op) = op {
                            if json {
                                let s = serde_json::to_string(&op).unwrap();
                                println!("{s}");
                            } else {
                                println!("{:?}", op);
                            }
                        }
                    }
            } else {
                for op in oplog.iter_ops() {
                    // println!("{} len {}", op.tag, op.len());
                    if json {
                        let s = serde_json::to_string(&op).unwrap();
                        println!("{s}");
                    } else {
                        println!("{:?}", op);
                    }
                }
            }
        }

        Commands::Version { oplog } => {
            let version = serde_json::to_string(&oplog.remote_frontier()).unwrap();
            println!("{version}");
        }

        Commands::Set { dt_filename, target_content_file, version, quiet, agent } => {
            let data = fs::read(&dt_filename)?;

            let new = if target_content_file == "-" {
                let mut s = String::new();
                std::io::stdin().read_to_string(&mut s)?;
                s
            } else {
                fs::read_to_string(target_content_file)?
            };

            let mut oplog = ListOpLog::load_from(&data)?;

            if !quiet {
                let v_json = if let Some(v) = version.as_ref() {
                    // println!("Editing from requested version {}",
                    serde_json::to_string(&v.0)
                } else {
                    // println!("Editing from tip version {:?}", oplog.remote_version());
                    serde_json::to_string(&oplog.remote_frontier())
                }.unwrap();
                println!("Editing from version {v_json}");
            }

            let mut branch = checkout_version_or_tip(&oplog, version.map(|v| v.0));

            let old = branch.content().to_string();
            let diff = TextDiff::from_chars(&old, &new);
            let remapper = TextDiffRemapper::from_text_diff(&diff, &old, &new);

            let agent_name = agent.unwrap_or_else(random_agent_name);
            let agent_id = oplog.get_or_create_agent_id(&agent_name);

            let mut pos = 0;
            for (tag, str) in diff.ops().iter()
                .flat_map(move |x| remapper.iter_slices(x)) {

                let len = str.chars().count();
                match tag {
                    ChangeTag::Equal => pos += len,
                    ChangeTag::Delete => {
                        // dbg!(("delete", pos .. pos+len));
                        branch.delete(&mut oplog, agent_id, pos .. pos+len);
                    }
                    ChangeTag::Insert => {
                        // dbg!(("insert", pos, str));
                        branch.insert(&mut oplog, agent_id, pos, str);
                        pos += len;
                    }
                }
            }

            if !quiet {
                println!("Resulting branch version after changes {}",
                         serde_json::to_string(&branch.remote_frontier(&oplog)).unwrap());
                println!("Resulting file version after changes {}",
                         serde_json::to_string(&oplog.remote_frontier()).unwrap());
            }

            // TODO: Do that atomic rename nonsense instead of just overwriting.
            let out_data = oplog.encode(&EncodeOptions::default());
            fs::write(&dt_filename, out_data)?;
        }

        Commands::Repack { dt_filename, output, force, uncompressed, version, truncate, patch, no_inserted_content, no_deleted_content, quiet } => {
            let data = fs::read(&dt_filename)?;
            let mut oplog = ListOpLog::load_from(&data)?;

            let from_version = match &version {
                Some(v) => v.0.as_ref(),
                None => &[],
            };
            let from_version = oplog.cg.agent_assignment.remote_to_local_frontier(from_version.iter());

            if let Some(truncate) = truncate {
                let mut trimmed_oplog = ListOpLog::new();
                for (op, graph, agent_span) in oplog.iter_full_range((0..truncate).into()) {
                    // I'm going to ignore the agent span and just let it extend naturally.
                    let agent = trimmed_oplog.get_or_create_agent_id(agent_span.0);
                    trimmed_oplog.add_operations_at(agent, graph.parents.as_ref(), &[op]);
                }
                oplog = trimmed_oplog;
            }

            let new_data = oplog.encode_from(&EncodeOptions {
                user_data: None,
                store_start_branch_content: !patch,
                experimentally_store_end_branch_content: false,
                store_inserted_content: !no_inserted_content,
                store_deleted_content: !no_deleted_content,
                compress_content: !uncompressed,
                verbose: false
            }, from_version.as_ref());

            let lossy = no_inserted_content || no_deleted_content || !from_version.is_empty();
            if output.is_none() && !force && lossy {
                eprintln!("Will not commit operation which may lose data. Try again with -f to force");
                std::process::exit(1); // Would be better to return a custom error.
            }

            if let Some(output) = output.as_ref() {
                maybe_overwrite(output, &new_data, force)?;
            } else {
                // Just overwrite the input file. We've already checked that --force is set or the
                // change is not lossy.
                fs::write(&dt_filename, &new_data)?;
            }

            if !quiet {
                println!("Initial size: {}", data.len());
                println!("Written {} bytes to {}", new_data.len(), output.unwrap_or(dt_filename)
                    .to_str()
                    .unwrap_or("(invalid)"));
            }
        }

        Commands::Export { dt_filename, output, pretty } => {
            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let result = export_full_to_json(&oplog);
            write_serde_data(output, pretty, &result)?;
        }

        Commands::ExportTrace { dt_filename, output, pretty, timestamp_filename, shatter } => {
            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let problems = check_trace_invariants(&oplog);
            if !problems.is_ok() {
                if problems.has_conflicts {
                    eprintln!("\
                        WARNING: Oplog has conflicts when merging.\n\
                        This means the oplog may have differing merge results based on the sequence CRDT\n\
                        used to process it.");
                }
                // Because we always export in "safe mode", this should never be a problem.
                // if problems.agent_ops_not_fully_ordered && !safe {
                //     eprintln!("WARNING: Operations from each agent are not fully ordered. Rerun in safe mode (--safe).");
                // }
                if problems.multiple_roots {
                    eprintln!("WARNING: Operation log has multiple roots.");
                }
                eprintln!("\n\
                    This trace is unsuitable for use as a general purpose editing trace.\n\
                    Please do not publish it on the editing trace repository.\
                ");
            }

            let result = export_trace_to_json(&oplog, timestamp_filename, shatter);
            write_serde_data(output, pretty, &result)?;
        }

        Commands::ExportTraceSimple { dt_filename, output, pretty, timestamp_filename, shatter } => {
            // In this editing trace format, a timestamp is passed in each transaction. We'll just
            // construct a single timestamp for the whole file based on the file's mtime and use
            // that everywhere.

            // let metadata = fs::metadata(&dt_filename)?;
            // let modified_time = metadata.modified()?;
            // let datetime: DateTime<Utc> = modified_time.into();
            // let timestamp = datetime.with_minute(0).unwrap()
            //     .with_second(0).unwrap()
            //     .to_rfc3339_opts(SecondsFormat::Secs, true);

            // println!("time {time}");

            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let result = export_transformed(&oplog, timestamp_filename, shatter);
            write_serde_data(output, pretty, &result)?;
        }

        Commands::GenConformance { output, num, steps, seed, pretty, unicode, simple } => {
            let num = num.unwrap_or(100);
            let steps = steps.unwrap_or(if pretty { 1 } else { 50 });
            let seed = seed.unwrap_or_else(|| rand::thread_rng().next_u64());

            if pretty && steps != 1 && output.is_some() {
                panic!("Cannot pretty-print more than 1 step to a file, because the output is in line-delimited JSON and that isn't readable!");
            }

            write_serde_data_iter(output, pretty, (0..num).into_iter().map(|i| {
                // Hardcoded agent interleaving. Might be worth turning that off at some point.
                let oplog = gen_oplog(seed + i as u64, steps, unicode, !simple);
                export_full_to_json(&oplog)
            }))?;
        }

        Commands::Dot { dt_filename, no_render, output, dot_path, truncate } => {
            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let dot_input = if let Some(t) = truncate {
                // There's probably a way to do this using nice rust constructs, but the borrow checker is hard.
                oplog.cg.to_dot_graph(Some(&[t]))
            } else {
                oplog.cg.to_dot_graph(None)
            };
            // println!("{dot_input}");

            let render = !no_render;

            if render {
                let svg_contents = generate_svg_with_dot(dot_input, dot_path)
                    .expect("Error running DOT");
                let out_filename = get_filename_from(&dt_filename, output, "svg");
                if let Some(out_filename) = out_filename {
                    fs::write(&out_filename, svg_contents)?;
                    println!("Wrote SVG to {}", out_filename.to_string_lossy());
                } else {
                    println!("{svg_contents}");
                }
            } else {
                let out_filename = get_filename_from(&dt_filename, output, "dot");
                if let Some(out_filename) = out_filename {
                    fs::write(&out_filename, dot_input)?;
                    println!("Wrote dot to {}", out_filename.to_string_lossy());
                } else {
                    println!("{dot_input}");
                }
            }
        }

        Commands::GitImport { path, branch, quiet, out, map_out } => {
            let oplog = extract_from_git(path.clone(), branch, quiet, map_out)?;

            let out_filename = out.unwrap_or_else(|| {
                let stem = path.file_stem().expect("Invalid path");
                let mut path = PathBuf::from(stem);
                path.set_extension("dt");
                path
            });

            let data = oplog.encode(&ENCODE_FULL);
            fs::write(&out_filename, &data).unwrap();
            if !quiet {
                println!("{} bytes written to {}", data.len(), out_filename.display());
            }
        }

        Commands::BenchDuplicate { path, output, force, number, quiet } => {
            let data = fs::read(&path)?;
            let orig_oplog = ListOpLog::load_from(&data)?;

            // I'll copy the agent order directly.
            let mut new_oplog = ListOpLog::new();
            for i in 0..orig_oplog.num_agents() {
                let agent = orig_oplog.get_agent_name(i);
                let new_id = new_oplog.get_or_create_agent_id(agent);
                assert_eq!(i, new_id);
            }

            // This could be implemented in a more efficient way - but this is straightforward and
            // fine.

            // Each time we iterate, we'll glue the first operations of the graph to the end of
            // the graph from last time.
            let mut last_end = Frontier::root();
            let mut last_len = 0;
            let mut f_buf = Frontier::root();
            for _i in 0..number {
                for (op, graph, agent_span) in orig_oplog.iter_full() {
                    // dbg!(&graph);
                    // I'm going to ignore the agent span and just let it extend naturally.
                    let agent = new_oplog.get_or_create_agent_id(agent_span.0);

                    let parents = if graph.parents.is_root() {
                        last_end.as_ref()
                    } else {
                        f_buf.0.clear();
                        f_buf.0.extend(graph.parents.iter().map(|p| { p + last_len }));
                        f_buf.as_ref()
                        // graph.parents.as_ref()
                    };
                    new_oplog.add_operations_at(agent, parents, &[op]);
                }

                last_end = new_oplog.local_frontier().clone();
                last_len = new_oplog.len();
            }

            // let new_data = ENCODE_FULL.clone().verbose(true).encode(&new_oplog);
            let new_data = ENCODE_FULL.encode(&new_oplog);

            if let Some(output) = output.as_ref() {
                maybe_overwrite(output, &new_data, force)?;
            } else {
                // Overwrite the input file. We've already checked that --force is set or the
                // change is not lossy.

                let base_path = path.file_stem().expect("Invalid path")
                    .to_str()
                    .unwrap();

                let path = format!("{base_path}x{number}.dt");

                // let stem = path.file_stem().expect("Invalid path");
                //
                // // Rewrite foo.dt -> foox20.dt.
                // let mut path = PathBuf::from(stem);
                // // let x = path.as_mut_os_str(); //(format!("x{number}"));
                // path.set_extension("dt");
                // dbg!(&path);

                fs::write(&path, &new_data)?;
                println!("Wrote result to {:?}", path);
            }

            if !quiet {
                println!("Operation length {} -> {}", orig_oplog.len(), new_oplog.len());
                println!("Resulting file length {} -> {}", data.len(), new_data.len());
            }
        }
    }
    // dbg!(&cli);
    Ok(())
}

fn write_serde_data<T: Serialize>(output: Option<OsString>, pretty: bool, val: T) -> Result<(), Error> {
    write_serde_data_iter(output, pretty, std::iter::once(val))
}

fn write_serde_data_iter<T: Serialize, I: Iterator<Item = T>>(mut output: Option<OsString>, pretty: bool, iter: I) -> Result<(), Error> {
    // This repetition is gross, but I'm not sure a better way to do it given the type of
    // stdout and File are different. Halp!

    // Bit gross. Handle -o- even though its unnecessary.
    if let Some(path) = &output {
        if path == "-" { output = None; }
    }

    if let Some(path) = output {
        // I can't treat std::io::stdout as a file, so this code is repeated unnecessarily.
        // Could instead box and use Box<dyn Writer>. Bleh.
        let mut writer = BufWriter::new(File::create(path)?);
        for data in iter {
            if pretty {
                serde_json::to_writer_pretty(&mut writer, &data)?;
            } else {
                serde_json::to_writer(&mut writer, &data)?;
            }
            writer.write_all("\n".as_bytes())?;
        }
    } else {
        let mut writer = BufWriter::new(std::io::stdout());
        for data in iter {
            if pretty {
                serde_json::to_writer_pretty(&mut writer, &data)?;
            } else {
                serde_json::to_writer(&mut writer, &data)?;
            }
            writer.write_all("\n".as_bytes())?;
        }
    }

    Ok(())
}

fn get_filename_from(dt_filename: &PathBuf, output: Option<OsString>, extension: &str) -> Option<PathBuf> {
    if let Some(output) = output {
        if output == "-" {
            // Output to stdout.
            None
        } else {
            Some(PathBuf::from(output))
        }
    } else {
        Some(dt_filename.with_extension(extension))
    }
}

fn maybe_overwrite(output: &Path, new_data: &Vec<u8>, force: bool) -> Result<(), anyhow::Error> {
    let file_result = fs::OpenOptions::new()
        .create_new(!force)
        .create(true)
        .write(true)
        .truncate(true)
        .open(output);

    if let Err(x) = file_result.as_ref() {
        if x.kind() == ErrorKind::AlreadyExists {
            let f = output.to_str().unwrap_or("(invalid)");
            eprintln!("Output file '{f}' already exists. Overwrite by passing -f");
        }
    }

    file_result?.write_all(&new_data)?;
    Ok(())
}

fn random_agent_name() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

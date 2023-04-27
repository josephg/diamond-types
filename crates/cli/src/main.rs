mod export;

use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::str::FromStr;
use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::Rng;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionOwned;
use diamond_types::list::{ListBranch, ListOpLog};
use diamond_types::list::encoding::{ENCODE_FULL, EncodeOptions};
use crate::export::export_to_json;

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
        filename: OsString,

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
        dt_filename: OsString,

        /// Save the resulting content to this file. If not specified, the original file will be
        /// overwritten.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Force overwrite the file which exists with the same name.
        #[arg(short, long)]
        force: bool,

        /// Disable internal LZ4 compression on the file when saving.
        #[arg(long)]
        uncompressed: bool,

        /// Trim the file to only contain changes from the specified point in time onwards.
        #[arg(short, long)]
        version: Option<Version>,

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

    /// Export a diamond types file to raw JSON. This produces an editing log which can be processed
    /// by other compatible CRDT libraries for benchmarking and testing.
    Export {
        /// File to edit
        dt_filename: OsString,

        /// Output the result to the specified filename. If missing, output is printed to stdout.
        #[arg(short, long)]
        output: Option<OsString>,

        /// Use pretty JSON output
        #[arg(short, long)]
        pretty: bool,
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

            let data = oplog.encode(ENCODE_FULL);

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
                for op in oplog.iter() {
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
            let out_data = oplog.encode(EncodeOptions::default());
            fs::write(&dt_filename, out_data)?;
        }

        Commands::Repack { dt_filename, output, force, uncompressed, version, patch, no_inserted_content, no_deleted_content, quiet } => {
            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let from_version = match &version {
                Some(v) => v.0.as_ref(),
                None => &[],
            };
            let from_version = oplog.cg.agent_assignment.remote_to_local_frontier(from_version.iter());

            let new_data = oplog.encode_from(EncodeOptions {
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

        Commands::Export { dt_filename, mut output, pretty } => {
            let data = fs::read(&dt_filename)?;
            let oplog = ListOpLog::load_from(&data)?;

            let result = export_to_json(&oplog);

            // This repetition is gross, but I'm not sure a better way to do it given the type of
            // stdout and File are different. Halp!

            // Bit gross. Handle -o- even though its unnecessary.
            if let Some(path) = &output {
                if path == "-" { output = None; }
            }

            if let Some(path) = output {
                let writer = BufWriter::new(File::create(path)?);
                if pretty {
                    serde_json::to_writer_pretty(writer, &result)?;
                } else {
                    serde_json::to_writer(writer, &result)?;
                }
            } else {
                let writer = BufWriter::new(std::io::stdout());
                if pretty {
                    serde_json::to_writer_pretty(writer, &result)?;
                } else {
                    serde_json::to_writer(writer, &result)?;
                }
            }
            // serde_json::to_writer()
        }
    }
    // dbg!(&cli);
    Ok(())
}

fn maybe_overwrite(output: &OsString, new_data: &Vec<u8>, force: bool) -> Result<(), anyhow::Error> {
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

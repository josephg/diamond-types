use std::ffi::OsString;
use std::fs;
use std::io::Write;
use clap::{Parser, Subcommand};
use rand::distributions::Alphanumeric;
use rand::Rng;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;
use diamond_types::list::{ListCRDT, OpLog};
use diamond_types::list::encoding::{ENCODE_FULL, EncodeOptions};
use diamond_types::list::remote_ids::RemoteId;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new diamond types file on disk
    Create {
        #[clap(parse(from_os_str))]
        filename: OsString,

        /// Initialize the DT file with contents from here.
        ///
        /// Equivalent to calling create followed by set.
        #[clap(short)]
        content_file: Option<String>,
    },

    /// Dump (cat) the contents of a diamond-types file to stdout or to a file
    Cat {
        /// Diamond types file to read
        #[clap(name = "filename", parse(try_from_str = parse_dt_oplog))]
        oplog: OpLog,

        /// Output contents to the named file instead of stdout
        #[clap(short, long)]
        output: Option<String>,

        /// Checkout at the specified (requested) version
        #[clap(short, long, parse(try_from_str = serde_json::from_str))]
        version: Option<Box<[RemoteId]>>,
    },

    /// Print the operations contained within a diamond types file
    Ops {
        /// Diamond types file to read
        #[clap(name = "filename", parse(try_from_str = parse_dt_oplog))]
        oplog: OpLog,

        /// Output the changes in a form where they can be applied directly (in order)
        #[clap(short, long)]
        transformed: bool,

        /// Output the changes in JSON format
        #[clap(short, long)]
        json: bool,

        /// Output the history instead (time DAG)
        #[clap(long)]
        history: bool,
    },

    /// Get (print) the current version of a DT file
    Version {
        /// Diamond types file to read
        #[clap(name = "filename", parse(try_from_str = parse_dt_oplog))]
        oplog: OpLog,
    },

    /// Set the contents of a DT file by applying a diff
    Set {
        /// Diamond types file to modify
        #[clap(parse(from_os_str))]
        dt_filename: OsString,

        /// The file containing the new content
        #[clap(parse(from_os_str))]
        target_content_file: OsString,
    }
}

fn parse_dt_oplog(filename: &str) -> Result<OpLog, anyhow::Error> {
    let data = fs::read(filename)?;
    let oplog = OpLog::load_from(&data)?;
    Ok(oplog)
}

fn main() -> Result<(), anyhow::Error> {
    let cli: Cli = Cli::parse();
    match cli.command {
        Commands::Create { filename, content_file } => {
            let mut oplog = OpLog::new();

            if let Some(content_file) = content_file {
                let content = fs::read_to_string(content_file)?;
                let agent = oplog.get_or_create_agent_id(&random_agent_name());
                oplog.add_insert(agent, 0, &content);
            }

            let data = oplog.encode(ENCODE_FULL);

            fs::write(filename, data)?;
        }

        Commands::Cat { oplog, output, version } => {
            // let data = fs::read(filename)?;
            // Using custom oplog / branch here to support custom versions
            // let oplog = OpLog::load_from(&data).unwrap();

            let v = if let Some(version) = version {
                oplog.try_remote_to_local_version(version.iter()).unwrap()
            } else {
                oplog.local_version().into() // Gross copy.
            };

            let branch = oplog.checkout(&v);
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

        Commands::Ops { oplog, transformed, json, history: history_mode } => {
            if history_mode {
                for hist in oplog.iter_history() {
                    println!("{:?}", hist);
                }
            } else {
                if transformed {
                    for (_, op) in oplog.iter_xf_operations() {
                        if json {
                            let s = serde_json::to_string(&op).unwrap();
                            println!("{s}");
                        } else {
                            println!("{:?}", op);
                        }
                    }
                }
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
            let version = serde_json::to_string(&oplog.remote_version()).unwrap();
            println!("{version}");
        }

        Commands::Set { dt_filename, target_content_file } => {
            let data = fs::read(&dt_filename)?;
            let new = fs::read_to_string(target_content_file)?;

            let mut doc = ListCRDT::load_from(&data).unwrap();
            let old = doc.branch.content().to_string();
            let diff = TextDiff::from_chars(&old, &new);
            let remapper = TextDiffRemapper::from_text_diff(&diff, &old, &new);

            let agent_name: String = random_agent_name();
            let agent_id = doc.get_or_create_agent_id(&agent_name);

            let mut pos = 0;
            for (tag, str) in diff.ops().iter()
                .flat_map(move |x| remapper.iter_slices(x)) {

                let len = str.chars().count();
                match tag {
                    ChangeTag::Equal => pos += len,
                    ChangeTag::Delete => {
                        doc.delete(agent_id, pos .. pos+len);
                    }
                    ChangeTag::Insert => {
                        doc.insert(agent_id, pos, str);
                        pos += len;
                    }
                }
            }

            // TODO: Do that atomic rename nonsense instead of just overwriting.
            let out_data = doc.oplog.encode(EncodeOptions::default());
            fs::write(&dt_filename, out_data)?;
        }
    }
    // dbg!(&cli);
    Ok(())
}

fn random_agent_name() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

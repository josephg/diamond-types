use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use git2::{Oid, Repository};
use git2::ObjectType::Blob;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;

use diamond_types_positional::list::*;
use diamond_types_positional::list::encoding::EncodeOptions;
use diamond_types_positional::list::list::*;

fn main() -> Result<(), Box<dyn Error>> {
    let repo = Repository::open("/home/seph/3rdparty/node")?;
    let file = "src/node.cc";
    // let file = "Makefile";

    // let repo = Repository::open("/home/seph/3rdparty/yjs")?;
    // let file = "package.json";
    // let file = "y.js";

    // let repo = Repository::open("/home/seph/temp/g")?;
    // let file = "foo";

    let path = Path::new(file);

    println!("Loading {:?} from {:?}", path, repo.path());

    let head = repo.head().unwrap();
    // let y = head.resolve().unwrap();
    // dbg!(&head.name(), head.target());

    let c = head.peel_to_commit().unwrap();
    // dbg!(c.id());
    // let c_t = c.tree().unwrap();

    let mut commits_seen = HashSet::new();

    // The commits in reverse order
    let mut commits_rev = Vec::new();

    let mut frontier = Vec::new();
    // commits.insert(c.id());
    frontier.push(c.id());

    while let Some(c) = frontier.pop() {
        if commits_seen.contains(&c) { continue; }
        commits_seen.insert(c);
        commits_rev.push(c);

        // println!("Scanning {:?}", c);

        let c = repo.find_commit(c)?;

        for p in c.parents() {
            let p_id = p.id();
            // dbg!(&p_id);
            frontier.push(p_id);
        }
    }

    let mut oplog = OpLog::new();
    // let empty_branch = Branch::new();
    let mut branch_at_oid = HashMap::<Oid, Branch>::new();

    // This time through I'm taking each item *out* of the commits hashset when we process it.
    let mut commits_not_processed = commits_seen;

    // let mut branch_vec = Vec::new();

    // This is n^2 but whatever. This is essentially a script for generating test data. We only
    // need to run it once.
    while !commits_not_processed.is_empty() {
        // Scan from the back of commits_rev looking for something where we've done all the parents
        // but we haven't done this item.
        'outer: for commit_id in commits_rev.iter().rev() {
            if !commits_not_processed.contains(commit_id) { continue; }
            let commit = repo.find_commit(*commit_id)?;

            // dbg!(commit_id, commit.parents().map(|p| p.id()).collect::<Vec<_>>());

            let mut branch = if commit.parent_count() == 0 {
                // The branch is fresh at ROOT.
                Branch::new()
                // Cow::Owned(Branch::new())
                // (branch_vec.push(Branch::new()), branch_vec.len())
            } else {
                for p in commit.parents() {
                    // We want to have handled all the parents
                    let id = p.id();
                    if commits_not_processed.contains(&id) { continue 'outer; }
                }

                // Go through again and make a branch here.
                let mut iter = commit.parents();
                let first_parent = iter.next().unwrap();
                let mut branch = branch_at_oid[&first_parent.id()].clone();
                // let mut branch: Cow<Branch> = Cow::Borrowed(&branch_at_oid[&first_parent.id()]);
                // let idx = branch_at_oid[&first_parent.id()];

                for p in iter {
                    let frontier = &branch_at_oid[&p.id()].frontier;
                    branch.merge(&oplog, frontier);
                }

                branch
            };


            let tree = commit.tree()?;

            // if let Some(entry) = tree.get_name(file) {
            if let Ok(entry) = tree.get_path(path) {
                // dbg!(&entry.name(), entry.kind());
                if entry.kind() == Some(Blob) {
                    // println!("Processing {:?} at frontier {:?}", commit_id, &branch.frontier);
                    let obj = entry.to_object(&repo)?;
                    let blob = obj.as_blob().unwrap();
                    let new = std::str::from_utf8(blob.content())?;

                    if branch.content != new {
                        // branch.to_owned();
                        let sig = commit.author();
                        let author = sig.name().unwrap_or("unknown");
                        let agent = oplog.get_or_create_agent_id(author);

                        let branch_string = branch.content.to_string();
                        let old = branch_string.as_str();
                        let diff = TextDiff::from_chars(old, new);
                        // I could just consume diff.ops() directly here - but that would be awkward
                        // without the string utilities.
                        // dbg!(diff.ops());

                        let remapper = TextDiffRemapper::from_text_diff(&diff, old, new);
                        // .collect::<Vec<_>>();
                        // dbg!(changes);
                        // for change in diff.iter

                        let mut pos = 0;
                        for (tag, str) in diff.ops().iter()
                            .flat_map(move |x| remapper.iter_slices(x)) {
                            // dbg!(tag, str);
                            let len = str.chars().count();
                            // dbg!((tag, str, len));
                            match tag {
                                ChangeTag::Equal => pos += len,
                                ChangeTag::Delete => {
                                    let op = branch.make_delete_op(pos, len);
                                    apply_local_operation(&mut oplog, &mut branch, agent, &[op]);
                                    // local_delete(&mut oplog, &mut branch, agent, pos, len);
                                }
                                ChangeTag::Insert => {
                                    local_insert(&mut oplog, &mut branch, agent, pos, str);
                                    pos += len;
                                }
                            }
                        }

                        assert_eq!(&branch.content, new);
                        // println!("branch '{}' -> '{}'", old, branch.content);
                    } else {
                        // println!("Branch content matches expected: '{}'", branch.content);
                    }
                }
            }

            branch_at_oid.insert(*commit_id, branch);

            commits_not_processed.remove(commit_id);
        }
    }

    // dbg!(&oplog);
    let branch = Branch::new_at_tip(&oplog);
    // println!("{}: '{}'", file, branch.content);
    println!("Branch at {:?}", branch.frontier);

    // dbg!(&oplog.history.entries.len());

    let data = oplog.encode(EncodeOptions {
        store_inserted_content: true,
        store_deleted_content: false,
        verbose: true
    });
    std::fs::write("data.dt", data.as_slice()).unwrap();
    println!("{} bytes written to 'data.dt'", data.len());

    let data_old = oplog.encode_old(EncodeOptions::default());
    println!("(vs {} bytes)", data_old.len());

    // oplog.make_time_dag_graph("makefile.svg");

    Ok(())
}

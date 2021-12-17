use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use git2::{Oid, Repository};
use git2::ObjectType::Blob;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;
use smallvec::SmallVec;

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

    let mut scan_frontier = Vec::new();
    let mut fwd_frontier = Vec::new();

    // Could wrap this stuff up in a struct or something, but its not a big deal.
    let mut commit_children = HashMap::<Oid, SmallVec<[Oid; 3]>>::new();
    let mut commit_parents = HashMap::<Oid, SmallVec<[Oid; 3]>>::new();

    scan_frontier.push(c.id());
    commit_children.insert(c.id(), c.parents().map(|p| p.id()).collect());

    while let Some(c_id) = scan_frontier.pop() {
        if commits_seen.contains(&c_id) { continue; }
        commits_seen.insert(c_id);
        commits_rev.push(c_id);

        // println!("Scanning {:?}", c);

        let commit = repo.find_commit(c_id)?;

        commit_parents.insert(c_id, commit.parents().map(|p| p.id()).collect());
        for p in commit.parents() {
            let p_id = p.id();
            // dbg!(&p_id);
            scan_frontier.push(p_id);

            commit_children.entry(p_id).or_insert_with(|| SmallVec::new())
                .push(c_id);
        }

        if commit.parent_count() == 0 {
            fwd_frontier.push(commit.id());
        }
    }

    drop(scan_frontier);

    let mut oplog = OpLog::new();
    // let empty_branch = Branch::new();
    let mut branch_at_oid = HashMap::<Oid, Branch>::new();

    // This time through I'm taking each item *out* of the commits hashset when we process it.
    let mut commits_not_processed = commits_seen;

    // let mut branch_vec = Vec::new();

    while let Some(commit_id) = fwd_frontier.pop() {
        // println!("Pop {:?}. ({} remaining)", commit_id, fwd_frontier.len());
        // For something to enter fwd_frontier we must have processed all of its parents.
        let commit = repo.find_commit(commit_id)?;

        let mut branch = if commit.parent_count() == 0 {
            // The branch is fresh at ROOT.
            Branch::new()
        } else {
            for p in commit.parents() {
                // We want to have handled all the parents
                let id = p.id();
                if commits_not_processed.contains(&id) { panic!("Parent not processed!") }
            }

            // Go through again and make a branch here.
            let mut iter = commit.parents();
            let first_parent = iter.next().unwrap();
            let mut branch = branch_at_oid[&first_parent.id()].clone();

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

        branch_at_oid.insert(commit_id, branch);
        commits_not_processed.remove(&commit_id);

        // Go through all the children. Add any child which has all its dependencies met to the
        // frontier set.
        let children = commit_children.get(&commit_id).unwrap();
        for c in children {
            if commits_not_processed.contains(c) {
                let processed_all = commit_parents.get(c).unwrap().iter()
                    .all(|p_id| !commits_not_processed.contains(p_id));
                if processed_all {
                    // println!("Adding {:?} to children", c);
                    fwd_frontier.push(*c);
                }
            }
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

    let data_old = oplog.encode_simple(EncodeOptions::default());
    println!("(vs {} bytes)", data_old.len());

    // oplog.make_time_dag_graph("makefile.svg");

    Ok(())
}

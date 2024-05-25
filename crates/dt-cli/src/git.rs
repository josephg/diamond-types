//! This contains the code to extract changes from git repositories and convert them into diamond
//! types documents. This mostly exists to generate testing / benchmarking data.

// #![allow(unused_imports)]

use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use anyhow::Context;
use git2::{BranchType, Commit, Oid, Repository};
use git2::ObjectType::Blob;
use similar::{ChangeTag, TextDiff};
use similar::utils::TextDiffRemapper;
use smallvec::{SmallVec, smallvec};
use indicatif::ProgressBar;
use std::io::{BufWriter, Write};

use diamond_types::list::*;

/// In the git repository for linux, there are commits (maybe just one commit?) with the same commit
/// named twice in the parents list. Its this commit: 13e652800d1644dfedcd0d59ac95ef0beb7f3165
///
/// This iterator through the parents of a commit is inefficient (its O(n^2)) but its fine because of
/// how small the parents list is for commits.
///
/// I'd just wrap ParentsIdxs from git2 but its not exported :(
pub struct UniqParentIds<'commit> {
    range: Range<usize>,
    commit: &'commit Commit<'commit>,
}

impl<'commit> Iterator for UniqParentIds<'commit> {
    type Item = Oid;
    fn next(&mut self) -> Option<Oid> {
        'b: while let Some(i) = self.range.next() {
            let id = self.commit.parent_id(i).ok()?;

            // Make sure it hasn't already been emitted.
            for ii in 0..i {
                if self.commit.parent_id(ii).ok() == Some(id) {
                    continue 'b;
                }
            }

            return Some(id);
        }

        None
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<'commit> UniqParentIds<'commit> {
    fn new(commit: &'commit Commit) -> Self {
        Self { range: 0..commit.parent_count(), commit }
    }
}

pub fn extract_from_git(mut input_path: PathBuf, branch: Option<String>, quiet: bool, map_out: Option<PathBuf>) -> anyhow::Result<ListOpLog> {
    // let mut args: Args = argh::from_env();

    if input_path.is_relative() {
        input_path = std::env::current_dir()?.join(input_path);
    }
    assert!(input_path.is_absolute());

    let mut repo_path = Repository::discover_path(&input_path, &[] as &[&PathBuf])?;
    // assert!(input_path.starts_with(&repo_path));

    // dbg!(&repo_path);
    if repo_path.ends_with(".git") {
        repo_path = repo_path.parent().unwrap().to_path_buf();
    }
    // dbg!(&input_path, &repo_path);
    let file_path = input_path.strip_prefix(&repo_path)?;

    // dbg!(&repo_path, &file_path);

    let repo = Repository::open(&repo_path)?;
    let file = file_path;
    let branch = branch.unwrap_or_else(|| "master".into());

    let path = Path::new(&file);

    if !quiet { println!("Loading {:?} from {:?}", path, repo.path()); }

    // let head = repo.head().unwrap();
    let head = repo.find_branch(&branch, BranchType::Local).unwrap().into_reference();

    let mut scan_frontier = Vec::new();
    let mut fwd_frontier = Vec::new();

    // Could wrap this stuff up in a struct or something, but its not a big deal.
    // let mut commits_seen = HashSet::new();
    let mut commit_children = HashMap::<Oid, SmallVec<Oid, 3>>::new();
    let mut commit_parents = HashMap::<Oid, SmallVec<Oid, 3>>::new();

    // (parents, children).
    // let mut commit_info = HashMap::<Oid, (SmallVec<[Oid; 3]>, SmallVec<[Oid; 3]>)>::new();

    let c = head.peel_to_commit().unwrap();
    scan_frontier.push(c.id());
    // Mark the final change as having no children.
    commit_children.insert(c.id(), smallvec![]);

    let start = std::time::SystemTime::now();

    if !quiet { println!("Scanning frontier..."); }
    while let Some(c_id) = scan_frontier.pop() {
        // println!("cc: {} / cp: {} / sf {} / ff {}", commit_children.len(), commit_parents.len(), scan_frontier.len(), fwd_frontier.len());
        if commit_parents.contains_key(&c_id) { continue; }

        // println!("Scanning {:?}", c);

        let commit = repo.find_commit(c_id)?;

        commit_parents.insert(c_id, UniqParentIds::new(&commit).collect());
        for p_id in UniqParentIds::new(&commit) {
            scan_frontier.push(p_id);

            commit_children.entry(p_id).or_insert_with(|| SmallVec::new())
                .push(c_id);
        }

        if commit.parent_count() == 0 {
            fwd_frontier.push(commit.id());
        }
    }

    drop(scan_frontier);

    let scan_commits_time = std::time::SystemTime::now();

    if !quiet { println!("Scanning commits..."); }
    let mut oplog = ListOpLog::new();
    // let empty_branch = Branch::new();

    // (DT branch, Oid of the file in git its current state, number of remaining children.)
    let mut branch_at_oid = HashMap::<Oid, (ListBranch, Oid, usize)>::new();
    // let mut branch_at_oid = HashMap::<Oid, ListBranch>::new();

    // Unwrap is lazy here, but kinda fine.
    let mut map_file = map_out.map(|map_path| BufWriter::new(File::create(map_path).unwrap()));

    let mut git_bytes_read = 0;

    let take = |branch_at_oid: &mut HashMap::<Oid, (ListBranch, Oid, usize)>, p_id: Oid| -> (ListBranch, Oid) {
        let (branch_here, oid, num_children) = branch_at_oid.get_mut(&p_id)
            .with_context(|| format!("When looking up OID {}", p_id))
            .unwrap();

        debug_assert!(*num_children >= 1);
        if *num_children == 1 {
            let (branch, oid, _) = branch_at_oid.remove(&p_id).unwrap();
            (branch, oid)
        } else {
            *num_children -= 1;
            (branch_here.clone(), *oid)
        }
    };

    let take_branch = |branch_at_oid: &mut HashMap::<Oid, (ListBranch, Oid, usize)>, oplog: &ListOpLog, commit: &Commit| -> (ListBranch, Option<Oid>) {
        if commit.parent_count() == 0 {
            // The branch is fresh at ROOT.
            (ListBranch::new(), None)
        } else {
            // So we need 2 things:
            // - A starting branch
            // - The desired version (which is the commit version, converted to a DT frontier).

            // TODO: This code (alternately) takes the first branch which has no other children, but
            // it ends up slower in practice because cloning a branch is cheap, but scanning git
            // commits is expensive. Go figure!

            // let mut branch = None;
            // let mut frontier: SmallVec<[LV; 2]> = smallvec![];
            // for p_id in commit.parent_ids() {
            //     let (branch_here, num_children) = branch_at_oid.get_mut(&p_id).unwrap();
            //
            //     frontier.extend_from_slice(branch_here.local_frontier_ref());
            //
            //     debug_assert!(*num_children > 0);
            //     if *num_children == 1 {
            //         let (branch_here, _) = branch_at_oid.remove(&p_id).unwrap();
            //         if branch.is_none() {
            //             branch = Some(branch_here);
            //         }
            //     } else {
            //         *num_children -= 1;
            //     }
            // }
            //
            // // The frontier might contain repeated elements. Simplify!
            // frontier.sort_unstable();
            // let merge_frontier = oplog.cg.graph.find_dominators(&frontier);
            //
            // let mut branch = branch.unwrap_or_else(|| {
            //     // We might not have found any branch with no parents.
            //     let p_id = commit.parent_id(0).unwrap();
            //     branch_at_oid[&p_id].0.clone()
            // });
            //
            // branch.merge(&oplog, merge_frontier.as_ref());
            // branch

            // Go through again and make a branch here.
            let mut iter = commit_parents[&commit.id()].iter().copied();
            // let mut iter = UniqParentIds::new(commit);
            let first_parent = iter.next().unwrap();
            let (mut branch, oid) = take(branch_at_oid, first_parent);
            let mut oid = Some(oid);

            for p in iter {
                let (child_branch, child_oid) = take(branch_at_oid, p);
                let child_frontier = child_branch.local_frontier_ref();

                // This is a microoptimization. It makes some traces a little faster.
                if oplog.cg.graph.frontier_contains_frontier(child_frontier, branch.local_frontier_ref()) {
                    // Well, just use the child branch then.
                    branch = child_branch;
                    oid = Some(child_oid);
                } else if !oplog.cg.graph.frontier_contains_frontier(branch.local_frontier_ref(), child_frontier) {
                    // They're concurrent.
                    branch.merge(&oplog, child_frontier);
                    oid = None;
                }
            }

            (branch, oid)
        }
    };

    // let mut log = std::io::BufWriter::new(std::fs::File::create("git-reader.log").unwrap());

    let bar = if quiet {
        ProgressBar::hidden()
    } else {
        ProgressBar::new(commit_parents.len() as _)
    };

    // let mut i = 0;
    while let Some(commit_id) = fwd_frontier.pop() {
        bar.inc(1);

        // if i % 1000 == 0 { println!("{i}..."); }
        // i += 1;

        // write!(log, "Pop {:?}. ({} remaining)\n", commit_id, fwd_frontier.len()).unwrap();
        // println!("Pop {:?}. ({} remaining) (bao: {})", commit_id, fwd_frontier.len(), branch_at_oid.len());

        // For something to enter fwd_frontier we must have processed all of its parents.
        let commit = repo.find_commit(commit_id)?;

        let (mut branch, stored_oid) = take_branch(&mut branch_at_oid, &oplog, &commit);

        let tree = commit.tree()?;

        // if let Some(entry) = tree.get_name(file) {
        let oid_here = if let Ok(entry) = tree.get_path(path) {
            let oid_here = entry.id();
            // dbg!(&entry.name(), entry.kind());
            if stored_oid != Some(oid_here) && entry.kind() == Some(Blob) {
                // println!("Processing {:?} at frontier {:?}", commit_id, &branch.frontier);
                let obj = entry.to_object(&repo)?;
                let blob = obj.as_blob().unwrap();

                let new = String::from_utf8_lossy(blob.content());

                if branch.content() != &new {
                    git_bytes_read += new.len();
                    let sig = commit.author();
                    let mut author = sig.name().unwrap_or("unknown");

                    // Diamond types only allows agent IDs up to 50 bytes long. We'll trim the
                    // name down to 30 bytes, just to be on the safe side.
                    if author.len() > 30 {
                        let mut end = 30;
                        // Make sure we cut at a unicode-safe boundary.
                        while !author.is_char_boundary(end) { end -= 1; }
                        author = &author[..end];
                    }
                    let agent = oplog.get_or_create_agent_id(author);

                    let branch_string = branch.content().to_string();
                    let old = branch_string.as_str();
                    let diff = TextDiff::from_chars(old, &new);
                    // I could just consume diff.ops() directly here - but that would be awkward
                    // without the string utilities.
                    // dbg!(diff.ops());

                    let remapper = TextDiffRemapper::from_text_diff(&diff, old, &new);
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
                                branch.delete(&mut oplog, agent, pos .. pos+len);

                                // let op = branch.make_delete_op(pos, len);
                                // apply_local_operation(&mut oplog, &mut branch, agent, &[op]);
                            }
                            ChangeTag::Insert => {
                                branch.insert(&mut oplog, agent, pos, str);
                                // local_insert(&mut oplog, &mut branch, agent, pos, str);
                                pos += len;
                            }
                        }
                    }

                    assert_eq!(branch.content(), &new);
                    // println!("branch '{}' -> '{}'", old, branch.content);

                    if let Some(map_file) = map_file.as_mut() {
                        let frontier = branch.local_frontier_ref();
                        let rv = oplog.cg.agent_assignment.local_to_remote_frontier(frontier);
                        writeln!(map_file, "{},{}",
                            commit.id(),
                            serde_json::to_string(&rv).unwrap()
                        )?;
                    }
                }
            }
            oid_here
        } else { stored_oid.unwrap_or(commit_id) }; // the commit ID here is pointless but eh.

        let children = commit_children.get(&commit_id).unwrap();
        branch_at_oid.insert(commit_id, (branch, oid_here, children.len()));

        // Go through all the children. Add any child which has all its dependencies met to the
        // frontier set.
        for c in children {
            if !branch_at_oid.contains_key(c) {
                let processed_all = commit_parents[c].iter()
                    .all(|p_id| branch_at_oid.contains_key(p_id));
                if processed_all {
                    // println!("Adding {:?} to children", c);
                    fwd_frontier.push(*c);
                }
            }
        }
    }
    bar.finish();

    let end_time = std::time::SystemTime::now();

    // dbg!(&oplog);
    // let branch = ListBranch::new_at_tip(&oplog);
    // println!("{}: '{}'", file, branch.content);
    // println!("Branch at {:?}", branch.local_frontier_ref());

    // dbg!(&oplog.history.entries.len());
    // println!("Number of entries in history: {}", &oplog.history.num_entries());

    // let data = oplog.encode(ENCODE_FULL);
    // std::fs::write("data.dt", data.as_slice()).unwrap();
    // println!("{} bytes written to 'data.dt'", data.len());

    // let data_old = oplog.encode_simple(EncodeOptions::default());
    // println!("(vs {} bytes)", data_old.len());

    // oplog.make_time_dag_graph("git-makefile.svg");

    if !quiet {
        let pass_1_dur = scan_commits_time.duration_since(start).unwrap();
        let pass_2_dur = end_time.duration_since(scan_commits_time).unwrap();
        let total_dur = end_time.duration_since(start).unwrap();
        println!("Time for first pass: {:?} / scan commits: {:?} / total: {:?}", pass_1_dur, pass_2_dur, total_dur);

        println!("Read {} bytes of content from git commits", git_bytes_read);
    }

    Ok(oplog)
}

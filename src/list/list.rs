use std::mem::replace;
use std::ops::Range;
use humansize::{file_size_opts, FileSize};
use crate::list::{Branch, ListCRDT, OpLog};
use smallvec::smallvec;
use crate::{AgentId, LocalVersion, Time};
use rle::HasLength;
use crate::list::encoding::encode_tools::ParseError;
use crate::list::operation::OpKind::{Del, Ins};
use crate::list::operation::Operation;
use crate::dtrange::DTRange;

// For local changes to a branch, we take the checkout's frontier as the new parents list.
fn insert_history_local(oplog: &mut OpLog, frontier: &mut LocalVersion, range: DTRange) {
    // Fast path for local edits. For some reason the code below is remarkably non-performant.
    // My kingdom for https://rust-lang.github.io/rfcs/2497-if-let-chains.html
    if frontier.len() == 1 && frontier[0] == range.start.wrapping_sub(1) {
        if let Some(last) = oplog.history.entries.0.last_mut() {
            last.span.end = range.end;
            frontier[0] = range.last();
            return;
        }
    }

    // Otherwise use the slow version.
    let txn_parents = replace(frontier, smallvec![range.last()]);
    oplog.history.insert(&txn_parents, range);
}

// Slow / small version.
// pub fn apply_local_operation(oplog: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) -> Time {
//     let time = oplog.push(agent, local_ops);
//     branch.merge(oplog, &[time]);
//     time
// }

/// Most of the time when you have a local branch, you need to both append the new change to the
/// oplog and merge the new change into your local branch.
///
/// Doing both of these steps at the same time is much faster because we don't need to worry about
/// concurrent edits. (The origin position == transformed position for the change).
///
/// This method does that.
///
/// (I low key hate the duplicated code though.)
pub(crate) fn apply_local_operation(oplog: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) -> Time {
    let first_time = oplog.len();
    let mut next_time = first_time;

    // for LocalOp { pos, ins_content, del_span } in local_ops {
    for c in local_ops {
        let pos = c.loc.span.start;
        let len = c.len();

        match c.kind {
            Ins => {
                // assert!(c.);
                // let new_content = consume_chars(&mut content, len);
                branch.content.insert(pos, c.content.as_ref().unwrap());
            }

            Del => {
                branch.content.remove(pos..pos + len);
            }
        }

        // oplog.operations.push(KVPair(next_time, c.clone()));
        oplog.push_op_internal(next_time, c.loc, c.kind, c.content_as_str());
        next_time += len;
    }

    let span = DTRange {
        start: first_time,
        end: next_time
    };

    oplog.assign_next_time_to_client_known(agent, span);

    oplog.advance_frontier(&branch.version, span);
    insert_history_local(oplog, &mut branch.version, span);

    // oplog.version = smallvec![next_time - 1];

    next_time - 1
}

impl Default for ListCRDT {
    fn default() -> Self {
        Self::new()
    }
}

impl ListCRDT {
    pub fn new() -> Self {
        Self {
            branch: Branch::new(),
            oplog: OpLog::new()
        }
    }

    pub fn load_from(bytes: &[u8]) -> Result<Self, ParseError> {
        let oplog = OpLog::load_from(bytes)?;
        let branch = oplog.checkout_tip();
        Ok(Self {
            branch, oplog
        })
    }

    pub fn merge_data_and_ff(&mut self, bytes: &[u8]) -> Result<LocalVersion, ParseError> {
        let v = self.oplog.decode_and_add(bytes)?;
        self.branch.merge(&self.oplog, &self.oplog.version);
        Ok(v)
    }

    pub fn len(&self) -> usize {
        self.branch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.branch.is_empty()
    }

    pub fn apply_local_operations(&mut self, agent: AgentId, local_ops: &[Operation]) -> Time {
        apply_local_operation(&mut self.oplog, &mut self.branch, agent, local_ops)
    }

    pub fn insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        self.branch.insert(&mut self.oplog, agent, pos, ins_content)
    }

    // pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Time {
    //     local_delete(&mut self.oplog, &mut self.branch, agent, pos, del_span)
    // }

    pub fn delete_without_content(&mut self, agent: AgentId, loc: Range<usize>) -> Time {
        self.branch.delete_without_content(&mut self.oplog, agent, loc)
    }

    pub fn delete(&mut self, agent: AgentId, range: Range<usize>) -> Time {
        self.branch.delete(&mut self.oplog, agent, range)
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.branch.len());

        println!("Content memory size: {}", self.branch.content.mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("(Efficient size: {})", self.branch.content.len_bytes().file_size(file_size_opts::CONVENTIONAL).unwrap());

        self.oplog.print_stats(detailed);
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.oplog.get_or_create_agent_id(name)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.insert(0, 0, "hi".into());
        doc.insert(0, 1, "yooo".into());
        // "hyoooi"
        assert_eq!(doc.branch.content, "hyoooi");
        doc.delete(0, 1..4);
        assert_eq!(doc.branch.content, "hoi");

        doc.dbg_check(true);
        // dbg!(doc);

        doc.oplog.dbg_print_all();
    }
}
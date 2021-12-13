use std::mem::replace;
use humansize::{file_size_opts, FileSize};
use crate::list::{Branch, Frontier, ListCRDT, OpLog, Time};
use crate::rle::KVPair;
use smallvec::smallvec;
use crate::AgentId;
use rle::HasLength;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::TimeSpan;
use crate::remotespan::CRDTId;

// For local changes to a branch, we take the checkout's frontier as the new parents list.
fn insert_history_local(opset: &mut OpLog, frontier: &mut Frontier, range: TimeSpan) {
    // Fast path for local edits. For some reason the code below is remarkably non-performant.
    // My kingdom for https://rust-lang.github.io/rfcs/2497-if-let-chains.html
    if frontier.len() == 1 && frontier[0] == range.start.wrapping_sub(1) {
        if let Some(last) = opset.history.entries.0.last_mut() {
            last.span.end = range.end;
            frontier[0] = range.last();
            return;
        }
    }

    // Otherwise use the slow version.
    let txn_parents = replace(frontier, smallvec![range.last()]);
    opset.insert_history(&txn_parents, range);
}

// Slow / small version.
// pub fn apply_local_operation(oplog: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) -> Time {
//     let time = oplog.push(agent, local_ops);
//     branch.merge(oplog, &[time]);
//     time
// }

/// This is an optimized version of simply pushing the operation to the oplog and then merging it.
///
/// It is much faster; but I hate the duplicated code.
pub fn apply_local_operation(oplog: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) -> Time {
    let first_time = oplog.len();
    let mut next_time = first_time;

    // for LocalOp { pos, ins_content, del_span } in local_ops {
    for c in local_ops {
        let pos = c.span.span.start;
        let len = c.len();

        match c.tag {
            Ins => {
                assert!(c.content_known);
                // let new_content = consume_chars(&mut content, len);
                branch.content.insert(pos, &c.content);
            }

            Del => {
                branch.content.remove(pos..pos + len);
            }
        }

        // oplog.operations.push(KVPair(next_time, c.clone()));
        oplog.push_op_internal(next_time, c.span, c.tag, if c.content_known {
            Some(&c.content)
        } else { None });
        next_time += len;
    }

    let span = TimeSpan {
        start: first_time,
        end: next_time
    };

    oplog.assign_next_time_to_client(agent, span);
    insert_history_local(oplog, &mut branch.frontier, span);

    oplog.frontier = smallvec![next_time - 1];
    next_time - 1
}

pub fn local_insert(opset: &mut OpLog, branch: &mut Branch, agent: AgentId, pos: usize, ins_content: &str) -> Time {
    apply_local_operation(opset, branch, agent, &[Operation::new_insert(pos, ins_content)])
}

pub fn local_delete(opset: &mut OpLog, branch: &mut Branch, agent: AgentId, pos: usize, del_span: usize) -> Time {
    apply_local_operation(opset, branch, agent, &[Operation::new_delete(pos, del_span)])
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
            ops: OpLog::new()
        }
    }

    pub fn len(&self) -> usize {
        self.branch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.branch.is_empty()
    }

    pub fn apply_local_operation(&mut self, agent: AgentId, local_ops: &[Operation]) -> Time {
        apply_local_operation(&mut self.ops, &mut self.branch, agent, local_ops)
    }

    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        local_insert(&mut self.ops, &mut self.branch, agent, pos, ins_content)
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Time {
        local_delete(&mut self.ops, &mut self.branch, agent, pos, del_span)
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.branch.len());

        println!("Content memory size: {}", self.branch.content.mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("(Efficient size: {})", self.branch.content.len_bytes().file_size(file_size_opts::CONVENTIONAL).unwrap());

        self.ops.print_stats(detailed);
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.ops.get_or_create_agent_id(name)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.local_insert(0, 0, "hi".into());
        doc.local_insert(0, 1, "yooo".into());
        // "hyoooi"
        assert_eq!(doc.branch.content, "hyoooi");
        doc.local_delete(0, 1, 3);
        assert_eq!(doc.branch.content, "hoi");

        doc.check(true);
        // dbg!(doc);

        doc.ops.dbg_print_all();
    }
}
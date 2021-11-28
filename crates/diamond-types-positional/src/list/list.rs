use std::mem::replace;
use humansize::{file_size_opts, FileSize};
use crate::list::{Branch, Frontier, ListCRDT, OpLog};
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

pub fn apply_local_operation(opset: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) {
    let first_time = opset.len();
    let mut next_time = first_time;

    let op_len = local_ops.iter().map(|c| c.len()).sum();

    opset.assign_time_to_client(CRDTId {
        agent,
        seq: opset.client_data[agent as usize].get_next_seq()
    }, first_time, op_len);

    // for LocalOp { pos, ins_content, del_span } in local_ops {
    for c in local_ops {
        let pos = c.pos as usize;
        let len = c.len() as usize;

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

        opset.operations.push(KVPair(next_time, c.clone()));
        next_time += len;
    }

    insert_history_local(opset, &mut branch.frontier, TimeSpan {
        start: first_time,
        end: first_time + op_len
    });
}

pub fn local_insert(opset: &mut OpLog, branch: &mut Branch, agent: AgentId, pos: usize, ins_content: &str) {
    apply_local_operation(opset, branch, agent, &[Operation::new_insert(pos, ins_content)]);
}

pub fn local_delete(opset: &mut OpLog, branch: &mut Branch, agent: AgentId, pos: usize, del_span: usize) {
    apply_local_operation(opset, branch, agent, &[Operation::new_delete(pos, del_span)]);
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

    pub fn apply_local_operation(&mut self, agent: AgentId, local_ops: &[Operation]) {
        apply_local_operation(&mut self.ops, &mut self.branch, agent, local_ops);
    }

    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) {
        local_insert(&mut self.ops, &mut self.branch, agent, pos, ins_content);
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) {
        local_delete(&mut self.ops, &mut self.branch, agent, pos, del_span);
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.branch.len());

        println!("Content memory size: {}", self.branch.content.mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("(Efficient size: {})", self.branch.content.len_bytes().file_size(file_size_opts::CONVENTIONAL).unwrap());

        self.ops.operations.print_stats("Operations", detailed);

        // Get some stats on how operations are distributed
        let mut i_1 = 0;
        let mut d_1 = 0;
        let mut i_n = 0;
        let mut i_r = 0;
        let mut d_n = 0;
        let mut d_r = 0;
        for op in self.ops.operations.iter_merged() {
            match (op.1.len(), op.1.tag, op.1.reversed) {
                (1, InsDelTag::Ins, _) => { i_1 += 1; }
                (_, InsDelTag::Ins, false) => { i_n += 1; }
                (_, InsDelTag::Ins, true) => { i_r += 1; }

                (1, InsDelTag::Del, _) => { d_1 += 1; }
                (_, InsDelTag::Del, false) => { d_n += 1; }
                (_, InsDelTag::Del, true) => { d_r += 1; }
            }
        }
        // These stats might make more sense as percentages.
        println!("ins: singles {}, fwd {}, rev {}", i_1, i_n, i_r);
        println!("del: singles {}, fwd {}, rev {}", d_1, d_n, d_r);

        self.ops.client_with_localtime.print_stats("Client localtime map", detailed);
        self.ops.history.entries.print_stats("History", detailed);
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
        dbg!(doc);
    }
}
use std::mem::replace;
use humansize::{file_size_opts, FileSize};
use jumprope::JumpRope;
use crate::list::{Checkout, ClientData, Frontier, ListCRDT, OpSet, Time};
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
use smallvec::smallvec;
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use smartstring::alias::{String as SmartString};
use rle::{HasLength, MergableSpan, Searchable};
use crate::list::branch::branch_eq;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::list::operation::{InsDelTag, PositionalComponent, PositionalOp};
use crate::list::history::HistoryEntry;
use crate::localtime::TimeSpan;
use crate::remotespan::{CRDT_DOC_ROOT, CRDTId, CRDTSpan};
use crate::unicount::{consume_chars, count_chars};

// For local changes to a branch, we take the checkout's frontier as the new parents list.
fn insert_history_local(opset: &mut OpSet, frontier: &mut Frontier, range: TimeSpan) {
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

pub fn apply_local_operation(opset: &mut OpSet, checkout: &mut Checkout, agent: AgentId, local_ops: &[PositionalComponent], mut content: &str) {
    let first_time = opset.get_next_time();
    let mut next_time = first_time;

    let op_len = local_ops.iter().map(|c| c.len).sum();

    opset.assign_time_to_client(CRDTId {
        agent,
        seq: opset.client_data[agent as usize].get_next_seq()
    }, first_time, op_len);

    // for LocalOp { pos, ins_content, del_span } in local_ops {
    for c in local_ops {
        let pos = c.pos as usize;
        let len = c.len as usize;

        match c.tag {
            Ins => {
                assert!(c.content_known);
                let new_content = consume_chars(&mut content, len);
                checkout.content.insert(pos, new_content);
            }

            Del => {
                checkout.content.remove(pos..pos + len);
            }
        }

        opset.operations.push(KVPair(next_time, c.clone()));
        next_time += len;
    }

    insert_history_local(opset, &mut checkout.frontier, TimeSpan {
        start: first_time,
        end: first_time + op_len
    });
}

pub fn local_insert(opset: &mut OpSet, checkout: &mut Checkout, agent: AgentId, pos: usize, ins_content: &str) {
    apply_local_operation(opset, checkout, agent, &[PositionalComponent {
        pos,
        len: count_chars(ins_content),
        rev: false,
        content_known: true,
        tag: Ins
    }], ins_content);
}

pub fn local_delete(opset: &mut OpSet, checkout: &mut Checkout, agent: AgentId, pos: usize, del_span: usize) {
    apply_local_operation(opset, checkout, agent, &[PositionalComponent {
        pos, len: del_span, rev: false, content_known: true, tag: Del
    }], "")
}


impl ListCRDT {
    pub fn new() -> Self {
        Self {
            checkout: Checkout::new(),
            ops: OpSet::new()
        }
    }

    pub fn len(&self) -> usize {
        self.checkout.len()
    }

    pub fn apply_local_operation(&mut self, agent: AgentId, local_ops: &[PositionalComponent], mut content: &str) {
        apply_local_operation(&mut self.ops, &mut self.checkout, agent, local_ops, content);
    }

    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) {
        local_insert(&mut self.ops, &mut self.checkout, agent, pos, ins_content);
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) {
        local_delete(&mut self.ops, &mut self.checkout, agent, pos, del_span);
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.checkout.len());

        println!("Content memory size: {}", self.checkout.content.mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
        println!("(Efficient size: {})", self.checkout.content.len_bytes().file_size(file_size_opts::CONVENTIONAL).unwrap());

        self.ops.operations.print_stats("Operations", detailed);

        // Get some stats on how operations are distributed
        let mut i_1 = 0;
        let mut d_1 = 0;
        let mut i_n = 0;
        let mut i_r = 0;
        let mut d_n = 0;
        let mut d_r = 0;
        for op in self.ops.operations.iter_merged() {
            match (op.1.len, op.1.tag, op.1.rev) {
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
        assert_eq!(doc.checkout.content, "hyoooi");
        doc.local_delete(0, 1, 3);
        assert_eq!(doc.checkout.content, "hoi");

        doc.check(true);
        dbg!(doc);
    }
}
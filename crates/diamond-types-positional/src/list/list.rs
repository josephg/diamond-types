use std::mem::replace;
use humansize::{file_size_opts, FileSize};
use jumprope::JumpRope;
use crate::list::{ClientData, ListCRDT, Time};
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

impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        if let Some(last) = self.item_orders.last() {
            last.end()
        } else { 0 }
    }

    pub fn seq_to_time(&self, seq: usize) -> Time {
        let (entry, offset) = self.item_orders.find_with_offset(seq).unwrap();
        entry.1.start + offset
    }

    /// Note the returned timespan might be shorter than seq_range.
    pub fn seq_to_time_span(&self, seq_range: TimeSpan) -> TimeSpan {
        let (entry, offset) = self.item_orders.find_with_offset(seq_range.start).unwrap();

        let start = entry.1.start + offset;
        let end = usize::min(entry.1.end, start + seq_range.len());
        TimeSpan { start, end }
    }

    // pub fn seq_to_time_span_old(&self, seq: usize, max_len: usize) -> TimeSpan {
    //     let (entry, offset) = self.item_orders.find_with_offset(seq).unwrap();
    //     let start = entry.1.start + offset;
    //     let len = max_len.min(entry.1.start + entry.1.end - offset);
    //     TimeSpan {
    //         start, end: start + len
    //     }
    // }
}

impl ListCRDT {
    pub fn new() -> Self {
        Self {
            client_with_localtime: RleVec::new(),
            client_data: vec![],
            operations: Default::default(),
            frontier: smallvec![ROOT_TIME],
            history: Default::default(),
            text_content: Some(JumpRope::new()),
        }
    }

    pub fn has_content(&self) -> bool {
        self.text_content.is_some()
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        // Probably a nicer way to write this.
        if name == "ROOT" { return ROOT_AGENT; }

        if let Some(id) = self.get_agent_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: SmartString::from(name),
                item_orders: RleVec::new()
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    pub(crate) fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(ROOT_AGENT) }
        else {
            self.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    pub(crate) fn get_agent_name(&self, agent: AgentId) -> &str {
        self.client_data[agent as usize].name.as_str()
    }

    pub(crate) fn get_crdt_location(&self, time: usize) -> CRDTId {
        if time == ROOT_TIME { CRDT_DOC_ROOT }
        else {
            let (loc, offset) = self.client_with_localtime.find_packed_with_offset(time);
            loc.1.at_offset(offset as usize)
        }
    }

    pub(crate) fn get_crdt_span(&self, time: TimeSpan) -> CRDTSpan {
        if time.start == ROOT_TIME { CRDTSpan { agent: ROOT_AGENT, seq_range: Default::default() } }
        else {
            let (loc, offset) = self.client_with_localtime.find_packed_with_offset(time.start);
            let start = loc.1.seq_range.start + offset;
            let end = usize::min(loc.1.seq_range.end, start + time.len());
            CRDTSpan {
                agent: loc.1.agent,
                seq_range: TimeSpan { start, end }
            }
        }
    }

    pub(crate) fn get_time(&self, loc: CRDTId) -> usize {
        if loc.agent == ROOT_AGENT { ROOT_TIME }
        else { self.client_data[loc.agent as usize].seq_to_time(loc.seq) }
    }

    // pub(crate) fn get_time_span(&self, loc: CRDTId, max_len: u32) -> OrderSpan {
    //     assert_ne!(loc.agent, ROOT_AGENT);
    //     self.client_data[loc.agent as usize].seq_to_order_span(loc.seq, max_len)
    // }

    pub fn get_next_time(&self) -> usize {
        if let Some(last) = self.client_with_localtime.last() {
            last.end()
        } else { 0 }
    }

    fn assign_time_to_client(&mut self, loc: CRDTId, time_start: usize, len: usize) {
        self.client_with_localtime.push(KVPair(time_start, CRDTSpan {
            agent: loc.agent,
            seq_range: TimeSpan { start: loc.seq, end: loc.seq + len },
        }));

        self.client_data[loc.agent as usize].item_orders.push(KVPair(loc.seq, TimeSpan {
            start: time_start,
            end: time_start + len,
        }));
    }

    // For local changes, where we just take the frontier as the new parents list.
    fn insert_history_local(&mut self, range: TimeSpan) {
        // Fast path for local edits. For some reason the code below is remarkably non-performant.
        // My kingdom for https://rust-lang.github.io/rfcs/2497-if-let-chains.html
        if self.frontier.len() == 1 && self.frontier[0] == range.start.wrapping_sub(1) {
            if let Some(last) = self.history.entries.0.last_mut() {
                last.span.end = range.end;
                self.frontier[0] = range.last();
                return;
            }
        }

        // Otherwise use the slow version.
        let txn_parents = replace(&mut self.frontier, smallvec![range.last()]);
        self.insert_history_internal(&txn_parents, range);
    }

    // fn insert_txn_remote(&mut self, txn_parents: &[Order], range: Range<Order>) {
    //     advance_branch_by_known(&mut self.frontier, &txn_parents, range.clone());
    //     self.insert_history_internal(txn_parents, range);
    // }

    fn insert_history_internal(&mut self, txn_parents: &[Time], range: TimeSpan) {
        // Fast path. The code below is weirdly slow, but most txns just append.
        if let Some(last) = self.history.entries.0.last_mut() {
            if txn_parents.len() == 1
                && txn_parents[0] == last.last_time()
                && last.span.can_append(&range)
            {
                last.span.append(range);
                return;
            }
        }

        // let parents = replace(&mut self.frontier, txn_parents);
        let mut shadow = range.start;
        while shadow >= 1 && txn_parents.contains(&(shadow - 1)) {
            shadow = self.history.entries.find(shadow - 1).unwrap().shadow;
        }
        if shadow == 0 { shadow = ROOT_TIME; }

        let will_merge = if let Some(last) = self.history.entries.last() {
            // TODO: Is this shadow check necessary?
            // This code is from TxnSpan splitablespan impl. Copying it here is a bit ugly but
            // its the least ugly way I could think to implement this.
            txn_parents.len() == 1 && txn_parents[0] == last.last_time() && shadow == last.shadow
        } else { false };

        let mut parent_indexes = smallvec![];
        if !will_merge {
            // The item wasn't merged. So we need to go through the parents and wire up children.
            let new_idx = self.history.entries.0.len();

            for &p in txn_parents {
                if p == ROOT_TIME {
                    self.history.root_child_indexes.push(new_idx);
                } else {
                    let parent_idx = self.history.entries.find_index(p).unwrap();
                    // Interestingly the parent_idx array will always end up the same length as parents
                    // because it would be invalid for multiple parents to point to the same entry in
                    // txns. (That would imply one parent is a descendant of another.)
                    debug_assert!(!parent_indexes.contains(&parent_idx));
                    parent_indexes.push(parent_idx);

                    let parent_children = &mut self.history.entries.0[parent_idx].child_indexes;
                    if !parent_children.contains(&new_idx) {
                        parent_children.push(new_idx);

                        // This is a tiny optimization for txn_trace. We store the child_indexes in
                        // order of their first parent - which will usually be the order in which we
                        // want to iterate them.
                        // TODO: Make this work and benchmark.
                        // if parent_children.len() > 1 {
                        //     parent_children.sort_unstable_by(|&a, &b| {
                        //         u32::cmp(&self.txns.0[a].parents[0].wrapping_add(1),
                        //                  &self.txns.0[b].parents[0].wrapping_add(1))
                        //     });
                        // }
                    }
                }

            }
        }

        let txn = HistoryEntry {
            span: range,
            shadow,
            parents: txn_parents.into_iter().copied().collect(),
            parent_indexes,
            child_indexes: smallvec![]
        };

        let did_merge = self.history.entries.push(txn);
        assert_eq!(will_merge, did_merge);
    }

    pub fn apply_local_operation(&mut self, agent: AgentId, local_ops: &[PositionalComponent], mut content: &str) {
        let first_time = self.get_next_time();
        let mut next_time = first_time;

        let op_len = local_ops.iter().map(|c| c.len).sum();

        self.assign_time_to_client(CRDTId {
            agent,
            seq: self.client_data[agent as usize].get_next_seq()
        }, first_time, op_len);

        // for LocalOp { pos, ins_content, del_span } in local_ops {
        for c in local_ops {
            let pos = c.pos as usize;
            let len = c.len as usize;

            match c.tag {
                Ins => {
                    assert!(c.content_known);
                    let new_content = consume_chars(&mut content, len);

                    if let Some(text) = self.text_content.as_mut() {
                        text.insert(pos, new_content);
                    }
                }

                Del => {
                    if let Some(ref mut text) = self.text_content {
                        text.remove(pos..pos + len);
                    }
                }
            }

            self.operations.push(KVPair(next_time, c.clone()));
            next_time += len;
        }

        self.insert_history_local(TimeSpan { start: first_time, end: first_time + op_len });
    }

    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) {
        self.apply_local_operation(agent, &[PositionalComponent {
            pos,
            len: count_chars(ins_content),
            rev: false,
            content_known: true,
            tag: Ins
        }], ins_content);
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) {
        self.apply_local_operation(agent, &[PositionalComponent {
            pos, len: del_span, rev: false, content_known: true, tag: Del
        }], "")
    }

    pub fn append_remote_operation(&mut self, agent: AgentId, parents: &[Time], ops: &[PositionalComponent], mut content: &str) {
        let first_time = self.get_next_time();
        let mut next_time = first_time;

        let op_len = ops.iter().map(|c| c.len).sum();

        self.assign_time_to_client(CRDTId {
            agent,
            seq: self.client_data[agent as usize].get_next_seq()
        }, first_time, op_len);

        // for LocalOp { pos, ins_content, del_span } in local_ops {
        for c in ops {
            // let pos = c.pos as usize;
            let len = c.len as usize;

            // match c.tag {
            //     Ins => {
            //         assert!(c.content_known);
            //         let new_content = consume_chars(&mut content, len);
            //
            //         if let Some(text) = self.text_content.as_mut() {
            //             text.insert(pos, new_content);
            //         }
            //     }
            //
            //     Del => {
            //         if let Some(ref mut text) = self.text_content {
            //             text.remove(pos..pos + len);
            //         }
            //     }
            // }

            self.operations.push(KVPair(next_time, c.clone()));
            next_time += len;
        }

        self.insert_history_internal(parents, TimeSpan { start: first_time, end: first_time + op_len });
    }

    pub fn append_remote_insert(&mut self, agent: AgentId, parents: &[Time], pos: usize, ins_content: &str) {
        self.append_remote_operation(agent, parents, &[PositionalComponent {
            pos,
            len: count_chars(ins_content),
            rev: false,
            content_known: true,
            tag: Ins
        }], ins_content);
    }

    pub fn append_remote_delete(&mut self, agent: AgentId, parents: &[Time], pos: usize, del_span: usize) {
        self.append_remote_operation(agent, parents, &[PositionalComponent {
            pos, len: del_span, rev: false, content_known: true, tag: Del
        }], "")
    }

    pub fn len(&self) -> usize {
        if let Some(content) = &self.text_content {
            content.len_chars()
        } else {
            panic!("Cannot calculate length")
        }
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.len());

        if let Some(r) = &self.text_content {
            println!("Content memory size: {}", r.mem_size().file_size(file_size_opts::CONVENTIONAL).unwrap());
            println!("(Efficient size: {})", r.len_bytes().file_size(file_size_opts::CONVENTIONAL).unwrap());
        }

        self.operations.print_stats("Operations", detailed);

        // Get some stats on how operations are distributed
        let mut i_1 = 0;
        let mut d_1 = 0;
        let mut i_n = 0;
        let mut i_r = 0;
        let mut d_n = 0;
        let mut d_r = 0;
        for op in self.operations.iter_merged() {
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

        self.client_with_localtime.print_stats("Client localtime map", detailed);
        self.history.entries.print_stats("History", detailed);
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
        doc.dbg_assert_content_eq("hyoooi");
        doc.local_delete(0, 1, 3);
        doc.dbg_assert_content_eq("hoi");

        doc.check(true);
        dbg!(doc);
    }
}
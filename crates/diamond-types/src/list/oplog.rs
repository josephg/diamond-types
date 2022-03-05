use smallvec::smallvec;
use smartstring::SmartString;
use rle::{HasLength, MergableSpan, Searchable};
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use crate::list::{Branch, ClientData, OpLog, switch, Time};
use crate::list::frontier::advance_frontier_by_known_run;
use crate::list::history::{HistoryEntry, MinimalHistoryEntry};
use crate::list::internal_op::{OperationCtx, OperationInternal};
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::TimeSpan;
use crate::remotespan::*;
use crate::rev_span::TimeSpanRev;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
use crate::unicount::count_chars;

impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        if let Some(last) = self.item_times.last() {
            last.end()
        } else { 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.item_times.is_empty()
    }

    #[inline]
    pub(crate) fn try_seq_to_time(&self, seq: usize) -> Option<Time> {
        let (entry, offset) = self.item_times.find_with_offset(seq)?;
        Some(entry.1.start + offset)
    }

    pub(crate) fn seq_to_time(&self, seq: usize) -> Time {
        self.try_seq_to_time(seq).unwrap()
    }

    // /// Note the returned timespan might be shorter than seq_range.
    // pub fn try_seq_to_time_span(&self, seq_range: TimeSpan) -> Option<TimeSpan> {
    //     let (KVPair(_, entry), offset) = self.item_orders.find_with_offset(seq_range.start)?;
    //
    //     let start = entry.start + offset;
    //     let end = usize::min(entry.end, start + seq_range.len());
    //     Some(TimeSpan { start, end })
    // }
}

impl Default for OpLog {
    fn default() -> Self {
        Self::new()
    }
}

const ROOT_AGENT_NAME: &str = "ROOT";

impl OpLog {
    pub fn new() -> Self {
        Self {
            doc_id: None,
            client_with_localtime: RleVec::new(),
            client_data: vec![],
            operation_ctx: OperationCtx::new(),
            operations: Default::default(),
            // inserted_content: "".to_string(),
            history: Default::default(),
            frontier: smallvec![ROOT_TIME]
        }
    }

    pub fn checkout(&self, frontier: &[Time]) -> Branch {
        let mut branch = Branch::new();
        branch.merge(self, frontier);
        branch
    }

    pub fn checkout_tip(&self) -> Branch {
        let mut branch = Branch::new();
        branch.merge(self, &self.frontier);
        branch
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
                item_times: RleVec::new()
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

    pub fn get_agent_name(&self, agent: AgentId) -> &str {
        if agent == ROOT_AGENT { ROOT_AGENT_NAME }
        else { self.client_data[agent as usize].name.as_str() }
    }

    pub(crate) fn time_to_crdt_id(&self, time: usize) -> CRDTId {
        if time == ROOT_TIME { CRDT_DOC_ROOT }
        else {
            let (loc, offset) = self.client_with_localtime.find_packed_with_offset(time);
            loc.1.at_offset(offset as usize)
        }
    }

    #[allow(unused)]
    pub(crate) fn crdt_id_to_time(&self, id: CRDTId) -> Time {
        // if id.agent == ROOT_AGENT {
        //     ROOT_TIME
        // } else {
        //     let client = &self.client_data[id.agent as usize];
        //     client.seq_to_time(id.seq)
        // }
        self.try_crdt_id_to_time(id).unwrap()
    }

    #[allow(unused)]
    pub(crate) fn try_crdt_id_to_time(&self, id: CRDTId) -> Option<Time> {
        if id.agent == ROOT_AGENT {
            Some(ROOT_TIME)
        } else {
            let client = &self.client_data[id.agent as usize];
            client.try_seq_to_time(id.seq)
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

    // pub(crate) fn get_time(&self, loc: CRDTId) -> usize {
    //     if loc.agent == ROOT_AGENT { ROOT_TIME }
    //     else { self.client_data[loc.agent as usize].seq_to_time(loc.seq) }
    // }

    // pub(crate) fn get_time_span(&self, loc: CRDTId, max_len: u32) -> OrderSpan {
    //     assert_ne!(loc.agent, ROOT_AGENT);
    //     self.client_data[loc.agent as usize].seq_to_order_span(loc.seq, max_len)
    // }

    /// Get the number of operations
    pub fn len(&self) -> usize {
        if let Some(last) = self.client_with_localtime.last() {
            last.end()
        } else { 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.client_with_localtime.is_empty()
    }

    // Unused for now, but it should work.
    // #[allow(unused)]
    // pub(crate) fn assign_next_time_to_client(&mut self, agent: AgentId, len: usize) {
    //     let start = self.len();
    //     self.assign_next_time_to_client_known(agent, (start..start+len).into());
    // }

    // This is a modified version of assign_next_time_to_client_known to support arbitrary CRDTSpans
    // loaded from remote peers / files.
    pub(crate) fn assign_time_to_crdt_span(&mut self, start: Time, span: CRDTSpan) {
        debug_assert_eq!(start, self.len());

        let CRDTSpan { agent, seq_range } = span;
        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        let timespan = (start..start + span.len()).into();

        // Could just optimize .insert() to efficiently handle both of these cases.
        if next_seq <= seq_range.start {
            // 99.9% of the time we'll hit this case. Its really rare for seq numbers to go
            // backwards, but its possible for malicious clients to do it and introduce N^2
            // behaviour.
            client_data.item_times.push(KVPair(seq_range.start, timespan));
        } else {
            client_data.item_times.insert(KVPair(seq_range.start, timespan));
        }

        self.client_with_localtime.push(KVPair(start, span));
    }

    /// span is the local timespan we're assigning to the named agent.
    pub(crate) fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: TimeSpan) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.client_with_localtime.push(KVPair(span.start, CRDTSpan {
            agent,
            seq_range: TimeSpan { start: next_seq, end: next_seq + span.len() },
        }));
    }

    // fn insert_txn_remote(&mut self, txn_parents: &[Order], range: Range<Order>) {
    //     advance_branch_by_known(&mut self.frontier, &txn_parents, range.clone());
    //     self.insert_history_internal(txn_parents, range);
    // }

    /// Insert a new history entry for the specified range of patches, and the named parents.
    ///
    /// This method will try to extend the last entry if it can.
    pub(crate) fn insert_history(&mut self, txn_parents: &[Time], range: TimeSpan) {
        // dbg!(txn_parents, range, &self.history.entries);
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

        // let mut parent_indexes = smallvec![];
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
                    // debug_assert!(!parent_indexes.contains(&parent_idx));
                    // parent_indexes.push(parent_idx);

                    let parent_children = &mut self.history.entries.0[parent_idx].child_indexes;
                    debug_assert!(!parent_children.contains(&new_idx));
                    parent_children.push(new_idx);
                }

            }
        }

        let txn = HistoryEntry {
            span: range,
            shadow,
            parents: txn_parents.iter().copied().collect(),
            // parent_indexes,
            child_indexes: smallvec![]
        };

        let did_merge = self.history.entries.push(txn);
        assert_eq!(will_merge, did_merge);
    }

    /// Advance self.frontier by the named span of time.
    pub(crate) fn advance_frontier(&mut self, parents: &[Time], span: TimeSpan) {
        advance_frontier_by_known_run(&mut self.frontier, parents, span);
    }

    /// Append to operations list without adjusting metadata.
    ///
    /// NOTE: This method is destructive on its own. It must be paired with assign_internal() or
    /// something like that.
    pub(crate) fn push_op_internal(&mut self, next_time: Time, span: TimeSpanRev, tag: InsDelTag, content: Option<&str>) {
        // next_time should almost always be self.len - except when loading, or modifying the data
        // in some complex way.
        let content_pos = if let Some(c) = content {
            let storage = switch(tag, &mut self.operation_ctx.ins_content, &mut self.operation_ctx.del_content);
            let start = storage.len();
            storage.push_str(c);
            Some((start..start + c.len()).into())
        } else { None };

        // self.operations.push(KVPair(next_time, c.clone()));
        self.operations.push(KVPair(next_time, OperationInternal {
            span,
            tag,
            content_pos
        }));
    }

    fn assign_internal(&mut self, agent: AgentId, parents: &[Time], span: TimeSpan) {
        self.assign_next_time_to_client_known(agent, span);
        self.insert_history(parents, span);
        self.advance_frontier(parents, span);
    }

    /// Push new operations to the opset. Operation parents specified by parents parameter.
    ///
    /// Returns the single item frontier after merging.
    pub fn push_at(&mut self, agent: AgentId, parents: &[Time], ops: &[Operation]) -> Time {
        let first_time = self.len();
        let mut next_time = first_time;

        for op in ops {
            let len = op.len();

            // let content = if op.content_known { Some(op.content.as_str()) } else { None };
            // let content = op.content.map(|c| c.as_str());
            self.push_op_internal(next_time, op.span, op.tag, op.content_as_str());
            next_time += len;
        }

        self.assign_internal(agent, parents, TimeSpan { start: first_time, end: next_time });
        next_time - 1
    }

    /// Returns the single item frontier after the inserted change.
    pub fn push_insert_at(&mut self, agent: AgentId, parents: &[Time], pos: usize, ins_content: &str) -> Time {
        // This could just call push_at() but this is significantly faster according to benchmarks.
        // Equivalent to:
        // self.push_at(agent, parents, &[Operation::new_insert(pos, ins_content)])
        let len = count_chars(ins_content);
        let start = self.len();
        let end = start + len;

        self.push_op_internal(start, (pos..pos+len).into(), InsDelTag::Ins, Some(ins_content));
        self.assign_internal(agent, parents, TimeSpan { start, end });
        end - 1
    }

    /// Returns the single item frontier after the inserted change.
    pub fn push_delete_at(&mut self, agent: AgentId, parents: &[Time], pos: usize, len: usize) -> Time {
        // Equivalent to:
        // self.push_at(agent, parents, &[Operation::new_delete(pos, len)])
        let start = self.len();
        let end = start + len;

        self.push_op_internal(start, (pos..pos+len).into(), InsDelTag::Del, None);
        self.assign_internal(agent, parents, TimeSpan { start, end });
        end - 1
    }

    // *** Helpers for pushing at the current version ***

    /// Append local operations to the oplog. This method is used to make local changes to the
    /// document. Before calling this, first generate an agent ID using
    /// [`get_or_create_agent_id`](OpLog::get_or_create_agent_id). This method will:
    ///
    /// - Store the new operations
    /// - Assign the operations IDs based on the next available sequence numbers from the specified
    /// agent
    /// - Store the operation's parents as the most recent known version. (Use
    /// [`push_at`](OpLog::push_at) instead when pushing to a branch).
    pub fn push(&mut self, agent: AgentId, ops: &[Operation]) -> Time {
        // TODO: Rewrite this to avoid the .clone().
        let frontier = self.frontier.clone();
        self.push_at(agent, &frontier, ops)
    }

    /// Returns the single item frontier after the inserted change.
    /// This is a shorthand for `oplog.push(agent, *insert(pos, content)*)`
    /// TODO: Optimize these functions like push_insert_at / push_delete_at.
    pub fn push_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        self.push(agent, &[Operation::new_insert(pos, ins_content)])
    }

    /// Returns the single item frontier after the inserted change.
    /// This is a shorthand for `oplog.push(agent, *delete(pos, del_span)*)`
    pub fn push_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Time {
        self.push(agent, &[Operation::new_delete(pos, del_span)])
    }

    /// Iterate through history entries
    pub fn iter_history(&self) -> impl Iterator<Item = MinimalHistoryEntry> + '_ {
        self.history.entries.iter().map(|e| e.into())
    }

    pub fn iter_history_range(&self, range: TimeSpan) -> impl Iterator<Item = MinimalHistoryEntry> + '_ {
        self.history.entries.iter_range_map_packed(range, |e| e.into())
    }

    pub fn get_frontier(&self) -> &[Time] {
        &self.frontier
    }

    // pub(crate) fn content_str(&self, tag: InsDelTag) -> &str {
    //     switch(tag, &self.ins_content, &self.del_content)
    // }

    pub fn iter_mappings(&self) -> impl Iterator<Item = CRDTSpan> + '_ {
        self.client_with_localtime.iter().map(|item| item.1)
    }

    pub fn iter_mappings_range(&self, range: TimeSpan) -> impl Iterator<Item = CRDTSpan> + '_ {
        self.client_with_localtime.iter_range_packed_ctx(range, &()).map(|item| item.1)
    }

    pub fn print_stats(&self, detailed: bool) {
        self.operations.print_stats("Operations", detailed);

        // Get some stats on how operations are distributed
        let mut i_1 = 0;
        let mut d_1 = 0;
        let mut i_n = 0;
        let mut i_r = 0;
        let mut d_n = 0;
        let mut d_r = 0;
        for op in self.operations.iter_merged() {
            match (op.1.len(), op.1.tag, op.1.span.fwd) {
                (1, InsDelTag::Ins, _) => { i_1 += 1; }
                (_, InsDelTag::Ins, true) => { i_n += 1; }
                (_, InsDelTag::Ins, false) => { i_r += 1; }

                (1, InsDelTag::Del, _) => { d_1 += 1; }
                (_, InsDelTag::Del, true) => { d_n += 1; }
                (_, InsDelTag::Del, false) => { d_r += 1; }
            }
        }
        // These stats might make more sense as percentages.
        println!("ins: singles {}, fwd {}, rev {}", i_1, i_n, i_r);
        println!("del: singles {}, fwd {}, rev {}", d_1, d_n, d_r);

        println!("Insert content length {}", self.operation_ctx.ins_content.len());
        println!("Delete content length {}", self.operation_ctx.del_content.len());

        self.client_with_localtime.print_stats("Client localtime map", detailed);
        self.history.entries.print_stats("History", detailed);
    }
}
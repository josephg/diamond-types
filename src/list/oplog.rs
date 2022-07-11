use std::ops::Range;
use smallvec::{smallvec, SmallVec};
use smartstring::SmartString;
use rle::{HasLength, Searchable};
use crate::{AgentId, LocalVersion, ROOT_AGENT, ROOT_TIME, Time};
use crate::causalgraph::ClientData;
use crate::list::{ListBranch, ListOpLog};
use crate::frontier::{advance_frontier_by_known_run, clone_smallvec};
use crate::history::MinimalHistoryEntry;
use crate::list::internal_op::{OperationCtx, OperationInternal};
use crate::list::operation::{Operation, OpKind};
use crate::list::remote_ids::RemoteId;
use crate::dtrange::DTRange;
use crate::remotespan::*;
use crate::rev_range::RangeRev;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
use crate::unicount::count_chars;

impl Default for ListOpLog {
    fn default() -> Self {
        Self::new()
    }
}

const ROOT_AGENT_NAME: &str = "ROOT";

impl ListOpLog {
    pub fn new() -> Self {
        Self {
            doc_id: None,
            cg: Default::default(),
            operation_ctx: OperationCtx::new(),
            operations: Default::default(),
            // inserted_content: "".to_string(),
            version: smallvec![]
        }
    }

    pub fn checkout(&self, local_version: &[Time]) -> ListBranch {
        let mut branch = ListBranch::new();
        branch.merge(self, local_version);
        branch
    }

    pub fn checkout_tip(&self) -> ListBranch {
        let mut branch = ListBranch::new();
        branch.merge(self, &self.version);
        branch
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        if let Some(id) = self.get_agent_id(name) {
            id
        } else {
            // Create a new id.
            self.cg.client_data.push(ClientData {
                name: SmartString::from(name),
                item_times: RleVec::new()
            });
            (self.cg.client_data.len() - 1) as AgentId
        }
    }

    pub(crate) fn get_agent_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(ROOT_AGENT) }
        else {
            self.cg.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    pub fn get_agent_name(&self, agent: AgentId) -> &str {
        if agent == ROOT_AGENT { ROOT_AGENT_NAME }
        else { self.cg.client_data[agent as usize].name.as_str() }
    }

    pub(crate) fn time_to_crdt_id(&self, time: usize) -> CRDTGuid {
        if time == ROOT_TIME { CRDT_DOC_ROOT }
        else {
            let (loc, offset) = self.cg.client_with_localtime.find_packed_with_offset(time);
            loc.1.at_offset(offset as usize)
        }
    }

    #[allow(unused)]
    pub(crate) fn crdt_id_to_time(&self, id: CRDTGuid) -> Time {
        // if id.agent == ROOT_AGENT {
        //     ROOT_TIME
        // } else {
        //     let client = &self.cg.client_data[id.agent as usize];
        //     client.seq_to_time(id.seq)
        // }
        self.try_crdt_id_to_time(id).unwrap()
    }

    #[allow(unused)]
    pub(crate) fn try_crdt_id_to_time(&self, id: CRDTGuid) -> Option<Time> {
        if id.agent == ROOT_AGENT {
            Some(ROOT_TIME)
        } else {
            let client = &self.cg.client_data[id.agent as usize];
            client.try_seq_to_time(id.seq)
        }
    }

    pub(crate) fn get_crdt_span(&self, time: DTRange) -> CRDTSpan {
        if time.start == ROOT_TIME { CRDTSpan { agent: ROOT_AGENT, seq_range: Default::default() } }
        else {
            let (loc, offset) = self.cg.client_with_localtime.find_packed_with_offset(time.start);
            let start = loc.1.seq_range.start + offset;
            let end = usize::min(loc.1.seq_range.end, start + time.len());
            CRDTSpan {
                agent: loc.1.agent,
                seq_range: DTRange { start, end }
            }
        }
    }

    // pub(crate) fn get_time(&self, loc: CRDTId) -> usize {
    //     if loc.agent == ROOT_AGENT { ROOT_TIME }
    //     else { self.cg.client_data[loc.agent as usize].seq_to_time(loc.seq) }
    // }

    // pub(crate) fn get_time_span(&self, loc: CRDTId, max_len: u32) -> OrderSpan {
    //     assert_ne!(loc.agent, ROOT_AGENT);
    //     self.cg.client_data[loc.agent as usize].seq_to_order_span(loc.seq, max_len)
    // }

    /// Get the number of operations
    pub fn len(&self) -> usize {
        if let Some(last) = self.cg.client_with_localtime.last() {
            last.end()
        } else { 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.cg.client_with_localtime.is_empty()
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
        let client_data = &mut self.cg.client_data[agent as usize];

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

        self.cg.client_with_localtime.push(KVPair(start, span));
    }

    /// span is the local timespan we're assigning to the named agent.
    pub(crate) fn assign_next_time_to_client_known(&mut self, agent: AgentId, span: DTRange) {
        debug_assert_eq!(span.start, self.len());

        let client_data = &mut self.cg.client_data[agent as usize];

        let next_seq = client_data.get_next_seq();
        client_data.item_times.push(KVPair(next_seq, span));

        self.cg.client_with_localtime.push(KVPair(span.start, CRDTSpan {
            agent,
            seq_range: DTRange { start: next_seq, end: next_seq + span.len() },
        }));
    }

    // fn insert_txn_remote(&mut self, txn_parents: &[Order], range: Range<Order>) {
    //     advance_branch_by_known(&mut self.frontier, &txn_parents, range.clone());
    //     self.insert_history_internal(txn_parents, range);
    // }

    /// Advance self.frontier by the named span of time.
    pub(crate) fn advance_frontier(&mut self, parents: &[Time], span: DTRange) {
        advance_frontier_by_known_run(&mut self.version, parents, span);
    }

    /// Append to operations list without adjusting metadata.
    ///
    /// NOTE: This method is destructive on its own. It must be paired with assign_internal() or
    /// something like that.
    pub(crate) fn push_op_internal(&mut self, next_time: Time, loc: RangeRev, kind: OpKind, content: Option<&str>) {
        // next_time should almost always be self.len - except when loading, or modifying the data
        // in some complex way.
        let content_pos = if let Some(c) = content {
            Some(self.operation_ctx.push_str(kind, c))
        } else { None };

        // self.operations.push(KVPair(next_time, c.clone()));
        self.operations.push(KVPair(next_time, OperationInternal {
            loc,
            kind,
            content_pos
        }));
    }

    fn assign_internal(&mut self, agent: AgentId, parents: &[Time], span: DTRange) {
        self.assign_next_time_to_client_known(agent, span);
        self.cg.history.insert(parents, span);
        self.advance_frontier(parents, span);
    }

    /// Push new operations to the opset. Operation parents specified by parents parameter.
    ///
    /// Returns the single item version after merging. (The resulting LocalVersion after calling
    /// this method will be `[time]`).
    pub fn add_operations_at(&mut self, agent: AgentId, parents: &[Time], ops: &[Operation]) -> Time {
        let first_time = self.len();
        let mut next_time = first_time;

        for op in ops {
            let len = op.len();

            // let content = if op.content_known { Some(op.content.as_str()) } else { None };
            // let content = op.content.map(|c| c.as_str());
            self.push_op_internal(next_time, op.loc, op.kind, op.content_as_str());
            next_time += len;
        }

        self.assign_internal(agent, parents, DTRange { start: first_time, end: next_time });
        next_time - 1
    }

    /// Returns the single item localtime after the inserted change.
    pub fn add_insert_at(&mut self, agent: AgentId, parents: &[Time], pos: usize, ins_content: &str) -> Time {
        // This could just call add_operations_at() but this is significantly faster according to benchmarks.
        // Equivalent to:
        // self.add_operations_at(agent, parents, &[Operation::new_insert(pos, ins_content)])
        let len = count_chars(ins_content);
        let start = self.len();
        let end = start + len;

        self.push_op_internal(start, (pos..pos+len).into(), OpKind::Ins, Some(ins_content));
        self.assign_internal(agent, parents, DTRange { start, end });
        end - 1
    }

    /// Create and add a new operation from the specified agent which deletes the items (characters)
    /// in the passed range.
    ///
    /// Returns the single item localtime after the inserted change.
    pub fn add_delete_at(&mut self, agent: AgentId, parents: &[Time], loc: Range<usize>) -> Time {
        // Equivalent to:
        // self.push_at(agent, parents, &[Operation::new_delete(pos, len)])
        let start_time = self.len();
        let end_time = start_time + loc.len();

        self.push_op_internal(start_time, loc.into(), OpKind::Del, None);
        self.assign_internal(agent, parents, DTRange { start: start_time, end: end_time });
        end_time - 1
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
    /// [`branch.apply_local_operations`](Branch::apply_local_operations) instead when pushing to a
    /// branch).
    pub fn add_operations(&mut self, agent: AgentId, ops: &[Operation]) -> Time {
        // TODO: Rewrite this to avoid the .clone().
        let frontier = clone_smallvec(&self.version);
        self.add_operations_at(agent, &frontier, ops)
    }

    /// Add an insert operation to the oplog at the current version.
    ///
    /// Returns the single item localtime after the inserted change.
    /// This is a shorthand for `oplog.push(agent, *insert(pos, content)*)`
    /// TODO: Optimize these functions like push_insert_at / push_delete_at.
    pub fn add_insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        self.add_operations(agent, &[Operation::new_insert(pos, ins_content)])
    }

    /// Add a local delete operation to the oplog. This variant of the method allows a user to pass
    /// the content of the delete into the oplog. This can be useful for undos and things like that
    /// but it is NOT CHECKED. If you don't have access to the deleted content, use
    /// [`add_delete_without_content`](OpLog::add_delete_without_content) instead.
    ///
    /// If you have a local branch, its easier, faster, and safer to just call
    /// [`branch.delete(agent, pos, len)`](Branch::delete).
    ///
    /// # Safety
    /// The deleted content must match the content in the document at that range, at the
    /// current time.
    pub unsafe fn add_delete_with_unchecked_content(&mut self, agent: AgentId, pos: usize, del_content: &str) -> Time {
        self.add_operations(agent, &[Operation::new_delete_with_content(pos, del_content.into())])
    }

    /// Add a local delete operation to the oplog.
    /// Returns the single item frontier after the inserted change.
    /// This is a shorthand for `oplog.push(agent, *delete(pos, del_span)*)`
    pub fn add_delete_without_content(&mut self, agent: AgentId, loc: Range<usize>) -> Time {
        self.add_operations(agent, &[Operation::new_delete(loc)])
    }

    /// Iterate through history entries
    pub fn iter_history(&self) -> impl Iterator<Item = MinimalHistoryEntry> + '_ {
        self.cg.history.entries.iter().map(|e| e.into())
    }

    pub fn iter_history_range(&self, range: DTRange) -> impl Iterator<Item = MinimalHistoryEntry> + '_ {
        self.cg.history.entries.iter_range_map_packed(range, |e| e.into())
    }

    /// Returns a `&[usize]` reference to the tip of the oplog. This version contains all
    /// known operations.
    ///
    /// This method is provided alongside [`local_version`](OpLog::local_version) because its
    /// slightly faster.
    pub fn local_version_ref(&self) -> &[Time] {
        &self.version
    }

    /// Return the current tip version of the oplog. This is the version which contains all
    /// operations in the oplog.
    pub fn local_version(&self) -> LocalVersion {
        clone_smallvec(&self.version)
    }

    pub fn remote_version(&self) -> SmallVec<[RemoteId; 4]> {
        self.local_to_remote_version(&self.version)
    }

    // pub(crate) fn content_str(&self, tag: InsDelTag) -> &str {
    //     switch(tag, &self.ins_content, &self.del_content)
    // }

    pub fn iter_mappings(&self) -> impl Iterator<Item = CRDTSpan> + '_ {
        self.cg.client_with_localtime.iter().map(|item| item.1)
    }

    pub fn iter_mappings_range(&self, range: DTRange) -> impl Iterator<Item = CRDTSpan> + '_ {
        self.cg.client_with_localtime.iter_range_packed_ctx(range, &()).map(|item| item.1)
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
            match (op.1.len(), op.1.kind, op.1.loc.fwd) {
                (1, OpKind::Ins, _) => { i_1 += 1; }
                (_, OpKind::Ins, true) => { i_n += 1; }
                (_, OpKind::Ins, false) => { i_r += 1; }

                (1, OpKind::Del, _) => { d_1 += 1; }
                (_, OpKind::Del, true) => { d_n += 1; }
                (_, OpKind::Del, false) => { d_r += 1; }
            }
        }
        // These stats might make more sense as percentages.
        println!("ins: singles {}, fwd {}, rev {}", i_1, i_n, i_r);
        println!("del: singles {}, fwd {}, rev {}", d_1, d_n, d_r);

        println!("Insert content length {}", self.operation_ctx.ins_content.len());
        println!("Delete content length {}", self.operation_ctx.del_content.len());

        self.cg.client_with_localtime.print_stats("Client localtime map", detailed);
        self.cg.history.entries.print_stats("History", detailed);
    }

    /// Check if the specified version contains the specified point in time.
    // Exported for the fuzzer. Not sure if I actually want this exposed.
    pub fn version_contains_time(&self, local_version: &[Time], target: Time) -> bool {
        if local_version.is_empty() { true }
        else { self.cg.history.version_contains_time(local_version, target) }
    }

    // /// Returns all the changes since some (static) point in time.
    // pub fn linear_changes_since(&self, start: Time) -> TimeSpan {
    //     TimeSpan::new(start, self.len())
    // }

    /// Take the union of two versions.
    ///
    /// One way to think of a version is the name of some subset of operations in the operation log.
    /// But a local time array only explicitly names versions at the "tip" of the time DAG. For
    /// example, if we have 3 operations: A, B, C with ROOT <- A <- B <- C, then the local version
    /// will only name `{C}`, since A and B are implicit.
    ///
    /// version_union takes two versions and figures out the set union for all the contained
    /// changes, and returns the version name for that union. `version_union(a, b)` will often
    /// simply return `a` or `b`. This happens when one of the versions is a strict subset of the
    /// other.
    pub fn version_union(&self, a: &[Time], b: &[Time]) -> LocalVersion {
        self.cg.history.version_union(a, b)
    }
}
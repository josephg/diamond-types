use smallvec::smallvec;
use smartstring::SmartString;
use rle::{HasLength, MergableSpan, Searchable};
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use crate::list::{ClientData, Frontier, OpSet, Time};
use crate::list::frontier::advance_frontier_by_known_run;
use crate::list::history::HistoryEntry;
use crate::list::operation::Operation;
use crate::localtime::TimeSpan;
use crate::remotespan::*;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};

impl ClientData {
    pub fn get_next_seq(&self) -> usize {
        if let Some(last) = self.item_orders.last() {
            last.end()
        } else { 0 }
    }

    // pub fn seq_to_time(&self, seq: usize) -> Time {
    //     let (entry, offset) = self.item_orders.find_with_offset(seq).unwrap();
    //     entry.1.start + offset
    // }

    // /// Note the returned timespan might be shorter than seq_range.
    // pub fn seq_to_time_span(&self, seq_range: TimeSpan) -> TimeSpan {
    //     let (entry, offset) = self.item_orders.find_with_offset(seq_range.start).unwrap();
    //
    //     let start = entry.1.start + offset;
    //     let end = usize::min(entry.1.end, start + seq_range.len());
    //     TimeSpan { start, end }
    // }
}

impl Default for OpSet {
    fn default() -> Self {
        Self::new()
    }
}

impl OpSet {
    pub fn new() -> Self {
        Self {
            client_with_localtime: RleVec::new(),
            client_data: vec![],
            operations: Default::default(),
            // inserted_content: "".to_string(),
            history: Default::default()
        }
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

    pub(crate) fn assign_time_to_client(&mut self, loc: CRDTId, time_start: usize, len: usize) {
        self.client_with_localtime.push(KVPair(time_start, CRDTSpan {
            agent: loc.agent,
            seq_range: TimeSpan { start: loc.seq, end: loc.seq + len },
        }));

        self.client_data[loc.agent as usize].item_orders.push(KVPair(loc.seq, TimeSpan {
            start: time_start,
            end: time_start + len,
        }));
    }

    // fn insert_txn_remote(&mut self, txn_parents: &[Order], range: Range<Order>) {
    //     advance_branch_by_known(&mut self.frontier, &txn_parents, range.clone());
    //     self.insert_history_internal(txn_parents, range);
    // }

    pub(crate) fn insert_history(&mut self, txn_parents: &[Time], range: TimeSpan) {
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
            parents: txn_parents.iter().copied().collect(),
            parent_indexes,
            child_indexes: smallvec![]
        };

        let did_merge = self.history.entries.push(txn);
        assert_eq!(will_merge, did_merge);
    }

    /// Push new operations to the opset. Operation parents specified by parents parameter.
    ///
    /// Returns the single item frontier after merging.
    pub fn push(&mut self, agent: AgentId, parents: &[Time], ops: &[Operation]) -> Time {
        let first_time = self.len();
        let mut next_time = first_time;

        let op_len = ops.iter().map(|c| c.len()).sum();

        self.assign_time_to_client(CRDTId {
            agent,
            seq: self.client_data[agent as usize].get_next_seq()
        }, first_time, op_len);

        for c in ops {
            let len = c.len();

            // TODO: Remove this .clone().
            self.operations.push(KVPair(next_time, c.clone()));
            next_time += len;
        }

        self.insert_history(parents, TimeSpan { start: first_time, end: first_time + op_len });

        next_time - 1
    }

    /// Returns the single item frontier after the inserted change.
    pub fn push_insert(&mut self, agent: AgentId, parents: &[Time], pos: usize, ins_content: &str) -> Time {
        self.push(agent, parents, &[Operation::new_insert(pos, ins_content)])
    }

    /// Returns the single item frontier after the inserted change.
    pub fn push_delete(&mut self, agent: AgentId, parents: &[Time], pos: usize, del_span: usize) -> Time {
        self.push(agent, parents, &[Operation::new_delete(pos, del_span)])
    }

    pub fn get_frontier_inefficiently(&self) -> Frontier {
        // Could improve this by just looking at the last txn, and following shadows down.

        let mut b = smallvec![ROOT_TIME];
        for txn in self.history.entries.iter() {
            advance_frontier_by_known_run(&mut b, txn.parents.as_slice(), txn.span);
        }
        b
    }

    pub fn iter_history(&self) -> impl Iterator<Item = &HistoryEntry> {
        self.history.entries.iter()
    }
}
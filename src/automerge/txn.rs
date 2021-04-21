use crate::automerge::{TxnInternal, Op, TxnExternal, DocumentState, OpExternal, ClientData, MarkerEntry, Order, ROOT_ORDER, LocalOp};
use crate::range_tree::{RangeTree, NodeLeaf, Cursor, FullIndex, ContentIndex};
use ropey::Rope;
use crate::common::{CRDTLocation, AgentId, CRDT_DOC_ROOT};
use smallvec::{SmallVec, smallvec};
use std::collections::BTreeSet;
use crate::split_list::SplitList;
use std::ptr::NonNull;
use crate::splitable_span::SplitableSpan;
use crate::automerge::order::OrderMarker;
use inlinable_string::InlinableString;
use std::cmp::Ordering;
use crate::automerge::sibling_range::SiblingRange;

pub(crate) struct OpIterator<'a> {
    txn: &'a TxnInternal,
    index: usize,
    order: Order,
}

impl<'a> Iterator for OpIterator<'a> {
    type Item = (&'a Op, Order); // (Operation, operation's order for inserts, or 0.)

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.txn.ops.len() { return None; }

        let current = &self.txn.ops[self.index];
        self.index += 1;
        let len = current.item_len();

        let old_order = self.order;
        self.order += len;
        Some((current, old_order))
    }
}

impl Op {
    fn item_len(&self) -> usize {
        match self {
            Op::Insert { content, .. } => { content.chars().count() },
            Op::Delete { .. } => { 0 }
        }
    }
}


impl TxnInternal {
    fn iter(&self) -> OpIterator {
        OpIterator {
            txn: self,
            index: 0,
            order: self.insert_order_start
        }
    }

    #[allow(unused)]
    fn check(&self) {
        // A transaction must not reference anything within itself.
        let mut next_order = self.insert_order_start;
        for (op, order) in self.iter() {
            if let Op::Insert { content, parent: predecessor } = op {
                assert_eq!(*predecessor, next_order);
                next_order += content.chars().count();
                // The reference can't be within the range, and can't reference anything we haven't
                // seen yet.
                assert!(*predecessor < self.insert_order_start);
            }
        }
        assert_eq!(next_order, self.insert_order_start + self.num_inserts);
    }

    fn get_item_parent(&self, item_order: Order) -> Order {
        debug_assert!(self.contains_item(item_order));
        // Scan the txn looking for the insert
        for (op, order) in self.iter() {
            if let Op::Insert { parent, .. } = op {
                if item_order == order { return *parent; }
                else if item_order > order { return item_order - 1; }
            }
        }
        unreachable!("Failed invariant - txn does not contain item")
    }

    fn contains_item(&self, item_order: Order) -> bool {
        self.insert_order_start <= item_order && item_order < self.insert_order_start + self.num_inserts
    }
}

// Toggleable for testing.
const USE_INNER_ROPE: bool = false;

fn ordering_from(x: isize) -> Ordering {
    if x < 0 { Ordering::Less }
    else if x > 0 { Ordering::Greater }
    else { Ordering::Equal }
}

// Needed because otherwise ROOT_ORDER > everything else.
fn cmp_order(a: Order, b: Order) -> Ordering {
    if a == b { Ordering::Equal }
    else if a == ROOT_ORDER { Ordering::Less }
    else if b == ROOT_ORDER { Ordering::Greater }
    else { ordering_from(a as isize - b as isize) }
}

impl DocumentState {
    pub fn new() -> Self {
        Self {
            frontier: smallvec![ROOT_ORDER],
            txns: vec![],
            client_data: vec![],

            range_tree: RangeTree::new(),
            markers: SplitList::new(),
            // next_sibling_tree: RangeTree::new(),

            text_content: Rope::new()
        }
    }
    
    pub fn get_or_create_client_id(&mut self, name: &str) -> AgentId {
        // Probably a nicer way to write this.
        if name == "ROOT" { return AgentId::MAX; }

        if let Some(id) = self.get_client_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: InlinableString::from(name),
                txn_orders: Vec::new(),
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    fn get_client_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(AgentId::MAX) }
        else {
            self.client_data.iter()
                .position(|client_data| &client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    // fn map_external_crdt_location(&mut self, loc: &CRDTLocationExternal) -> CRDTLocation {
    //     CRDTLocation {
    //         agent: self.get_or_create_client_id(&loc.agent),
    //         seq: loc.seq
    //     }
    // }

    pub fn len(&self) -> usize {
        self.range_tree.as_ref().content_len()
    }

    fn branch_contains_version(&self, target: Order, branch: &[Order]) -> bool {
        println!("branch_contains_versions target: {} branch: {:?}", target, branch);
        // Order matters between these two lines because of how this is used in applyBackwards.
        if branch.len() == 0 { return false; }
        if target == ROOT_ORDER || branch.contains(&target) { return true; }

        // This works is via a DFS from the operation with a higher localOrder looking
        // for the Order of the smaller operation.
        // Note adding BTreeSet here adds a lot of code size. I could instead write this to use a
        // simple Vec<> + bsearch and then do BFS instead of DFS, which would be slower but smaller.
        let mut visited = BTreeSet::<Order>::new();
        let mut found = false;

        // LIFO queue. We could use a priority queue here but I'm not sure it'd be any
        // faster in practice.
        let mut queue = SmallVec::<[usize; 4]>::from(branch); //branch.to_vec();
        queue.sort_by(|a, b| b.cmp(a)); // descending so we hit the lowest first.

        while !found {
            let order = match queue.pop() {
                Some(o) => o,
                None => { break; }
            };

            if order <= target || order == ROOT_ORDER {
                if order == target { found = true; }
                continue;
            }

            if visited.contains(&order) { continue; }
            visited.insert(order);

            // let op = self.operation_by_order(order);
            let txn = &self.txns[order];

            // Operation versions. Add all of op's parents to the queue.
            queue.extend(txn.parents.iter().copied());

            // Ordered so we hit this next. This isn't necessary, the succeeds field
            // will just often be smaller than the parents.
            // if let Some(succeeds) = txn.succeeds {
            //     queue.push(succeeds);
            // }
        }

        found
    }

    /// Compare two versions to see if a>b, a<b, a==b or a||b (a and b are concurrent).
    /// This follows the pattern of PartialOrd, where we return None for concurrent operations.
    fn compare_versions(&self, a: Order, b: Order) -> Option<Ordering> {
        if a == b { return Some(Ordering::Equal); }

        // Its impossible for the operation with a smaller order to dominate the op with a larger
        // order
        let (start, target, result) = if a > b {
            (a, b, Ordering::Greater)
        } else {
            (b, a, Ordering::Less)
        };

        if self.branch_contains_version(target, &[start]) { Some(result) } else { None }
    }


    fn notify(markers: &mut SplitList<MarkerEntry<OrderMarker, ContentIndex>>, entry: OrderMarker, ptr: NonNull<NodeLeaf<OrderMarker, ContentIndex>>) {
        markers.replace_range(entry.order as usize, MarkerEntry {
            ptr, len: entry.len() as u32
        });
    }

    fn next_txn_with_inserts(&self, mut txn_order: usize) -> &TxnInternal {
        for txn in &self.txns[txn_order..] {
            if txn.num_inserts > 0 { return txn; }
        }
        unreachable!()
    }

    fn get_item_order(&self, item_loc: CRDTLocation) -> usize {
        // dbg!(item_loc);
        if item_loc == CRDT_DOC_ROOT {
            return ROOT_ORDER
        }

        let client_data: &ClientData = &self.client_data[item_loc.agent as usize];
        let txn = match client_data.txn_orders
        .binary_search_by_key(&item_loc.seq, |order| {
            let txn: &TxnInternal = &self.txns[*order];
            txn.insert_seq_start
        }) {
            Ok(seq) => {
                // If there's a delete followed by an insert, we might have landed in the delete
                // and not found the subsequent insert (which is the one we're interested in).
                let mut txn_order: Order = client_data.txn_orders[seq];
                self.next_txn_with_inserts(txn_order)
            }
            Err(next_seq) => {
                let txn_order: Order = client_data.txn_orders[next_seq - 1];
                &self.txns[txn_order]
            }
        };

        // dbg!(txn_order, txn);

        // Yikes the code above is complex. Make sure we found the right element.
        debug_assert!(txn.num_inserts > 0);
        assert!(item_loc.seq >= txn.id.seq && item_loc.seq < txn.id.seq + txn.num_inserts as u32);
        txn.insert_order_start + (item_loc.seq - txn.insert_seq_start) as usize
    }

    fn try_get_txn_order(&self, txn_id: CRDTLocation) -> Option<usize> {
        if txn_id == CRDT_DOC_ROOT {
            return Some(ROOT_ORDER)
        }
        let client = &self.client_data[txn_id.agent as usize];
        client.txn_orders.get(txn_id.seq as usize).copied()
    }

    fn get_txn_order(&self, txn_id: CRDTLocation) -> usize {
        self.try_get_txn_order(txn_id).unwrap()
    }

    fn get_txn_containing_item(&self, item_order: Order) -> &TxnInternal {
        // println!("get_txn_containing_item {}", item_order);
        match self.txns.binary_search_by_key(&item_order, |txn| {
            txn.insert_order_start
        }) {
            Ok(txn_order) => {
                // dbg!("-> OK", txn_order);
                self.next_txn_with_inserts(txn_order)
            }
            Err(txn_order) => {
                // dbg!("-> Err", txn_order);
                &self.txns[txn_order - 1]
                // &self.txns[txn_order]
            }
        }
    }

    fn get_item_parent(&self, item_order: Order) -> Order {
        let txn = self.get_txn_containing_item(item_order);
        // Scan the txn looking for the insert
        for (op, order) in txn.iter() {
            if let Op::Insert { parent, .. } = op {
                // TODO: Add a field for content length. This is super inefficient.
                if item_order >= order { return *parent; }
            }
        }
        unreachable!("Failed invariant - txn does not contain item")
    }

    fn advance_frontier(&mut self, order: usize, parents: &SmallVec<[usize; 2]>) {
        // TODO: Port these javascript checks in debug mode.
        // assert(!this.branchContainsVersion(txn.order, this.frontier), 'doc already contains version')
        // for (const parent of txn.parentsOrder) {
        //     assert(this.branchContainsVersion(parent, this.frontier), 'operation in the future')
        // }

        let mut new_frontier = smallvec![order];

        // TODO: Make this code not need to allocate if the frontier is large.
        for order in self.frontier.iter() {
            if !parents.contains(order) {
                new_frontier.push(*order);
            }
        }

        self.frontier = new_frontier;
    }

    fn next_item_order(&self) -> usize {
        if let Some(txn) = self.txns.last() {
            txn.insert_order_start + txn.num_inserts
        } else { 0 }
    }

    /// Compare two item orders to see the order in which they should end up in the resulting
    /// document. The ordering follows the resulting positions - so a<b implies a earlier than b in
    /// the document.
    fn cmp_item_order2(&self, a: Order, txn_a: &TxnInternal, b: Order, txn_b: &TxnInternal) -> Ordering {
        if cfg!(debug_assertions) {
            assert!(txn_a.contains_item(a));
            assert!(txn_b.contains_item(b));
        }

        if a == b { return Ordering::Equal; }

        // dbg!(txn_a, txn_b);
        if txn_a.id.agent == txn_b.id.agent {
            // We can just compare the sequence numbers to see which is newer.
            // Newer (higher seq) -> earlier in the document.
            txn_b.id.seq.cmp(&txn_a.id.seq)
        } else {
            let cmp = self.compare_versions(txn_a.order, txn_b.order);
            cmp.unwrap_or_else(|| {
                // Do'h - they're concurrent. Order based on sorting the agent strings.
                let a_name = &self.client_data[txn_a.id.agent as usize].name;
                let b_name = &self.client_data[txn_b.id.agent as usize].name;
                a_name.cmp(&b_name)
            })
        }
    }

    fn cmp_item_order(&self, a: Order, b: Order) -> Ordering {
        if a == b { return Ordering::Equal; }

        let txn_a = self.get_txn_containing_item(a);
        let txn_b = self.get_txn_containing_item(b);
        self.cmp_item_order2(a, txn_a, b, txn_b)
    }

    fn get_cursor_before(&self, item: Order) -> Cursor<OrderMarker, ContentIndex> {
        assert_ne!(item, ROOT_ORDER);
        let marker: NonNull<NodeLeaf<OrderMarker, ContentIndex>> = self.markers[item];
        unsafe { RangeTree::cursor_before_item(item, marker) }
    }

    fn get_cursor_after(&self, parent: Order) -> Cursor<OrderMarker, ContentIndex> {
        if parent == ROOT_ORDER {
            self.range_tree.iter()
        } else {
            let marker: NonNull<NodeLeaf<OrderMarker, ContentIndex>> = self.markers[parent];
            // self.range_tree.
            let mut cursor = unsafe {
                RangeTree::cursor_before_item(parent, marker)
            };
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            cursor
        }
    }

    fn internal_apply_ops(&mut self, txn_order: Order) {
        let txn = &self.txns[txn_order];
        // Apply the operation to the marker tree & document
        // TODO: Use iter on ops instead of unrolling it here.
        let mut item_order = txn.insert_order_start;
        let next_doc_item_order = self.next_item_order();

        for op in txn.ops.iter() {
            match op {
                Op::Insert { content, mut parent } => {
                    // We need to figure out the insert position. Usually this is right after our
                    // parent, but if the parent already has children, we need to check where
                    // amongst our parents' children we fit in.
                    //
                    // The first child (if present in the document) will always be the position-wise
                    // successor to our parent.

                    // This cursor points to the desired insert location; which might contain
                    // a sibling to skip.
                    let mut marker_cursor = self.get_cursor_after(parent);

                    // Scan items until we find the right insert location.
                    loop {
                        // This takes O(n log n) time but its a rare operation. I could optimize
                        // it further by storing the parents in the marker tree, but this is
                        // probably rare enough not to matter.
                        let sibling = marker_cursor.get_item();
                        let mut last_txn: Option<&TxnInternal> = None;
                        if let Some(sibling) = sibling {
                            let sibling_txn = match last_txn {
                                Some(t) => {
                                    if !t.contains_item(sibling) {
                                        self.get_txn_containing_item(sibling)
                                    } else { t }
                                },
                                None => self.get_txn_containing_item(sibling)
                            };
                            // dbg!(sibling_txn, sibling);
                            let sibling_parent = sibling_txn.get_item_parent(sibling);

                            // 3 cases:
                            // - If the parent > our parent, this is part of a sibling's
                            //   subtree. Its guaranteed this won't happen on the first loop
                            //   iteration. Skip.
                            // - If the parent < our parent, we've reached the end of our
                            //   siblings. Insert here.
                            // - If the parents match, we have concurrent changes. Compare
                            //   versions.

                            // This is past the end of the subtree. Insert here.
                            // dbg!(sibling_parent, parent);
                            match cmp_order(sibling_parent, parent) {
                                Ordering::Less => { break; }
                                Ordering::Equal => {
                                    let order = self.cmp_item_order2(sibling, sibling_txn, item_order, txn);
                                    assert_ne!(order, Ordering::Equal);

                                    // We go before our sibling. Insert here.
                                    if order == Ordering::Less { break; }
                                }
                                Ordering::Greater => {
                                    // Keep scanning children.
                                }
                            }

                            if !marker_cursor.next() {
                                break; // Reached the end of the document. Its gross this condition is repeated.
                            }
                            last_txn = Some(sibling_txn);
                        } else { break; } // Insert at the end of the document.
                    }

                    // println!("parent order {}", parent);

                    // Ok now we'll update the marker tree and sibling tree.

                    let inserted_len = content.chars().count();
                    let markers = &mut self.markers;
                    self.range_tree.insert(marker_cursor, OrderMarker {
                        order: item_order as u32,
                        len: inserted_len as _
                    }, |entry, leaf| {
                        DocumentState::notify(markers, entry, leaf);
                    });

                    let pos = marker_cursor.count_pos();

                    if USE_INNER_ROPE {
                        self.text_content.insert(pos as usize, content);
                        assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
                    }

                    if cfg!(debug_assertions) {
                        self.range_tree.check();
                    }

                    item_order += inserted_len;
                }
                Op::Delete { mut target, mut span } => {
                    // The span we're deleting might be split by inserts locally. Eg xxx<hi>xxx.
                    // We'll loop through deleting as much as we can each time from the document.
                    while span > 0 {
                        let cursor = self.get_cursor_before(target);
                        let cursor_pos = cursor.count_pos() as usize;
                        let markers = &mut self.markers;

                        let deleted_here = self.range_tree.remote_delete(cursor, span, |entry, leaf| {
                            DocumentState::notify(markers, entry, leaf);
                        });

                        // We don't need to update the sibling tree.

                        if USE_INNER_ROPE {
                            self.text_content.remove(cursor_pos..cursor_pos + deleted_here);
                            assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
                        }

                        span -= deleted_here;
                        // This is safe because the deleted span is guaranteed to be order-contiguous.
                        target += deleted_here;
                    }
                }
            }
        }
    }

    fn add_external_txn(&mut self, txn_ext: &TxnExternal) -> usize {
        // let id = self.map_external_crdt_location(&txn_ext.id);
        let id = txn_ext.id;

        if let Some(existing) = self.try_get_txn_order(id) {
            return existing;
        }

        let parents: SmallVec<[usize; 2]> = txn_ext.parents.iter().map(|p| {
            // self.get_txn_order(self.map_external_crdt_location(p))
            self.get_txn_order(*p)
        }).collect();

        // Go through the ops and count the number of inserted items
        let mut num_inserts = 0;
        let ops = txn_ext.ops.iter().map(|op_ext: &OpExternal| {
            match op_ext {
                OpExternal::Insert { content, parent } => {
                    num_inserts += content.chars().count();
                    Op::Insert {
                        content: content.clone(),
                        // parent: self.get_item_order(self.map_external_crdt_location(predecessor))
                        parent: self.get_item_order(*parent)
                    }
                }
                OpExternal::Delete { target, span } => {
                    Op::Delete {
                        target: self.get_item_order(*target),
                        span: *span
                    }
                }
            }
        }).collect();

        // TODO: Check the external item's insert_seq_start is correct.

        let order = self.txns.len();
        self.client_data[id.agent as usize].txn_orders.push(order);

        let txn = TxnInternal {
            id,
            order, // TODO: Remove me!
            parents,
            insert_seq_start: txn_ext.insert_seq_start,
            insert_order_start: self.next_item_order(),
            num_inserts,
            dominates: 0,
            submits: 0,
            ops,
        };

        // Not sure if this should be here or in integrate_external...
        self.advance_frontier(order, &txn.parents);

        // Last because we need to access the transaction above.
        self.txns.push(txn);

        order
    }

    fn integrate_external_txn(&mut self, txn_ext: &TxnExternal) -> usize {
        let order = self.add_external_txn(txn_ext);

        // internal_apply_ops depends on the transaction being in self.txns.
        self.internal_apply_ops(order);
        self.check();
        order
    }


    pub fn internal_txn(&mut self, agent: AgentId, local_ops: &[LocalOp]) -> Order {
        // This could be implemented by creating an external transaction then calling
        // add_external_txn, but that would be pretty inefficient. Instead we can take a lot of
        // shortcuts.
        let txns = &self.txns;
        let order = txns.len();

        let client_data = &mut self.client_data[agent as usize];
        let seq = client_data.txn_orders.len() as u32;

        let insert_seq_start = client_data.txn_orders.last().map(|order| {
            let txn = &txns[*order];
            txn.insert_seq_start + txn.num_inserts as u32
        }).unwrap_or(0);

        let insert_order_start = txns.last().map(|txn| {
            txn.insert_order_start + txn.num_inserts
        }).unwrap_or(0);

        client_data.txn_orders.push(order);

        let mut ops: SmallVec<[Op; 1]> = SmallVec::new();
        let mut num_inserts: usize = 0;

        for LocalOp { pos, ins_content, del_span } in local_ops {
            if *del_span > 0 {
                let cursor = self.range_tree.cursor_at_content_pos(*pos, false);
                let markers = &mut self.markers;
                let deleted_items = self.range_tree.local_delete(cursor, *del_span, |entry, leaf| {
                    DocumentState::notify(markers, entry, leaf);
                });
                for item in deleted_items {
                    assert!(item.len > 0);
                    ops.push(Op::Delete {
                        target: item.order as _,
                        span: item.len.abs() as _
                    });
                }

                if USE_INNER_ROPE {
                    self.text_content.remove(*pos..*pos + *del_span);
                }
            }

            if !ins_content.is_empty() {
                let len = ins_content.chars().count();
                let cursor = self.range_tree.cursor_at_content_pos(*pos, true);
                let parent = cursor.tell_predecessor().unwrap_or(ROOT_ORDER);
                let markers = &mut self.markers;

                self.range_tree.insert(cursor, OrderMarker {
                    order: insert_order_start as u32 + num_inserts as u32,
                    len: len as i32
                }, |entry, leaf| {
                    DocumentState::notify(markers, entry, leaf);
                });

                ops.push(Op::Insert {
                    // TODO: Somehow move instead of clone here
                    content: ins_content.clone(),
                    parent
                });

                if USE_INNER_ROPE {
                    self.text_content.insert(*pos, ins_content);
                }

                num_inserts += len;
            }

        }

        let txn = TxnInternal {
            id: CRDTLocation {
                agent,
                seq,
            },
            order,
            parents: SmallVec::from(self.frontier.as_slice()),
            insert_seq_start,
            insert_order_start,
            num_inserts,
            dominates: 0, // unused
            submits: 0, // unused
            ops
        };
        self.txns.push(txn);

        // The frontier is now just this element.
        self.frontier.truncate(1);
        self.frontier[0] = order;

        order
    }

    fn internal_insert(&mut self, agent: AgentId, pos: usize, ins_content: InlinableString) -> Order {
        self.internal_txn(agent, &[LocalOp {
            ins_content, pos, del_span: 0
        }])
    }
    fn internal_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Order {
        self.internal_txn(agent, &[LocalOp {
            ins_content: InlinableString::default(), pos, del_span
        }])
    }

    fn check(&self) {
        if USE_INNER_ROPE {
            assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
        }
        // ... TODO: More invasive checks here. There's a *lot* of invariants we're maintaining!
    }

    pub fn print_stats(&self) {
        // For debugging
        println!("Document length {:?}", self.range_tree.len());
        println!("Number of transactions {}", self.txns.len());
        println!("Number of nodes {}", self.range_tree.count_entries());
        // println!("marker entries {}", state.client_data[0].txn_orders.len());

    }
}


#[cfg(test)]
mod tests {
    use crate::automerge::{DocumentState, TxnExternal, OpExternal, LocalOp};
    use crate::common::{CRDTLocation, CRDT_DOC_ROOT};
    use inlinable_string::InlinableString;
    use smallvec::smallvec;
    use std::time::SystemTime;
    use std::io::Read;
    use crate::{get_thread_memory_usage, get_thread_num_allocations};
    use crate::testdata::{load_testing_data, TestPatch};
    use crate::automerge::txn::USE_INNER_ROPE;

    #[test]
    fn insert_stuff() {
        let mut state = DocumentState::new();
        let agent = state.get_or_create_client_id("seph");
        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocation {
                agent,
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT],
            ops: smallvec![OpExternal::Insert {
                content: InlinableString::from("oh hai"),
                parent: CRDT_DOC_ROOT
            }]
        });

        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocation {
                agent,
                seq: 1
            },
            insert_seq_start: 5,
            parents: smallvec![CRDTLocation {
                agent,
                seq: 0
            }],
            ops: smallvec![OpExternal::Insert {
                content: InlinableString::from("yooo"),
                parent: CRDTLocation {
                    agent: 0,
                    seq: 5
                }
            }]
        });
        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocation {
                agent,
                seq: 2
            },
            insert_seq_start: 9,
            parents: smallvec![CRDTLocation {
                agent,
                seq: 1
            }],
            ops: smallvec![OpExternal::Delete {
                target: CRDTLocation {
                    agent: 0,
                    seq: 3,
                },
                span: 3
            }]
        });

        dbg!(state);
    }

    #[test]
    fn concurrent_writes() {
        let mut state1 = DocumentState::new();
        let mut state2 = DocumentState::new();
        let seph = state1.get_or_create_client_id("seph");
        let mike = state1.get_or_create_client_id("mike");
        state2.get_or_create_client_id("seph"); // gross.
        state2.get_or_create_client_id("mike");

        let seph_txn = TxnExternal {
            id: CRDTLocation {
                agent: seph,
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT],
            ops: smallvec![OpExternal::Insert {
                content: InlinableString::from("yooo from seph"),
                parent: CRDT_DOC_ROOT
            }]
        };

        let mike_txn = TxnExternal {
            id: CRDTLocation {
                agent: mike,
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT],
            ops: smallvec![OpExternal::Insert {
                content: InlinableString::from("hi from mike"),
                parent: CRDT_DOC_ROOT
            }]
        };

        state1.integrate_external_txn(&seph_txn);
        state1.integrate_external_txn(&mike_txn);

        // State 2 gets the operations in the opposite order
        state2.integrate_external_txn(&mike_txn);
        state2.integrate_external_txn(&seph_txn);

        assert_eq!(state1.text_content, state2.text_content);

        dbg!(state1.text_content);
    }


    #[test]
    fn txn_real_world_data() {
        let test_data = load_testing_data("benchmark_data/sveltecomponent.json.gz");

        assert_eq!(test_data.start_content.len(), 0);
        println!("final length: {}, txns {} patches {}", test_data.end_content.len(), test_data.txns.len(),
                 test_data.txns.iter().fold(0, |x, i| x + i.patches.len()));

        let start_alloc = get_thread_memory_usage();

        let mut state = DocumentState::new();
        let id = state.get_or_create_client_id("jeremy");
        let mut local_ops: Vec<LocalOp> = Vec::new();

        for (_i, txn) in test_data.txns.iter().enumerate() {
            local_ops.clear();
            local_ops.extend(txn.patches.iter().map(|TestPatch(pos, del_span, ins_content)| {
                assert!(*pos <= state.len());
                LocalOp {
                    pos: *pos,
                    del_span: *del_span,
                    ins_content: InlinableString::from(ins_content.as_str())
                }
            }));

            state.internal_txn(id, local_ops.as_slice());
        }

        assert_eq!(state.len(), test_data.end_content.len());

        if USE_INNER_ROPE {
            assert!(state.text_content.eq(&test_data.end_content));
        }

        // state.client_data[0].markers.print_stats();
        // state.range_tree.print_stats();
        println!("alloc {}", get_thread_memory_usage() - start_alloc);
        println!("alloc count {}", get_thread_num_allocations());

        state.print_stats();
    }
}
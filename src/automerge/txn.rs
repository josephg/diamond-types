use crate::automerge::*;
use crate::range_tree::{RangeTree, NodeLeaf, Cursor, ContentIndex};
use ropey::Rope;
use crate::common::{CRDTLocation, AgentId, CRDT_DOC_ROOT, LocalOp};
use smallvec::{SmallVec, smallvec};
use std::collections::BTreeSet;
use crate::split_list::SplitList;
use std::ptr::NonNull;
use crate::splitable_span::SplitableSpan;
use crate::order::OrderMarker;
use smartstring::alias::{String as SmartString};
use std::cmp::Ordering;
use crate::universal::markers::MarkerOp;

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
const USE_INNER_ROPE: bool = true;

fn ordering_from(x: isize) -> Ordering {
    x.cmp(&0)
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
                name: SmartString::from(name),
                txn_orders: Vec::new(),
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    fn get_client_id(&self, name: &str) -> Option<AgentId> {
        if name == "ROOT" { Some(AgentId::MAX) }
        else {
            self.client_data.iter()
                .position(|client_data| client_data.name == name)
                .map(|id| id as AgentId)
        }
    }

    fn location_ext_to_int_mut(&mut self, id: &CRDTLocationExternal) -> CRDTLocation {
        CRDTLocation {
            agent: self.get_or_create_client_id(id.agent.as_str()),
            seq: id.seq
        }
    }

    fn location_ext_to_int(&self, id: &CRDTLocationExternal) -> Option<CRDTLocation> {
        self.get_client_id(id.agent.as_str()).map(|agent| {
            CRDTLocation {
                agent,
                seq: id.seq
            }
        })
    }

    fn location_int_to_ext(&self, id: CRDTLocation) -> CRDTLocationExternal {
        if id == CRDT_DOC_ROOT {
            CRDT_DOC_ROOT_EXTERNAL.clone()
        } else {
            CRDTLocationExternal {
                agent: self.client_data[id.agent as usize].name.clone(),
                seq: id.seq
            }
        }
    }

    fn item_order_to_ext(&self, item: Order) -> CRDTLocationExternal {
        self.location_int_to_ext(self.get_item_id(item))
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn branch_contains_version(&self, target: Order, branch: &[Order]) -> bool {
        // println!("branch_contains_versions target: {} branch: {:?}", target, branch);
        // Order matters between these two lines because of how this is used in applyBackwards.
        if branch.is_empty() { return false; }
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
        // println!("NOTIFY {:?} {:?}", entry, ptr);
        markers.replace_range(entry.order as usize, MarkerEntry {
            op: MarkerOp::Ins(ptr), len: entry.len() as u32
        });
    }

    fn next_txn_with_inserts(&self, txn_order: usize) -> &TxnInternal {
        for txn in &self.txns[txn_order..] {
            if txn.num_inserts > 0 { return txn; }
        }
        unreachable!()
    }

    fn get_item_order(&self, item_loc: CRDTLocation) -> usize {
        // dbg!(item_loc, CRDT_DOC_ROOT);
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
                let txn_order: Order = client_data.txn_orders[seq];
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
        assert!(item_loc.seq >= txn.insert_seq_start && item_loc.seq < txn.insert_seq_start + txn.num_inserts as u32);
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

    fn get_txn_id(&self, txn_order: Order) -> CRDTLocation {
        // Ok that's really easy
        if txn_order == ROOT_ORDER { CRDT_DOC_ROOT }
        else { self.txns[txn_order].id }
    }

    fn get_item_id(&self, item_order: Order) -> CRDTLocation {
        if item_order == ROOT_ORDER { CRDT_DOC_ROOT }
        else {
            let txn = self.get_txn_containing_item(item_order);
            CRDTLocation {
                agent: txn.id.agent,
                seq: txn.insert_seq_start + item_order as u32 - txn.insert_order_start as u32
            }
        }
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
            txn_a.id.seq.cmp(&txn_b.id.seq)
            // txn_b.id.seq.cmp(&txn_a.id.seq)
        } else {
            let cmp = self.compare_versions(txn_a.order, txn_b.order);
            cmp.unwrap_or_else(|| {
                // Do'h - they're concurrent. Order based on sorting the agent strings.
                let a_name = &self.client_data[txn_a.id.agent as usize].name;
                let b_name = &self.client_data[txn_b.id.agent as usize].name;
                // dbg!(a_name);
                // dbg!(b_name);
                // Smaller agent comes first. (Unlike other tests)
                b_name.cmp(&a_name)
                // a_name.cmp(&b_name)
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
        let marker: NonNull<NodeLeaf<OrderMarker, ContentIndex>> = self.markers.entry_at(item).unwrap_ptr();
        // dbg!(item, self.get_item_id(item), marker);
        unsafe { RangeTree::cursor_before_item(item, marker) }
    }

    fn get_cursor_after(&self, parent: Order) -> Cursor<OrderMarker, ContentIndex> {
        if parent == ROOT_ORDER {
            self.range_tree.cursor_at_start()
        } else {
            let marker: NonNull<NodeLeaf<OrderMarker, ContentIndex>> = self.markers.entry_at(parent).unwrap_ptr();
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
        // let next_doc_item_order = self.next_item_order();

        for op in txn.ops.iter() {
            // println!("Applying op {:?}", op);
            match op {
                Op::Insert { content, parent } => {
                    // We need to figure out the insert position. Usually this is right after our
                    // parent, but if the parent already has children, we need to check where
                    // amongst our parents' children we fit in.
                    //
                    // The first child (if present in the document) will always be the position-wise
                    // successor to our parent.

                    // This cursor points to the desired insert location; which might contain
                    // a sibling to skip.
                    let mut marker_cursor = self.get_cursor_after(*parent);

                    // Scan items until we find the right insert location.
                    let mut last_txn: Option<&TxnInternal> = None;
                    loop {
                        // This takes O(n log n) time but its a rare operation. I could optimize
                        // it further by storing the parents in the marker tree, but this is
                        // probably rare enough not to matter.
                        let sibling = marker_cursor.get_item();
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
                            match cmp_order(sibling_parent, *parent) {
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

                        let deleted_here = self.range_tree.remote_mark_deleted(cursor, span, |entry, leaf| {
                            DocumentState::notify(markers, entry, leaf);
                        });

                        assert_ne!(deleted_here, 0);

                        // We don't need to update the sibling tree.

                        if deleted_here > 0 && USE_INNER_ROPE {
                            // dbg!(&self.text_content);

                            // println!("internal Removing '{}'", self.text_content.slice(cursor_pos..cursor_pos + deleted_here as usize));
                            self.text_content.remove(cursor_pos..cursor_pos + deleted_here as usize);
                            // assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
                        }

                        let deleted_here = deleted_here.abs() as usize;
                        span -= deleted_here;
                        // This is safe because the deleted span is guaranteed to be order-contiguous.
                        target += deleted_here;
                    }
                }
            }
            // println!("-> {}", self.text_content);
        }
    }

    fn add_external_txn(&mut self, txn_ext: &TxnExternal) -> usize {
        // let id = self.map_external_crdt_location(&txn_ext.id);
        let id = self.location_ext_to_int_mut(&txn_ext.id);

        if let Some(existing) = self.try_get_txn_order(id) {
            return existing;
        }

        let parents: SmallVec<[usize; 2]> = txn_ext.parents.iter().map(|p| {
            // self.get_txn_order(self.map_external_crdt_location(p))
            self.get_txn_order(self.location_ext_to_int(p).unwrap())
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
                        parent: self.get_item_order(self.location_ext_to_int(parent).unwrap())
                    }
                }
                OpExternal::Delete { target, span } => {
                    Op::Delete {
                        target: self.get_item_order(self.location_ext_to_int(target).unwrap()),
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
            // dominates: 0,
            // submits: 0,
            ops,
        };

        // Not sure if this should be here or in integrate_external...
        self.advance_frontier(order, &txn.parents);

        // Last because we need to access the transaction above.
        self.txns.push(txn);

        order
    }

    fn export_txn(&self, order: Order) -> TxnExternal {
        let txn = &self.txns[order];

        // It would be nice to just map the ops across from the internal txn, but internal deletes
        // express spans via contiguous item orders. We might actually need multiple OpExternal
        // instances to express them!
        let mut ops: SmallVec<[OpExternal; 1]> = SmallVec::new();

        for op in txn.ops.iter() {
            match op {
                Op::Insert { content, parent } => {
                    ops.push(OpExternal::Insert {
                        content: content.clone(),
                        parent: self.location_int_to_ext(self.get_item_id(*parent))
                    });
                },
                Op::Delete { mut target, mut span } => {
                    while span > 0 {
                        // Take as much as we can from the transaction and append to ops.
                        let txn = self.get_txn_containing_item(target);
                        let offset = target - txn.insert_order_start;
                        // let base_seq = txn.insert_seq_start as usize + *target - txn.insert_order_start;
                        debug_assert!(offset < txn.num_inserts);
                        let delete_here = span.min(txn.num_inserts - offset);
                        ops.push(OpExternal::Delete {
                            target: CRDTLocationExternal {
                                agent: self.client_data[txn.id.agent as usize].name.clone(),
                                seq: txn.insert_seq_start + offset as u32
                            },
                            span: delete_here
                        });
                        target += delete_here;
                        span -= delete_here;
                    }
                }
            }
        }

        TxnExternal {
            id: self.location_int_to_ext(txn.id),
            insert_seq_start: txn.insert_seq_start,
            parents: txn.parents.iter().map(|p| {
                self.location_int_to_ext(self.get_txn_id(*p))
            }).collect(),
            ops
        }
    }

    fn integrate_external_txn(&mut self, txn_ext: &TxnExternal) -> usize {
        let order = self.add_external_txn(txn_ext);
        // dbg!(order);

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
            // TODO: Consider reusing the cursor if we can for replaces.
            if *del_span > 0 {
                let cursor = self.range_tree.cursor_at_content_pos(*pos, false);
                let markers = &mut self.markers;
                let deleted_items = self.range_tree.local_mark_deleted(cursor, *del_span, |entry, leaf| {
                    DocumentState::notify(markers, entry, leaf);
                });
                for item in deleted_items {
                    assert!(item.len > 0);
                    // NOTE: Its totally possible (and likely in some tests) for the deleted span
                    // to cross transaction boundaries. This Op::Delete might span multiple peers!
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
                // If there's deleted characters at this point in the document, we have a choice to
                // make here around whether the inserted characters should go before or after the
                // deleted range. Before is marginally better because it will give us more chance
                // to prune the tree later.
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
            // dominates: 0, // unused
            // submits: 0, // unused
            ops
        };
        self.txns.push(txn);

        // The frontier is now just this element.
        self.frontier.truncate(1);
        self.frontier[0] = order;

        order
    }

    pub fn internal_insert(&mut self, agent: AgentId, pos: usize, ins_content: SmartString) -> Order {
        self.internal_txn(agent, &[LocalOp {
            ins_content, pos, del_span: 0
        }])
    }
    pub fn internal_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Order {
        self.internal_txn(agent, &[LocalOp {
            ins_content: SmartString::default(), pos, del_span
        }])
    }

    // fn merge(a: &mut Self, b: &mut Self) {
    pub fn merge_from(&mut self, other: &Self) {
        // Locally merge all the operations which are present in other but missing locally.
        // TODO: This is horribly written - for now its just for testing. The real procedure here
        // would implement export and import for binary operations.

        let mut new_txn_orders = Vec::new();

        for other_client in other.client_data.iter() {
            let other_len = other_client.txn_orders.len();

            let self_id = self.get_or_create_client_id(&other_client.name);
            let self_client = &self.client_data[self_id as usize];
            let self_len = self_client.txn_orders.len();

            if other_len > self_len {
                new_txn_orders.extend_from_slice(&other_client.txn_orders[self_len..]);
            }
        }

        if new_txn_orders.is_empty() { return; }

        // Sort by order. The other peer will have a reasonable order.
        new_txn_orders.sort_unstable();
        for order in new_txn_orders {
            let txn = other.export_txn(order);
            // dbg!(order, &txn);
            self.integrate_external_txn(&txn);
            // dbg!(&self.text_content);

            if USE_INNER_ROPE {
                debug_assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
            }
        }
    }

    fn tell_item_position_order(&self, item: Order) -> usize {
        let cursor = self.get_cursor_before(item);
        cursor.count_pos() as _
    }

    fn tell_item_position_from_txn(&self, txn: Order, item_offset: Order) -> usize {
        let txn = &self.txns[txn];
        assert!(item_offset < txn.num_inserts);
        self.tell_item_position_order(txn.insert_order_start + item_offset)
    }

    pub fn document_items_iter(&self) -> impl Iterator<Item=CRDTLocationExternal> + '_ {
        self.range_tree.item_iter().map(move |item| {
            self.item_order_to_ext(item)
        })
    }
    pub fn document_items_with_parents_iter(&self) -> impl Iterator<Item=ItemDebugInfo> + '_ {
        self.range_tree.item_iter().map(move |item| {
            let id = self.item_order_to_ext(item);
            let txn = self.get_txn_containing_item(item);
            let parent_order = txn.get_item_parent(item);

            let parents: Vec<_> = txn.parents.iter().map(|p| {
                self.location_int_to_ext(self.get_txn_id(*p))
            }).collect();
            // (id, self.item_order_to_ext(parent_order), parents)
            ItemDebugInfo {
                item: id,
                insert_parent: self.item_order_to_ext(parent_order),
                txn_id: self.location_int_to_ext(txn.id),
                parents
            }
        }).filter(|item| {
            item.insert_parent == *CRDT_DOC_ROOT_EXTERNAL
        })
    }

    // fn tell_item_position_loc(&self, item: CRD) -> usize {
    //     let cursor = self.get_cursor_before(item);
    //     cursor.count_pos() as _
    // }

    pub fn check(&self) {
        if USE_INNER_ROPE {
            assert_eq!(self.text_content.len_chars(), self.range_tree.content_len());
        }
        // ... TODO: More invasive checks here. There's a *lot* of invariants we're maintaining!
    }

    pub fn check_content(&self, expected_content: &str) {
        if USE_INNER_ROPE {
            assert!(self.text_content.eq(expected_content));
        } else {
            assert_eq!(self.range_tree.content_len(), expected_content.chars().count());
        }
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
    use crate::automerge::*;
    use crate::common::*;
    use smartstring::SmartString;
    use smallvec::smallvec;
    use rand::prelude::*;
    use rand::Rng;

    #[test]
    fn insert_stuff() {
        let mut state = DocumentState::new();
        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocationExternal {
                agent: "seph".into(),
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
            ops: smallvec![OpExternal::Insert {
                content: SmartString::from("oh hai"),
                parent: CRDT_DOC_ROOT_EXTERNAL.clone()
            }]
        });

        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocationExternal {
                agent: "seph".into(),
                seq: 1
            },
            insert_seq_start: 5,
            parents: smallvec![CRDTLocationExternal {
                agent: "seph".into(),
                seq: 0
            }],
            ops: smallvec![OpExternal::Insert {
                content: SmartString::from("yooo"),
                parent: CRDTLocationExternal {
                    agent: "seph".into(),
                    seq: 5
                }
            }]
        });
        state.integrate_external_txn(&TxnExternal {
            id: CRDTLocationExternal {
                agent: "seph".into(),
                seq: 2
            },
            insert_seq_start: 9,
            parents: smallvec![CRDTLocationExternal {
                agent: "seph".into(),
                seq: 1
            }],
            ops: smallvec![OpExternal::Delete {
                target: CRDTLocationExternal {
                    agent: "seph".into(),
                    seq: 3,
                },
                span: 3
            }]
        });

        // dbg!(state);
    }

    #[test]
    fn concurrent_writes() {
        let mut state1 = DocumentState::new();
        let mut state2 = DocumentState::new();
        // let seph = state1.get_or_create_client_id("seph");
        // let mike = state1.get_or_create_client_id("mike");
        // state2.get_or_create_client_id("seph"); // gross.
        // state2.get_or_create_client_id("mike");

        let seph_txn = TxnExternal {
            id: CRDTLocationExternal {
                agent: "seph".into(),
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
            ops: smallvec![OpExternal::Insert {
                content: SmartString::from("yooo from seph"),
                parent: CRDT_DOC_ROOT_EXTERNAL.clone()
            }]
        };

        let mike_txn = TxnExternal {
            id: CRDTLocationExternal {
                agent: "mike".into(),
                seq: 0
            },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
            ops: smallvec![OpExternal::Insert {
                content: SmartString::from("hi from mike"),
                parent: CRDT_DOC_ROOT_EXTERNAL.clone()
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
    fn merging() {
        let mut a = DocumentState::new();
        let mut b = DocumentState::new();

        let a_seph = a.get_or_create_client_id("seph");
        a.internal_insert(a_seph, 0, "hey from seph".into());

        let b_mike = b.get_or_create_client_id("mike");
        b.internal_insert(b_mike, 0, "hey from mike".into());

        dbg!(&a);
        b.merge_from(&a);
        dbg!(&b);
        a.merge_from(&b);

        assert_eq!(a.text_content, b.text_content);
    }


    fn random_str(len: usize, rng: &mut SmallRng) -> String {
        let mut str = String::new();
        let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
        for _ in 0..len {
            str.push(alphabet[rng.gen_range(0..alphabet.len())]);
        }
        str
    }

    fn make_random_change(doc: &mut DocumentState, agent: AgentId, rng: &mut SmallRng) -> Order {
        let doc_len = doc.len();
        let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };
        if doc_len == 0 || rng.gen_bool(insert_weight) {
            // Insert something.
            let pos = rng.gen_range(0..=doc_len);
            let len: usize = rng.gen_range(1..2); // Ideally skew toward smaller inserts.
            // let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.

            let content = random_str(len as usize, rng);
            // println!("Inserting '{}' at position {}", content, pos);
            doc.internal_insert(agent, pos, content.into())
        } else {
            // Delete something
            let pos = rng.gen_range(0..doc_len);
            // println!("range {}", u32::min(10, doc_len - pos));
            let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
            // dbg!(&state.marker_tree, pos, len);
            // println!("deleting {} at position {}", span, pos);
            doc.internal_delete(agent, pos, span)
        }
    }

    #[test]
    fn internal_external_consistency() {
        // In this test we generate random operations on one document and replay them on another.
        // Document states should always exactly match.

        let mut rng = SmallRng::seed_from_u64(7);
        let mut doc1 = DocumentState::new();
        let mut doc2 = DocumentState::new();

        let agent = doc1.get_or_create_client_id("seph");

        for _i in 0..1000 {
            let order = make_random_change(&mut doc1, agent, &mut rng);
            let txn = doc1.export_txn(order);
            // dbg!(&txn);
            doc2.integrate_external_txn(&txn);

            assert_eq!(doc1.text_content, doc2.text_content);
            assert_eq!(doc1.frontier, doc2.frontier);

            // dbg!(&doc1.range_tree, &doc2.range_tree);
        }
    }

    fn make_ordering_test_doc() -> DocumentState {
        let mut doc = DocumentState::new();
        doc.integrate_external_txn(&TxnExternal {
            id: CRDTLocationExternal { agent: "z".into(), seq: 0 },
            insert_seq_start: 0,
            parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
            ops: smallvec![OpExternal::Insert {content: "zzz".into(), parent: CRDT_DOC_ROOT_EXTERNAL.clone()}]
        });
        doc
    }

    // If two concurrent inserts happen at the same place, how should they be ordered?
    fn ordering_from_base(base: &CRDTLocationExternal, base_loc: usize) {
        // TODO: Find a better way to express this sort of test case that's not so big!

        // 1. If they come from the same agent, the later seq comes first
        {
            let mut doc = make_ordering_test_doc();
            let txn_a = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "a".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "x".into(), parent: base.clone()}]
            });

            let txn_b = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "a".into(), seq: 1 },
                insert_seq_start: 0,
                parents: smallvec![CRDTLocationExternal { agent: "a".into(), seq: 0 }],
                ops: smallvec![OpExternal::Insert {content: "y".into(), parent: base.clone()}]
            });

            assert_eq!(base_loc+1, doc.tell_item_position_from_txn(txn_a, 0));
            assert_eq!(base_loc+0, doc.tell_item_position_from_txn(txn_b, 0));
        }

        // 2. If they come from different agents but the second op depends on the first...
        {
            let mut doc = make_ordering_test_doc();
            let txn_a = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "a".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "x".into(), parent: base.clone()}]
            });

            let txn_b = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "b".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDTLocationExternal { agent: "a".into(), seq: 0 }],
                ops: smallvec![OpExternal::Insert {content: "y".into(), parent: base.clone()}]
            });

            assert_eq!(base_loc+1, doc.tell_item_position_from_txn(txn_a, 0));
            assert_eq!(base_loc+0, doc.tell_item_position_from_txn(txn_b, 0));
        }

        // 3. If they come from different agents but the second op *does not* depend on the first
        //    then they should be ordered based on their agent IDs.
        {
            let mut doc = make_ordering_test_doc();
            let txn_a = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "a".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "x".into(), parent: base.clone()}]
            });

            let txn_b = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "b".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "y".into(), parent: base.clone()}]
            });

            assert_eq!(base_loc+0, doc.tell_item_position_from_txn(txn_a, 0));
            assert_eq!(base_loc+1, doc.tell_item_position_from_txn(txn_b, 0));

            // And we'll insert them in the other order too to make sure that works as well.

            let mut doc = make_ordering_test_doc();
            let txn_b = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "b".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "y".into(), parent: base.clone()}]
            });

            let txn_a = doc.integrate_external_txn(&TxnExternal {
                id: CRDTLocationExternal { agent: "a".into(), seq: 0 },
                insert_seq_start: 0,
                parents: smallvec![CRDT_DOC_ROOT_EXTERNAL.clone()],
                ops: smallvec![OpExternal::Insert {content: "x".into(), parent: base.clone()}]
            });

            assert_eq!(base_loc+0, doc.tell_item_position_from_txn(txn_a, 0));
            assert_eq!(base_loc+1, doc.tell_item_position_from_txn(txn_b, 0));
        }
    }

    #[test]
    fn ordering() {
        ordering_from_base(&CRDT_DOC_ROOT_EXTERNAL, 0);
        ordering_from_base(&CRDTLocationExternal {
            agent: "z".into(), seq: 0
        }, 1);
    }

    #[test]
    #[ignore]
    fn random_concurrency() {
        let mut rng = SmallRng::seed_from_u64(34124);

        let mut docs = [DocumentState::new(), DocumentState::new(), DocumentState::new()];

        // Each document will have a different local agent ID. I'm cheating here - just making agent
        // 0 for all of them.
        for (i, doc) in docs.iter_mut().enumerate() {
            doc.get_or_create_client_id(format!("agent {}", i).as_str());
        }

        for _i in 0..100 {
            // println!("\n\n{}", _i);

            // Generate some operations
            for _j in 0..3 {
                let doc_idx = rng.gen_range(0..docs.len());
                let doc = &mut docs[doc_idx];

                // println!("editing doc {} (content '{}')", doc_idx, doc.text_content);
                make_random_change(doc, 0, &mut rng);
                // println!("doc {} -> '{}'", doc_idx, doc.text_content);
            }

            // Then merge 2 documents at random
            let a = rng.gen_range(0..docs.len());
            let b = rng.gen_range(0..docs.len());

            if a != b {
                // println!("Merging {} and {}", a, b);
                // Oh god this is awful. I can't take mutable references to two array items.
                let (a, b) = if a > b { (a, b) } else { (b, a) };
                // a>b.
                let (start, end) = docs[..].split_at_mut(a);
                let b = &mut start[b];
                let a = &mut end[0];

                // dbg!(&a.text_content, &b.text_content);
                // dbg!(&a.range_tree, &b.range_tree);

                a.merge_from(&b);
                b.merge_from(&a);

                let a_items: Vec<_> = a.document_items_with_parents_iter().collect();
                let b_items: Vec<_> = b.document_items_with_parents_iter().collect();
                if a_items != b_items {
                    dbg!(&a_items, &b_items);
                }
                assert_eq!(a_items, b_items);

                assert_eq!(a.text_content, b.text_content);
                // println!("Post merge content {}", a.text_content);
            }
        }
    }
}
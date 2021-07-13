use crate::list::*;
// use crate::split_list::SplitList;
use crate::range_tree::{RangeTree, Cursor, NodeLeaf};
use crate::common::{AgentId, LocalOp};
use smallvec::smallvec;
use std::ptr::NonNull;
use crate::splitable_span::SplitableSpan;
use std::cmp::Ordering;
use crate::rle::Rle;
use std::iter::FromIterator;
use std::mem::replace;

// #[cfg(inlinerope)]
// const USE_INNER_ROPE: bool = true;
// #[cfg(not(inlinerope))]
const USE_INNER_ROPE: bool = false;

impl ClientData {
    pub fn get_next_seq(&self) -> u32 {
        if let Some(KVPair(loc, range)) = self.item_orders.last() {
            loc + range.len as u32
        } else { 0 }
    }
}

impl ListCRDT {
    pub fn new() -> Self {
        ListCRDT {
            client_with_order: Rle::new(),
            frontier: smallvec![ROOT_ORDER],
            client_data: vec![],
            // markers: RangeTree::new(),
            index: SplitList::new(),
            range_tree: RangeTree::new(),
            text_content: Rope::new(),
            deletes: Rle::new(),
            txns: Rle::new(),
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
                item_orders: Rle::new()
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

    fn get_agent_name(&self, agent: AgentId) -> &str {
        self.client_data[agent as usize].name.as_str()
    }

    fn get_next_order(&self) -> Order {
        if let Some(KVPair(base, entry)) = self.client_with_order.last() {
            base + entry.len as u32
        } else { 0 }
    }

    fn marker_at(&self, order: Order) -> NonNull<NodeLeaf<YjsSpan, ContentIndex>> {
        // let cursor = self.markers.cursor_at_offset_pos(order as usize, false);
        // cursor.get_item().unwrap().unwrap()
        // self.markers.find(order).unwrap().0.ptr

        self.index.entry_at(order as usize).unwrap_ptr()
    }

    fn get_cursor_after(&self, order: Order) -> Cursor<YjsSpan, ContentIndex> {
        if order == ROOT_ORDER {
            self.range_tree.cursor_at_start()
        } else {
            let marker = self.marker_at(order);
            // let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers.at(order as usize).unwrap();
            // self.range_tree.
            let mut cursor = unsafe {
                RangeTree::cursor_before_item(order, marker)
            };
            // The cursor points to parent. This is safe because of guarantees provided by
            // cursor_before_item.
            cursor.offset += 1;
            cursor
        }
    }

    fn notify(markers: &mut SpaceIndex, entry: YjsSpan, ptr: NonNull<NodeLeaf<YjsSpan, ContentIndex>>) {
        // println!("notify {:?}", &entry);

        // let cursor = markers.cursor_at_offset_pos(entry.order as usize, true);
        // markers.replace_range(cursor, MarkerEntry {
        //     ptr: Some(ptr), len: entry.len() as u32
        // }, |_,_| {});
        markers.replace_range(entry.order as usize, MarkerEntry {
            ptr: Some(ptr), len: entry.len() as u32
        });
    }

    fn integrate(&mut self, loc: CRDTLocation, item: YjsSpan, ins_content: &str, cursor_hint: Option<Cursor<YjsSpan, ContentIndex>>) {
        if cfg!(debug_assertions) {
            let next_order = self.get_next_order();
            assert_eq!(item.order, next_order);
        }

        self.client_with_order.append(KVPair(item.order, CRDTSpan {
            loc,
            len: item.len as u32
        }));

        self.client_data[loc.agent as usize].item_orders.append(KVPair(loc.seq, OrderMarker {
            order: item.order,
            len: item.len
        }));

        // Ok now that's out of the way, lets integrate!
        let mut cursor = cursor_hint.unwrap_or_else(|| {
            self.get_cursor_after(item.origin_left)
        });
        let left_cursor = cursor;
        let mut scan_start = cursor;
        let mut scanning = false;

        loop {
            let other_order = match cursor.get_item() {
                None => { break; } // End of the document
                Some(o) => { o }
            };

            if other_order == item.origin_right { break; }

            // This code could be better optimized, but its already O(n * log n), and its extremely
            // rare that you actually get concurrent inserts at the same location in the document
            // anyway.

            let other_entry = cursor.get_entry();
            let other_left_order = other_entry.origin_left_at_offset(cursor.offset as u32);
            let other_left_cursor = self.get_cursor_after(other_left_order);

            // Yjs semantics.
            match std::cmp::Ord::cmp(&other_left_cursor, &left_cursor) {
                Ordering::Less => { break; } // Top row
                Ordering::Greater => { } // Bottom row. Continue.
                Ordering::Equal => {
                    // These items might be concurrent.
                    let my_name = self.get_agent_name(loc.agent);
                    let other_loc = self.client_with_order.get(other_entry.order);
                    let other_name = self.get_agent_name(other_loc.agent);
                    if my_name > other_name {
                        scanning = false;
                    } else if item.origin_right == other_entry.origin_right {
                        break;
                    } else {
                        scanning = true;
                        scan_start = cursor;
                    }
                }
            }

            cursor.next_entry();
        }
        if scanning { cursor = scan_start; }

        // Now insert here.
        let markers = &mut self.index;
        self.range_tree.insert(cursor, item, |entry, leaf| {
            Self::notify(markers, entry, leaf);
        });

        if USE_INNER_ROPE {
            let pos = cursor.count_pos() as usize;
            self.text_content.insert(pos, ins_content);
        }
    }

    pub fn local_txn(&mut self, agent: AgentId, local_ops: &[LocalOp]) {
        let first_order = self.get_next_order();
        let mut next_order = first_order;

        for LocalOp { pos, ins_content, del_span } in local_ops {
            let pos = *pos;
            if *del_span > 0 {
                let loc = CRDTLocation {
                    agent,
                    seq: self.client_data[agent as usize].get_next_seq()
                };
                let order = next_order;
                next_order += *del_span as u32;

                self.client_with_order.append(KVPair(order, CRDTSpan { loc, len: *del_span as u32 }));

                self.client_data[loc.agent as usize].item_orders.append(KVPair(loc.seq, OrderMarker {
                    order,
                    len: *del_span as i32
                }));

                let cursor = self.range_tree.cursor_at_content_pos(pos, false);
                let markers = &mut self.index;
                let deleted_items = self.range_tree.local_mark_deleted(cursor, *del_span, |entry, leaf| {
                    Self::notify(markers, entry, leaf);
                });

                // TODO: Remove me. This is only needed because Rle doesn't support gaps.
                self.index.append_entry(self.index.last().map_or(MarkerEntry::default(), |m| {
                    MarkerEntry { len: *del_span as u32, ptr: Some(m.unwrap_ptr()) }
                }));

                // let cursor = self.markers.cursor_at_end();
                // self.markers.insert(cursor, MarkerEntry {
                //     ptr: None,
                //     len: *del_span as u32,
                // }, |_, _| {});

                let mut deleted_length = 0; // To check.
                for item in deleted_items {
                    // self.markers.append_entry(MarkerEntry::Del {
                    //     len: item.len as u32,
                    //     order: item.order
                    // });

                    self.deletes.append(KVPair(order, DeleteEntry {
                        order: item.order,
                        len: item.len as u32
                    }));
                    deleted_length += item.len as usize;
                }
                // I might be able to relax this, but we'd need to change del_span above.
                assert_eq!(deleted_length, *del_span);

                if USE_INNER_ROPE {
                    self.text_content.remove(pos..pos + *del_span);
                }
            }

            if !ins_content.is_empty() {
                // First we need the insert's base order
                let loc = CRDTLocation {
                    agent,
                    seq: self.client_data[agent as usize].get_next_seq()
                };
                let ins_len = ins_content.chars().count();

                let order = next_order;
                next_order += ins_len as u32;

                // Find the preceeding item and successor
                let (origin_left, cursor) = if pos == 0 {
                    (ROOT_ORDER, self.range_tree.cursor_at_start())
                } else {
                    let mut cursor = self.range_tree.cursor_at_content_pos(pos - 1, false);
                    let origin_left = cursor.get_item().unwrap();
                    assert!(cursor.next());
                    (origin_left, cursor)
                };

                let origin_right = cursor.get_item().unwrap_or(ROOT_ORDER);

                let item = YjsSpan {
                    order,
                    origin_left,
                    origin_right,
                    len: ins_len as i32
                };
                // dbg!(item);

                self.integrate(loc, item, ins_content.as_str(), Some(cursor));
            }
        }

        let txn_len = next_order - first_order;
        let parents = replace(&mut self.frontier, smallvec![next_order - 1]);
        let mut min_succeeds = first_order;
        while min_succeeds >= 1 && parents.contains(&(min_succeeds - 1)) {
            min_succeeds -= 1;
        }

        let txn = TxnSpan {
            order: first_order,
            len: txn_len,
            succeeds: 0,
            parents: SmallVec::from_iter(parents.into_iter())
        };
        self.txns.append(txn);
    }

    // pub fn internal_insert(&mut self, agent: AgentId, pos: usize, ins_content: SmartString) -> Order {
    pub fn local_insert(&mut self, agent: AgentId, pos: usize, ins_content: SmartString) {
        self.local_txn(agent, &[LocalOp {
            ins_content, pos, del_span: 0
        }])
    }

    pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) {
        self.local_txn(agent, &[LocalOp {
            ins_content: SmartString::default(), pos, del_span
        }])
    }

    pub fn len(&self) -> usize {
        self.range_tree.content_len()
    }

    pub fn is_empty(&self) -> bool {
        self.range_tree.len() != 0
    }

    pub fn print_stats(&self, detailed: bool) {
        self.range_tree.print_stats(detailed);
        self.index.print_stats("index", detailed);
        // self.markers.print_rle_size();
        self.deletes.print_stats("deletes", detailed);
        self.txns.print_stats("txns", detailed);
    }
}

impl ToString for ListCRDT {
    fn to_string(&self) -> String {
        self.text_content.to_string()
    }
}

impl Default for ListCRDT {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::list::*;
    use rand::prelude::*;
    use crate::common::*;
    use crate::list::doc::USE_INNER_ROPE;

    #[test]
    fn smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_client_id("seph"); // 0
        doc.local_insert(0, 0, "hi".into());
        doc.local_insert(0, 1, "yooo".into());
        doc.local_delete(0, 0, 3);
        // "hyoooi"

        dbg!(doc);
    }


    fn random_str(len: usize, rng: &mut SmallRng) -> String {
        let mut str = String::new();
        let alphabet: Vec<char> = "abcdefghijklmnop_".chars().collect();
        for _ in 0..len {
            str.push(alphabet[rng.gen_range(0..alphabet.len())]);
        }
        str
    }

    fn make_random_change(doc: &mut ListCRDT, rope: &mut Rope, agent: AgentId, rng: &mut SmallRng) {
        let doc_len = doc.len();
        let insert_weight = if doc_len < 100 { 0.55 } else { 0.45 };
        if doc_len == 0 || rng.gen_bool(insert_weight) {
            // Insert something.
            let pos = rng.gen_range(0..=doc_len);
            let len: usize = rng.gen_range(1..2); // Ideally skew toward smaller inserts.
            // let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.

            let content = random_str(len as usize, rng);
            println!("Inserting '{}' at position {}", content, pos);
            rope.insert(pos, content.as_str());
            doc.local_insert(agent, pos, content.into())
        } else {
            // Delete something
            let pos = rng.gen_range(0..doc_len);
            // println!("range {}", u32::min(10, doc_len - pos));
            let span = rng.gen_range(1..=usize::min(10, doc_len - pos));
            // dbg!(&state.marker_tree, pos, len);
            println!("deleting {} at position {}", span, pos);
            rope.remove(pos..pos+span);
            doc.local_delete(agent, pos, span)
        }
        // dbg!(&doc.markers);
        doc.index.check();
    }

    #[test]
    fn random_single_document() {
        let mut rng = SmallRng::seed_from_u64(7);
        let mut doc = ListCRDT::new();

        let agent = doc.get_or_create_client_id("seph");
        let mut expected_content = Rope::new();

        for _i in 0..1000 {
            make_random_change(&mut doc, &mut expected_content, agent, &mut rng);
            if USE_INNER_ROPE {
                assert_eq!(doc.text_content, expected_content);
            }
        }
        assert_eq!(doc.client_data[0].item_orders.num_entries(), 1);
        assert_eq!(doc.client_with_order.num_entries(), 1);
    }

    #[test]
    fn deletes_merged() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_client_id("seph");
        doc.local_insert(0, 0, "abc".into());
        // doc.local_delete(0, 2, 1);
        // doc.local_delete(0, 1, 1);
        // doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        doc.local_delete(0, 0, 1);
        dbg!(doc);

    }
}
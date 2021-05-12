use crate::yjs::*;
use crate::split_list::SplitList;
use crate::range_tree::{RangeTree, Cursor, NodeLeaf};
use crate::common::{AgentId, LocalOp};
use smallvec::{SmallVec, smallvec};
use std::ptr::NonNull;
use crate::splitable_span::SplitableSpan;

// #[cfg(inlinerope)]
// const USE_INNER_ROPE: bool = true;
// #[cfg(not(inlinerope))]
const USE_INNER_ROPE: bool = false;

impl ClientData {
    pub fn get_next_seq(&self) -> u32 {
        if let Some((loc, range)) = self.item_orders.last() {
            loc + range.len as u32
        } else { 0 }
    }
}

impl YjsDoc {
    pub fn new() -> Self {
        YjsDoc {
            client_with_order: RLE::new(),
            frontier: smallvec![ROOT_ORDER],
            client_data: vec![],
            markers: SplitList::new(),
            range_tree: RangeTree::new(),
            text_content: Rope::new(),
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
                item_orders: RLE::new()
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

    fn get_next_order(&self) -> Order {
        if let Some((base, y)) = self.client_with_order.last() {
            base + y.len as u32
        } else { 0 }
    }

    fn get_cursor_after(&self, order: Order) -> Cursor<YjsSpan, ContentIndex> {
        if order == ROOT_ORDER {
            self.range_tree.cursor_at_start()
        } else {
            let marker: NonNull<NodeLeaf<YjsSpan, ContentIndex>> = self.markers[order as usize];
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

    fn notify(markers: &mut SplitList<MarkerEntry<YjsSpan, ContentIndex>>, entry: YjsSpan, ptr: NonNull<NodeLeaf<YjsSpan, ContentIndex>>) {
        markers.replace_range(entry.order as usize, MarkerEntry {
            ptr, len: entry.len() as u32
        });
    }

    fn integrate(&mut self, loc: CRDTLocation, item: YjsSpan, ins_content: &str, cursor_hint: Option<Cursor<YjsSpan, ContentIndex>>) {
        if cfg!(debug_assertions) {
            let next_order = self.get_next_order();
            assert_eq!(item.order, next_order);
        }

        self.client_with_order.append(item.order, Entry { loc, len: item.len });

        self.client_data[loc.agent as usize].item_orders.append(loc.seq, OrderMarker {
            order: item.order,
            len: item.len
        });

        // Ok now thats out of the way, lets integrate!
        let mut cursor = cursor_hint.unwrap_or_else(|| {
            self.get_cursor_after(item.origin_left)
        });

        loop {
            let other_order = match cursor.get_item() {
                None => { break; } // End of the document
                Some(o) => { o }
            };

            if other_order == item.origin_right { break; }

            panic!("Concurrent edit!");
        }

        // Now insert here.
        let markers = &mut self.markers;
        self.range_tree.insert(cursor, item, |entry, leaf| {
            Self::notify(markers, entry, leaf);
        });

        if USE_INNER_ROPE {
            let pos = cursor.count_pos() as usize;
            self.text_content.insert(pos, ins_content);
        }
    }

    pub fn local_txn(&mut self, agent: AgentId, local_ops: &[LocalOp]) {

        for LocalOp { pos, ins_content, del_span } in local_ops {
            let pos = *pos;
            if *del_span > 0 {
                let cursor = self.range_tree.cursor_at_content_pos(pos, false);
                let markers = &mut self.markers;
                let _deleted_items = self.range_tree.local_delete(cursor, *del_span, |entry, leaf| {
                    Self::notify(markers, entry, leaf);
                });

                // ...

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
                let order = self.get_next_order();

                let ins_len = ins_content.chars().count();

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
}

#[cfg(test)]
mod tests {
    use crate::yjs::YjsDoc;

    #[test]
    fn foo() {
        let mut doc = YjsDoc::new();
        doc.get_or_create_client_id("seph");
        doc.local_insert(0, 0, "hi".into());
        doc.local_insert(0, 1, "yooo".into());

        dbg!(doc);
    }

}
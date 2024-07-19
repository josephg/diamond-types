// use diamond_core_old::{AgentId, CRDTId};
// use crate::list::{InsDelTag, ListCRDT, ROOT_LV, LV};
// use crate::list::branch::branch_eq;
// use crate::list::positional::PositionalOpRef;
// use crate::list::span::YjsSpan;
// // use crate::list::time::positionmap::PositionMap;
// use crate::unicount::consume_chars;
// use InsDelTag::*;
// use crate::list::external_txn::RemoteId;
// 
// impl ListCRDT {
//     pub fn apply_patch_at_version(&mut self, agent: AgentId, op: PositionalOpRef, branch: &[LV]) {
//         if branch_eq(branch, self.frontier.as_slice()) {
//             self.apply_local_txn(agent, op);
//         } else {
//             let mut map = PositionMap::new_at_version(self, branch);
//             self.apply_patch_at_map(&mut map, agent, op, branch);
//         }
//     }
// 
//     pub fn apply_remote_patch_at_version(&mut self, id: &RemoteId, parents: &[RemoteId], op: PositionalOpRef) {
//         let agent = self.get_or_create_agent_id(id.agent.as_str());
//         let client = &self.client_data[agent as usize];
//         let next_seq = client.get_next_seq();
//         // If the seq does not match we either need to skip or buffer the transaction.
//         assert_eq!(next_seq, id.seq, "Sequence numbers are not linear");
// 
//         let parents = self.remote_ids_to_branch(&parents);
//         self.apply_patch_at_version(agent, op, parents.as_slice());
//     }
// 
//     pub(crate) fn apply_patch_at_map(&mut self, map: &mut PositionMap, agent: AgentId, mut op: PositionalOpRef, branch: &[LV]) {
//         // local_ops: &[PositionalComponent], mut content: &str
//         // TODO: Merge this with apply_local_txn
//         let first_time = self.get_next_lv();
//         let mut next_time = first_time;
//         let txn_len = op.components.iter().map(|c| c.len).sum::<usize>();
// 
//         self.assign_lv_to_client(CRDTId {
//             agent,
//             seq: self.client_data[agent as usize].get_next_seq()
//         }, first_time, txn_len);
// 
//         // for LocalOp { pos, ins_content, del_span } in local_ops {
//         for c in op.components {
//             let orig_pos = c.pos;
//             let len = c.len;
// 
//             match c.tag {
//                 Ins => {
//                     // First we need the insert's base order
//                     let order = next_time;
//                     next_time += c.len;
// 
//                     // Find the preceding item and successor
//                     let (origin_left, cursor) = if orig_pos == 0 {
//                         (ROOT_LV, self.range_tree.cursor_at_start())
//                     } else {
//                         let mut cursor = map.list_cursor_at_content_pos(self, orig_pos - 1).0;
//                         let origin_left = cursor.get_item().unwrap();
//                         assert!(cursor.next_item());
//                         (origin_left, cursor)
//                     };
// 
//                     // The origin right is interesting. We need to end up after
//                     // let origin_right = map.order_at_content_pos(self, orig_pos, true);
//                     let origin_right = map.right_origin_at(self, orig_pos);
//                     // dbg!((origin_left, origin_right));
//                     // let origin_right = if orig_pos == map.content_len() {
//                     //     ROOT_TIME
//                     // } else {
//                     //     // stick_end: false here matches the current semantics where we still use
//                     //     // deleted items as the origin_right.
//                     //     map.order_at_content_pos(self, orig_pos, true)
//                     // };
// 
//                     let item = YjsSpan {
//                         lv: order,
//                         origin_left,
//                         origin_right,
//                         len: len as isize
//                     };
//                     // dbg!(item);
// 
//                     let ins_content = if c.content_known {
//                         Some(consume_chars(&mut op.content, len))
//                     } else { None };
// 
//                     // This is dirty. The cursor here implicitly references self. Using cursor.inner
//                     // breaks the borrow checking rules.
//                     let raw_pos = cursor.count_offset_pos();
// 
//                     let inner_cursor = cursor.inner;
//                     self.integrate(agent, item, ins_content, Some(inner_cursor));
//                     // self.integrate(agent, item, ins_content, None);
// 
//                     // dbg!(&map);
//                     map.update_from_insert(raw_pos, len);
//                     // dbg!(&map);
//                 }
// 
//                 Del => {
//                     // We need to loop here because the deleted span might have been broken up by
//                     // subsequent inserts. We also need to mark double_deletes when they happen.
// 
//                     // TODO: remaining_len, len, len_here - Gross.
//                     let mut remaining_len = len;
//                     while remaining_len > 0 {
//                         // self.debug_print_segments();
//                         let (cursor, mut len) = map.list_cursor_at_content_pos(self, orig_pos);
//                         len = len.min(remaining_len);
//                         debug_assert!(len > 0);
//                         // remaining_len -= len;
// 
//                         // dbg!(len);
// 
//                         let mut unsafe_cursor = cursor.inner;
// 
//                         // unsafe_cursor.roll_to_next_entry();
//                         // debug_assert!(unsafe_cursor.get_raw_entry().is_activated());
// 
//                         // dbg!(unsafe_cursor.get_raw_entry());
// 
//                         // let target = unsafe { unsafe_cursor.get_item().unwrap() };
//                         let len_here = self.internal_mark_deleted_at(&mut unsafe_cursor, next_time, len as _, true);
// 
//                         // This is wild, but we don't actually care if the delete succeeded. If
//                         // the delete didn't succeed, its because the item was already deleted
//                         // in the main (current) branch. But at this point in time the item
//                         // isn't (can't) have been deleted. So the map will just be modified
//                         // from Inserted -> Upstream.
//                         // dbg!(&map, len_here, orig_pos);
//                         map.update_from_delete(orig_pos, len_here as _);
//                         // dbg!(&map);
// 
//                         // len -= len_here as usize;
//                         next_time += len_here;
//                         // The cursor has been updated already by internal_mark_deleted_at.
// 
//                         // We don't need to modify orig_pos because the position will be
//                         // unchanged.
// 
//                         remaining_len -= len_here as usize;
//                     }
//                 }
//             }
//         }
// 
//         // self.insert_txn_local(first_order..next_order);
//         self.insert_txn_full(branch, first_time..next_time);
//         debug_assert_eq!(next_time, self.get_next_lv());
//     }
// }
// 
// #[cfg(test)]
// mod tests {
//     use crate::list::PositionalComponent;
//     use super::*;
// 
//     #[test]
//     fn insert_with_patch_1() {
//         let mut doc = ListCRDT::new();
//         doc.get_or_create_agent_id("a"); // 0
//         doc.get_or_create_agent_id("b"); // 1
// 
//         doc.local_insert(0, 0, "aaa");
//         doc.local_insert(0, 0, "A");
// 
//         doc.apply_patch_at_version(1, PositionalOpRef {
//             components: &[PositionalComponent {
//                 pos: 1, len: 1, content_known: true, tag: InsDelTag::Ins
//             }],
//             content: "b"
//         }, &[1]); // when the document had "aa"
// 
//         // doc.apply_patch_at_version(0, &[PositionalComponent {
//         //     pos: 0, len: 1, content_known: true, tag: InsDelTag::Ins
//         // }], "a", &[ROOT_ORDER]);
//         // doc.apply_patch_at_version(1, &[PositionalComponent {
//         //     pos: 0, len: 1, content_known: true, tag: InsDelTag::Ins
//         // }], "b", &[ROOT_ORDER]);
// 
//         if let Some(text) = doc.text_content.as_ref() {
//             assert_eq!(text, "Aabaa");
//         }
//         doc.check(true);
// 
//         // dbg!(&doc);
//     }
// 
//     #[test]
//     fn del_with_patch_1() {
//         let mut doc = ListCRDT::new();
//         doc.get_or_create_agent_id("a"); // 0
//         doc.get_or_create_agent_id("b"); // 1
// 
//         doc.local_insert(0, 0, "abc");
//         doc.local_insert(0, 0, "A");
// 
//         doc.apply_patch_at_version(1, PositionalOpRef {
//             components: &[PositionalComponent {
//                 pos: 1, len: 1, content_known: false, tag: InsDelTag::Del
//             }],
//             content: ""
//         }, &[1]); // when the document had "aa"
// 
//         if let Some(text) = doc.text_content.as_ref() {
//             assert_eq!(text, "Aac");
//         }
//         doc.check(true);
// 
//         // dbg!(&doc);
//     }
// 
//     #[test]
//     fn del_with_patch_extended() {
//         let mut doc = ListCRDT::new();
//         doc.get_or_create_agent_id("a"); // 0
//         doc.get_or_create_agent_id("b"); // 1
// 
//         doc.local_insert(0, 0, "abc");
//         doc.local_insert(0, 2, "x"); // abxc
// 
//         doc.apply_patch_at_version(1, PositionalOpRef {
//             components: &[PositionalComponent {
//                 pos: 1, len: 2, content_known: false, tag: InsDelTag::Del
//             }],
//             content: ""
//         }, &[2]);
// 
//         if let Some(text) = doc.text_content.as_ref() {
//             assert_eq!(text, "ax");
//         }
//         doc.check(true);
// 
//         // dbg!(&doc);
//     }
// 
//     #[test]
//     fn patch_double_delete() {
//         let mut doc = ListCRDT::new();
//         doc.get_or_create_agent_id("a"); // 0
//         doc.get_or_create_agent_id("b"); // 1
// 
//         doc.local_insert(0, 0, "abc");
//         doc.local_delete(0, 1, 1); // ac
//         doc.local_insert(0, 0, "X"); // Xac
// 
//         doc.apply_patch_at_version(1, PositionalOpRef {
//             components: &[PositionalComponent {
//                 pos: 1, len: 2, content_known: false, tag: InsDelTag::Del
//             }],
//             content: ""
//         }, &[2]); // Xa
// 
// 
//         if let Some(text) = doc.text_content.as_ref() {
//             assert_eq!(text, "Xa");
//         }
//         doc.check(true);
// 
//         // dbg!(&doc);
//     }
// }
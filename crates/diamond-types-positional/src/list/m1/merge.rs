use std::pin::Pin;
use content_tree::{ContentTreeRaw, ContentTreeWithIndex, FullMetricsU32, null_notify};
use rle::HasLength;
use crate::{AgentId, ROOT_TIME};
use crate::list::{ListCRDT, Time};
use crate::list::frontier::frontier_eq;
use crate::list::list::apply_local_operation;
use crate::list::operation::{InsDelTag, Operation};
use crate::list::m1::yjsspan::YjsSpan;
use crate::localtime::TimeSpan;
use crate::rle::KVPair;

// TODO: Remove FullMetrics here?
type CRDTList = Pin<Box<ContentTreeWithIndex<YjsSpan, FullMetricsU32>>>;

impl ListCRDT {
    pub fn apply_operation_at(&mut self, agent: AgentId, branch: &[Time], op: &[Operation]) {
        if frontier_eq(branch, self.checkout.frontier.as_slice()) {
            apply_local_operation(&mut self.ops, &mut self.checkout, agent, op);
            return;
        }

        // TODO: Do all this in an arena. Allocations here are silly.

        let conflicting = self.ops.history.find_conflicting_simple(self.checkout.frontier.as_slice(), branch);
        dbg!(&conflicting);

        // Generate CRDT maps for each item
        // for mut span in conflicting.iter().rev().copied() {
        //     while !span.is_empty() {
        //         // Take as much as we can.
        //         // TODO: Use an index cursor here. We'll be walking over txns in order.
        //         let (txn, offset) = self.history.find_packed_with_offset(span.start);
        //
        //         // We can consume bytes limited by:
        //         // - The txn length
        //         // - Children
        //     }
        // }
    }

    fn new_crdt_list() -> CRDTList {
        let mut list = ContentTreeWithIndex::<YjsSpan, FullMetricsU32>::new();
        list.push(YjsSpan::new_underwater());
        list
    }

    fn write_crdt_chum_in_range(&self, list: &mut CRDTList, range: TimeSpan) {
        for KVPair(time, op) in self.ops.iter_range(range) {
            match op.tag {
                InsDelTag::Ins => {
                    if op.rev { unimplemented!("Implement me!") }

                    let (origin_left, mut cursor) = if op.pos == 0 {
                        (ROOT_TIME, list.mut_cursor_at_start())
                    } else {
                        let mut cursor = list.mut_cursor_at_content_pos((op.pos - 1) as usize, false);
                        let origin_left = cursor.get_item().unwrap();
                        assert!(cursor.next_item());
                        (origin_left, cursor)
                    };

                    let origin_right = cursor.get_item().unwrap_or(ROOT_TIME);

                    let item = YjsSpan {
                        id: TimeSpan::new(time, time + op.len()),
                        origin_left,
                        origin_right,
                        is_deleted: false
                    };

                    // There's no merging behaviour here. We can just blindly insert.
                    cursor.insert(item);
                }
                InsDelTag::Del => {
                    list.local_deactivate_at_content_notify(op.pos, op.len(), null_notify);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use crate::list::ListCRDT;
    use crate::list::operation::{Operation};

    #[test]
    fn foo() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        doc.local_insert(0, 0, "aaa".into());

        let b = doc.checkout.frontier.clone();
        doc.local_delete(0, 1, 1);

        doc.apply_operation_at(1, b.as_slice(), &[Operation::new_insert(3, "x")]);
    }

    #[test]
    fn foo2() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        doc.local_insert(0, 0, "aaa".into());
        doc.local_delete(0, 1, 1); // a_a

        let mut chum = ListCRDT::new_crdt_list();
        doc.write_crdt_chum_in_range(&mut chum, (1..4).into());
        dbg!(&chum);
    }
}
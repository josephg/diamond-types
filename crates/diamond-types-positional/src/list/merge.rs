use crate::AgentId;
use crate::list::branch::branch_eq;
use crate::list::{ListCRDT, Time};
use crate::list::operation::PositionalOp;

impl ListCRDT {
    pub fn apply_operation_at(&mut self, agent: AgentId, branch: &[Time], op: PositionalOp) {
        if branch_eq(branch, self.frontier.as_slice()) {
            self.apply_local_operation(agent, op.components.as_slice(), &op.content);
            return;
        }

        // TODO: Do all this in an arena. Allocations here are silly.

        let conflicting = self.history.find_conflicting(self.frontier.as_slice(), branch);
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
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use crate::list::ListCRDT;
    use crate::list::operation::{PositionalComponent, PositionalOp};

    #[test]
    fn foo() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        doc.local_insert(0, 0, "aaa".into());

        let b = doc.frontier.clone();
        doc.local_delete(0, 1, 1);

        doc.apply_operation_at(1, b.as_slice(), PositionalOp::new_insert(3, "x"));
    }
}
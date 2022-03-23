use jumprope::JumpRope;
use crate::list::{Branch, OpLog, Time};
use smallvec::smallvec;
use smartstring::SmartString;
use crate::list::list::apply_local_operation;
use crate::list::operation::InsDelTag::*;
use crate::list::operation::{InsDelTag, Operation};
use crate::localtime::TimeSpan;
use crate::{AgentId, ROOT_TIME};

impl Branch {
    /// Create a new (empty) branch at the start of history. The branch will be an empty list.
    pub fn new() -> Self {
        Self {
            version: smallvec![ROOT_TIME],
            content: JumpRope::new(),
        }
    }

    /// Create a new branch as a checkout from the specified oplog, at the specified local time.
    /// This method equivalent to calling [`oplog.checkout(version)`](OpLog::checkout).
    pub fn new_at_local_version(oplog: &OpLog, version: &[Time]) -> Self {
        oplog.checkout(version)
    }

    /// Create a new branch as a checkout from the specified oplog by merging all changes into a
    /// single view of time. This method equivalent to calling
    /// [`oplog.checkout_tip()`](OpLog::checkout_tip).
    pub fn new_at_tip(oplog: &OpLog) -> Self {
        oplog.checkout_tip()
    }

    /// Return the current version of the branch.
    pub fn local_version(&self) -> &[Time] { &self.version }

    /// Return the current document contents. Note there is no mutable variant of this method
    /// because mutating the document's content directly would violate the constraint that all
    /// changes must bump the document's version.
    pub fn content(&self) -> &JumpRope { &self.content }

    /// Returns the document's content length.
    ///
    /// Note this is different from the oplog's length (which returns the number of operations).
    pub fn len(&self) -> usize {
        self.content.len_chars()
    }

    /// Returns true if the document's content is empty.
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Apply a single operation. This method does not update the version.
    fn apply_internal(&mut self, tag: InsDelTag, pos: TimeSpan, content: Option<&str>) {
        match tag {
            Ins => {
                self.content.insert(pos.start, content.unwrap());
            }

            Del => {
                self.content.remove(pos.into());
            }
        }
    }

    /// Apply a set of operations. Does not update version.
    #[allow(unused)]
    pub(crate) fn apply(&mut self, ops: &[Operation]) {
        for op in ops {
            self.apply_internal(op.tag, op.span.span, op.content
                .as_ref()
                .map(|s| s.as_str())
            );
        }
    }

    pub(crate) fn apply_range_from(&mut self, ops: &OpLog, range: TimeSpan) {
        for (op, content) in ops.iter_range_simple(range) {
            self.apply_internal(op.1.tag, op.1.span.span, content);
        }
    }

    pub fn make_delete_op(&self, pos: usize, del_span: usize) -> Operation {
        assert!(pos + del_span <= self.content.len_chars());
        let mut s = SmartString::new();
        s.extend(self.content.slice_chars(pos..pos+del_span));
        Operation::new_delete_with_content(pos, s)
    }

    pub fn apply_local_operations(&mut self, oplog: &mut OpLog, agent: AgentId, ops: &[Operation]) -> Time {
        apply_local_operation(oplog, self, agent, ops)
    }

    pub fn insert(&mut self, oplog: &mut OpLog, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        apply_local_operation(oplog, self, agent, &[Operation::new_insert(pos, ins_content)])
    }

    pub fn delete_without_content(&mut self, oplog: &mut OpLog, agent: AgentId, pos: usize, del_span: usize) -> Time {
        apply_local_operation(oplog, self, agent, &[Operation::new_delete(pos, del_span)])
    }

    pub fn delete(&mut self, oplog: &mut OpLog, agent: AgentId, pos: usize, del_span: usize) -> Time {
        apply_local_operation(oplog, self, agent, &[self.make_delete_op(pos, del_span)])
    }

    /// Consume the Branch and return the contained rope content.
    pub fn into_inner(self) -> JumpRope {
        self.content
    }
}

impl Default for Branch {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Branch> for JumpRope {
    fn from(branch: Branch) -> Self {
        branch.into_inner()
    }
}

impl From<Branch> for String {
    fn from(branch: Branch) -> Self {
        branch.into_inner().to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn branch_at_version() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        let after_ins = oplog.add_insert(0, 0, "hi there");
        let after_del = oplog.add_delete_without_content(0, 2, " there".len());

        let b1 = Branch::new_at_local_version(&oplog, &[after_ins]);
        assert_eq!(b1.content, "hi there");

        let b2 = Branch::new_at_local_version(&oplog, &[after_del]);
        assert_eq!(b2.content, "hi");
    }
}
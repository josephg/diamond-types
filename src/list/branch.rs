use std::cell::Ref;
use std::ops::{Deref, Range};
use jumprope::{JumpRope, JumpRopeBuf};
use crate::list::{ListBranch, ListOpLog};
use smallvec::{smallvec, SmallVec};
use smartstring::SmartString;
use crate::list::list::{apply_local_operations};
use crate::list::operation::ListOpKind::*;
use crate::list::operation::{TextOperation, ListOpKind};
use crate::dtrange::DTRange;
use crate::{AgentId, LocalVersion, Time};
use crate::frontier::clone_smallvec;
use crate::list::remote_ids::RemoteId;

impl ListBranch {
    /// Create a new (empty) branch at the start of history. The branch will be an empty list.
    pub fn new() -> Self {
        Self {
            version: smallvec![],
            content: JumpRopeBuf::new(),
        }
    }

    /// Create a new branch as a checkout from the specified oplog, at the specified local time.
    /// This method equivalent to calling [`oplog.checkout(version)`](OpLog::checkout).
    pub fn new_at_local_version(oplog: &ListOpLog, version: &[Time]) -> Self {
        oplog.checkout(version)
    }

    /// Create a new branch as a checkout from the specified oplog by merging all changes into a
    /// single view of time. This method equivalent to calling
    /// [`oplog.checkout_tip()`](OpLog::checkout_tip).
    pub fn new_at_tip(oplog: &ListOpLog) -> Self {
        oplog.checkout_tip()
    }

    /// Return the current version of the branch as a `&[usize]`.
    ///
    /// This is provided because its slightly faster than calling local_version (since it prevents a
    /// clone(), and they're weirdly expensive with smallvec!)
    pub fn local_version_ref(&self) -> &[Time] { &self.version }

    /// Return the current version of the branch
    pub fn local_version(&self) -> LocalVersion { clone_smallvec(&self.version) }

    /// Return the current version of the branch in remote form
    pub fn remote_version(&self, oplog: &ListOpLog) -> SmallVec<[RemoteId; 4]> {
        oplog.local_to_remote_version(&self.version)
    }

    /// Return the current document contents. Note there is no mutable variant of this method
    /// because mutating the document's content directly would violate the constraint that all
    /// changes must bump the document's version.
    pub fn content(&self) -> Ref<JumpRope> { self.content.borrow() }

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
    fn apply_internal(&mut self, kind: ListOpKind, pos: DTRange, content: Option<&str>) {
        match kind {
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
    pub(crate) fn apply(&mut self, ops: &[TextOperation]) {
        for op in ops {
            self.apply_internal(op.kind, op.loc.span, op.content
                .as_ref()
                .map(|s| s.as_str())
            );
        }
    }

    pub(crate) fn apply_range_from(&mut self, ops: &ListOpLog, range: DTRange) {
        for (op, content) in ops.iter_range_simple(range) {
            self.apply_internal(op.1.kind, op.1.loc.span, content);
        }
    }

    pub fn make_delete_op(&self, loc: Range<usize>) -> TextOperation {
        assert!(loc.end <= self.content.len_chars());
        let mut s = SmartString::new();
        s.extend(self.content.borrow().slice_chars(loc.clone()));
        TextOperation::new_delete_with_content_range(loc, s)
    }

    pub fn apply_local_operations(&mut self, oplog: &mut ListOpLog, agent: AgentId, ops: &[TextOperation]) -> Time {
        apply_local_operations(oplog, self, agent, ops)
    }

    pub fn insert(&mut self, oplog: &mut ListOpLog, agent: AgentId, pos: usize, ins_content: &str) -> Time {
        // The internal_do_insert / do_delete methods require that the branch is at the same version
        // as the oplog.

        // internal_do_insert(oplog, self, agent, pos, ins_content)
        apply_local_operations(oplog, self, agent, &[TextOperation::new_insert(pos, ins_content)])
    }

    pub fn delete_without_content(&mut self, oplog: &mut ListOpLog, agent: AgentId, loc: Range<usize>) -> Time {
        // internal_do_delete(oplog, self, agent, loc)
        apply_local_operations(oplog, self, agent, &[TextOperation::new_delete(loc)])
    }

    pub fn delete(&mut self, oplog: &mut ListOpLog, agent: AgentId, del_span: Range<usize>) -> Time {
        apply_local_operations(oplog, self, agent, &[self.make_delete_op(del_span)])
    }

    #[cfg(feature = "wchar_conversion")]
    pub fn insert_at_wchar(&mut self, oplog: &mut ListOpLog, agent: AgentId, wchar_pos: usize, ins_content: &str) -> Time {
        let char_pos = self.content.wchars_to_chars(wchar_pos);
        self.insert(oplog, agent, char_pos, ins_content)
    }

    #[cfg(feature = "wchar_conversion")]
    pub fn delete_at_wchar(&mut self, oplog: &mut ListOpLog, agent: AgentId, del_span_wchar: Range<usize>) -> Time {
        let start_pos = self.content.wchars_to_chars(del_span_wchar.start);
        let end_pos = self.content.wchars_to_chars(del_span_wchar.end);
        apply_local_operation(oplog, self, agent, self.make_delete_op(start_pos .. end_pos))
    }

    /// Consume the Branch and return the contained rope content.
    pub fn into_inner(self) -> JumpRope {
        self.content.into_inner()
    }
}

impl Default for ListBranch {
    fn default() -> Self {
        Self::new()
    }
}

impl From<ListBranch> for JumpRope {
    fn from(branch: ListBranch) -> Self {
        branch.into_inner()
    }
}

impl From<ListBranch> for String {
    fn from(branch: ListBranch) -> Self {
        branch.into_inner().to_string()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn branch_at_version() {
        let mut oplog = ListOpLog::new();
        oplog.get_or_create_agent_id("seph");
        let after_ins = oplog.add_insert(0, 0, "hi there");
        let after_del = oplog.add_delete_without_content(0, 2 .. 2 + " there".len());

        let b1 = ListBranch::new_at_local_version(&oplog, &[after_ins]);
        assert_eq!(b1.content, "hi there");

        let b2 = ListBranch::new_at_local_version(&oplog, &[after_del]);
        assert_eq!(b2.content, "hi");
    }

    #[test]
    fn branch_at_early_version_applies_cleanly() {
        // Regression.
        let mut oplog = ListOpLog::new();
        oplog.get_or_create_agent_id("seph");

        let mut branch1 = oplog.checkout(&[]);
        branch1.insert(&mut oplog, 0, 0, "aaa");

        let mut branch2 = oplog.checkout(&[]);
        branch2.insert(&mut oplog, 0, 0, "bbb");

        oplog.dbg_check(true);
    }
}
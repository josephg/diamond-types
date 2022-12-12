use std::ops::Range;
use jumprope::JumpRopeBuf;
use smartstring::SmartString;
use rle::HasLength;
use crate::list::operation::TextOperation;
use crate::{CausalGraph, Frontier, LV};
use crate::causalgraph::graph::Graph;
use crate::experiments::textinfo::TextInfo;
use crate::list::op_iter::{OpIterFast, OpMetricsIter};
use crate::unicount::count_chars;

#[derive(Debug, Default)]
pub(crate) struct SimpleOpLog {
    pub cg: CausalGraph,
    pub info: TextInfo,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub(crate) struct SimpleBranch {
    pub content: JumpRopeBuf,

    // Always points to a version in the subgraph.
    pub version: Frontier,
}

impl SimpleOpLog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn goop(&mut self, n: usize) -> LV {
        // Just going to use agent 0 here.
        if self.cg.agent_assignment.client_data.is_empty() {
            self.cg.get_or_create_agent_id("goopy");
        }

        self.cg.assign_local_op(0, n).last()
    }

    pub(crate) fn add_operation(&mut self, agent_name: &str, op: TextOperation) -> LV  {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = op.len();
        let range = self.cg.assign_local_op(agent, len);
        self.info.local_push_op(op, range);
        range.last()
    }

    pub(crate) fn add_operation_at(&mut self, agent_name: &str, parents: &[LV], op: TextOperation) -> LV  {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = op.len();
        let range = self.cg.assign_local_op_with_parents(parents, agent, len);
        self.info.remote_push_op(op, range, parents, &self.cg.graph);
        range.last()
    }

    pub(crate) fn add_insert_at(&mut self, agent_name: &str, parents: &[LV], pos: usize, content: &str) -> LV {
        self.add_operation_at(agent_name, parents, TextOperation::new_insert(pos, content))
    }

    pub(crate) fn add_insert(&mut self, agent_name: &str, pos: usize, content: &str) -> LV {
        self.add_operation(agent_name, TextOperation::new_insert(pos, content))
    }

    pub(crate) fn add_delete_at(&mut self, agent_name: &str, parents: &[LV], del_range: Range<usize>) -> LV {
        self.add_operation_at(agent_name, parents, TextOperation::new_delete(del_range))
    }

    pub(crate) fn add_delete(&mut self, agent_name: &str, del_range: Range<usize>) -> LV {
        self.add_operation(agent_name, TextOperation::new_delete(del_range))
    }

    pub(crate) fn to_string(&self) -> String {
        let mut result = JumpRopeBuf::new();
        self.info.merge_into(&mut result, &self.cg, &[], self.cg.version.as_ref());
        result.to_string()
    }

    pub(crate) fn merge_raw(&self, into: &mut JumpRopeBuf, from: &[LV], to: &[LV]) -> Frontier {
        self.info.merge_into(into, &self.cg, from, to)
    }

    pub(crate) fn merge_all(&self, into: &mut SimpleBranch) {
        into.version = self.merge_raw(&mut into.content, into.version.as_ref(), self.cg.version.as_ref());
    }

    pub(crate) fn merge_to_version(&self, into: &mut SimpleBranch, to_version: &[LV]) {
        into.version = self.merge_raw(&mut into.content, into.version.as_ref(), to_version);
    }

    pub(crate) fn dbg_check(&self, deep: bool) {
        // TODO: Check the op ctx makes sense I guess?
        self.cg.dbg_check(deep);
    }
}

impl SimpleBranch {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn len(&self) -> usize {
        self.content.len_chars()
    }

    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    pub fn make_delete_op(&self, loc: Range<usize>) -> TextOperation {
        assert!(loc.end <= self.content.len_chars());
        let mut s = SmartString::new();
        s.extend(self.content.borrow().slice_chars(loc.clone()));
        TextOperation::new_delete_with_content_range(loc, s)
    }
}

use std::ops::Range;
use jumprope::JumpRopeBuf;
use crate::list::operation::TextOperation;
use crate::{CausalGraph, Frontier, LV};
use crate::experiments::TextInfo;
use crate::unicount::count_chars;

#[derive(Debug, Default)]
pub(crate) struct SimpleOpLog {
    pub cg: CausalGraph,
    pub info: TextInfo,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub(crate) struct SimpleBranch {
    pub content: JumpRopeBuf,
    pub version: Frontier,
}

impl SimpleOpLog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn goop(&mut self, n: usize) -> LV {
        // Just going to use agent 0 here.
        if self.cg.client_data.is_empty() {
            self.cg.get_or_create_agent_id("goopy");
        }

        self.cg.assign_local_op(0, n).last()
    }

    pub(crate) fn add_insert_at(&mut self, agent_name: &str, parents: &[LV], pos: usize, content: &str) -> LV {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = count_chars(content);
        let range = self.cg.assign_local_op_with_parents(parents, agent, len);
        self.info.push_op(TextOperation::new_insert(pos, content), range);
        range.last()
    }

    pub(crate) fn add_insert(&mut self, agent_name: &str, pos: usize, content: &str) -> LV {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = count_chars(content);
        let range = self.cg.assign_local_op(agent, len);
        self.info.push_op(TextOperation::new_insert(pos, content), range);
        range.last()
    }

    pub(crate) fn add_delete_at(&mut self, agent_name: &str, parents: &[LV], del_range: Range<usize>) -> LV {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = del_range.len();
        let v_range = self.cg.assign_local_op_with_parents(parents, agent, len);
        self.info.push_op(TextOperation::new_delete(del_range), v_range);
        v_range.last()
    }

    pub(crate) fn add_delete(&mut self, agent_name: &str, del_range: Range<usize>) -> LV {
        let agent = self.cg.get_or_create_agent_id(agent_name);
        let len = del_range.len();
        let v_range = self.cg.assign_local_op(agent, len);
        self.info.push_op(TextOperation::new_delete(del_range), v_range);
        v_range.last()
    }

    pub(crate) fn to_string(&self) -> String {
        let mut result = JumpRopeBuf::new();
        self.info.merge_into(&mut result, &self.cg, &[], self.cg.version.as_ref());
        result.to_string()
    }

    pub(crate) fn merge_raw(&self, into: &mut JumpRopeBuf, from: &[LV], to: &[LV]) {
        self.info.merge_into(into, &self.cg, from, to);
    }

    pub(crate) fn merge_all(&self, into: &mut SimpleBranch) {
        // This is a bit inelegant.
        let old_v = into.version.clone();
        self.merge_raw(&mut into.content, old_v.as_ref(), self.cg.version.as_ref());
        into.version = self.cg.version.clone();
    }

    pub(crate) fn merge_to_version(&self, into: &mut SimpleBranch, to_version: &[LV]) {
        let old_v = into.version.clone();
        self.merge_raw(&mut into.content, old_v.as_ref(), to_version);
        into.version = to_version.into();
    }
}

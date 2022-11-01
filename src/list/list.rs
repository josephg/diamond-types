use std::mem::replace;
use std::ops::Range;
use humansize::{BINARY, format_size};
use crate::list::{ListBranch, ListCRDT, ListOpLog};
use smallvec::smallvec;
use crate::{AgentId, LocalFrontier, LV};
use rle::HasLength;
use crate::list::operation::ListOpKind::{Del, Ins};
use crate::list::operation::{ListOpKind, TextOperation};
use crate::dtrange::DTRange;
use crate::encoding::parseerror::ParseError;
use crate::frontier::replace_frontier_with;
use crate::unicount::count_chars;

// For local changes to a branch, we take the checkout's frontier as the new parents list.
fn insert_history_local(oplog: &mut ListOpLog, frontier: &mut LocalFrontier, range: DTRange) {
    // Fast path for local edits. For some reason the code below is remarkably non-performant.
    // My kingdom for https://rust-lang.github.io/rfcs/2497-if-let-chains.html
    if frontier.len() == 1 && frontier[0] == range.start.wrapping_sub(1) {
        if let Some(last) = oplog.cg.parents.entries.0.last_mut() {
            last.span.end = range.end;
            frontier[0] = range.last();
            return;
        }
    }

    // Otherwise use the slow version.
    oplog.cg.parents.push(frontier, range);
    replace_frontier_with(frontier, range.last());
}

// Slow / small version.
// pub fn apply_local_operation(oplog: &mut OpLog, branch: &mut Branch, agent: AgentId, local_ops: &[Operation]) -> Time {
//     let time = oplog.push(agent, local_ops);
//     branch.merge(oplog, &[time]);
//     time
// }

/// Most of the time when you have a local branch, you need to both append the new change to the
/// oplog and merge the new change into your local branch.
///
/// Doing both of these steps at the same time is much faster because we don't need to worry about
/// concurrent edits. (The origin position == transformed position for the change).
///
/// This method does that.
///
/// (I low key hate the duplicated code though.)
pub(crate) fn apply_local_operations(oplog: &mut ListOpLog, branch: &mut ListBranch, agent: AgentId, local_ops: &[TextOperation]) -> LV {
    let first_time = oplog.len();
    let mut next_time = first_time;

    // for LocalOp { pos, ins_content, del_span } in local_ops {
    for c in local_ops {
        let pos = c.loc.span.start;
        let len = c.len();

        match c.kind {
            Ins => {
                // assert!(c.);
                // let new_content = consume_chars(&mut content, len);
                branch.content.insert(pos, c.content.as_ref().unwrap());
            }

            Del => {
                branch.content.remove(pos..pos + len);
            }
        }

        // oplog.operations.push(KVPair(next_time, c.clone()));
        oplog.push_op_internal(next_time, c.loc, c.kind, c.content_as_str());
        next_time += len;
    }

    let span = DTRange {
        start: first_time,
        end: next_time
    };

    oplog.assign_next_time_to_client_known(agent, span);

    oplog.advance_frontier(&branch.version, span);
    // replace_frontier_with(&mut oplog.version, next_time - 1);
    insert_history_local(oplog, &mut branch.version, span);

    next_time - 1
}

// These methods exist to make benchmark numbers better. I'm the worst!

fn internal_do_insert(oplog: &mut ListOpLog, branch: &mut ListBranch, agent: AgentId, pos: usize, content: &str) -> LV {
    let start = oplog.len();

    let len = count_chars(content);

    branch.content.insert(pos, content);

    oplog.push_op_internal(start, (pos..pos + len).into(), ListOpKind::Ins, Some(content));

    let end = start + len;
    let time_span = DTRange {
        start,
        end
    };

    oplog.assign_next_time_to_client_known(agent, time_span);

    // If this isn't true, we should use oplog.advance_frontier(&branch.version, span), but thats
    // slower.
    // oplog.advance_frontier(&branch.version, time_span);
    debug_assert_eq!(oplog.version, branch.version);
    replace_frontier_with(&mut oplog.version, end - 1);
    insert_history_local(oplog, &mut branch.version, time_span);
    end - 1
}

fn internal_do_delete(oplog: &mut ListOpLog, branch: &mut ListBranch, agent: AgentId, pos: Range<usize>) -> LV {
    let start = oplog.len();

    branch.content.remove(pos.clone());

    oplog.push_op_internal(start, pos.clone().into(), ListOpKind::Del, None);

    let end = start + pos.len();
    let time_span = DTRange {
        start,
        end
    };

    oplog.assign_next_time_to_client_known(agent, time_span);

    debug_assert_eq!(oplog.version, branch.version);
    replace_frontier_with(&mut oplog.version, end - 1);
    // oplog.advance_frontier(&branch.version, time_span);
    insert_history_local(oplog, &mut branch.version, time_span);
    end - 1
}

impl Default for ListCRDT {
    fn default() -> Self {
        Self::new()
    }
}

impl ListCRDT {
    pub fn new() -> Self {
        Self {
            branch: ListBranch::new(),
            oplog: ListOpLog::new()
        }
    }

    pub fn load_from(bytes: &[u8]) -> Result<Self, ParseError> {
        let oplog = ListOpLog::load_from(bytes)?;
        let branch = oplog.checkout_tip();
        Ok(Self {
            branch, oplog
        })
    }

    pub fn merge_data_and_ff(&mut self, bytes: &[u8]) -> Result<LocalFrontier, ParseError> {
        let v = self.oplog.decode_and_add(bytes)?;
        self.branch.merge(&self.oplog, &self.oplog.version);
        Ok(v)
    }

    pub fn len(&self) -> usize {
        self.branch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.branch.is_empty()
    }

    pub fn apply_local_operations(&mut self, agent: AgentId, local_ops: &[TextOperation]) -> LV {
        apply_local_operations(&mut self.oplog, &mut self.branch, agent, local_ops)
    }

    pub fn insert(&mut self, agent: AgentId, pos: usize, ins_content: &str) -> LV {
        // self.branch.insert(&mut self.oplog, agent, pos, ins_content)
        internal_do_insert(&mut self.oplog, &mut self.branch, agent, pos, ins_content)
    }

    #[cfg(feature = "wchar_conversion")]
    pub fn insert_at_wchar(&mut self, agent: AgentId, wchar_pos: usize, ins_content: &str) -> LV {
        self.branch.insert_at_wchar(&mut self.oplog, agent, wchar_pos, ins_content)
    }

    // pub fn local_delete(&mut self, agent: AgentId, pos: usize, del_span: usize) -> Time {
    //     local_delete(&mut self.oplog, &mut self.branch, agent, pos, del_span)
    // }

    pub fn delete_without_content(&mut self, agent: AgentId, loc: Range<usize>) -> LV {
        // self.branch.delete_without_content(&mut self.oplog, agent, loc)
        internal_do_delete(&mut self.oplog, &mut self.branch, agent, loc)
    }

    pub fn delete(&mut self, agent: AgentId, range: Range<usize>) -> LV {
        self.branch.delete(&mut self.oplog, agent, range)
    }

    #[cfg(feature = "wchar_conversion")]
    pub fn delete_at_wchar(&mut self, agent: AgentId, wchar_range: Range<usize>) -> LV {
        self.branch.delete_at_wchar(&mut self.oplog, agent, wchar_range)
    }

    pub fn print_stats(&self, detailed: bool) {
        println!("Document of length {}", self.branch.len());

        println!("Content memory size: {}", format_size(
            self.branch.content.borrow().mem_size(),
            BINARY
        ));
        println!("(Efficient size: {})", format_size(
            self.branch.content.len_bytes(),
            BINARY
        ));

        self.oplog.print_stats(detailed);
    }

    pub fn get_or_create_agent_id(&mut self, name: &str) -> AgentId {
        self.oplog.get_or_create_agent_id(name)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.insert(0, 0, "hi".into());
        doc.insert(0, 1, "yooo".into());
        // "hyoooi"
        assert_eq!(doc.branch.content, "hyoooi");
        doc.delete(0, 1..4);
        assert_eq!(doc.branch.content, "hoi");

        doc.dbg_check(true);
        // dbg!(doc);

        doc.oplog.dbg_print_all();
    }
}
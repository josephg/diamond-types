use std::cmp::Ordering;
/// TODO:
///
/// This export script works to export data sets to something cross-compatible with other CRDTs.
///
/// But if we want *identical* DT documents, this isn't valid for 2 reasons:
///
/// 1. The exported data is missing user agents. (Or should be missing user agents)
/// 2. The exported data is missing `fwd: bool` for operations.
///
/// Write a second export script which outputs the data to some dt-json style format (making this a
/// non-issue). Or just add these fields in and demand people ignore them.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufRead, BufReader};

use chrono::{DateTime, FixedOffset, SubsecRound};
use serde::{Deserialize, Serialize, Serializer};
use serde::ser::SerializeTupleStruct;
use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;

use diamond_types::{AgentId, DTRange, HasLength};
use diamond_types::causalgraph::agent_assignment::AgentAssignment;
use diamond_types::causalgraph::agent_assignment::remote_ids::RemoteVersionSpan;
use diamond_types::list::ListOpLog;
use diamond_types::list::operation::{ListOpKind, TextOperation};
use diamond_types::rle::{KVPair, RleSpanHelpers, RleVec};
use rle::{AppendRle, MergableSpan, MergeableIterator, RleRun, SplitableSpan};
use rle::take_max_iter::TakeMaxFns;

// Note this discards the fwd/backwards direction of the changes. This shouldn't matter in
// practice given the whole operation is unitary.
#[derive(Clone, Debug)]
pub struct SimpleTextOp {
    pos: usize,
    del_len: usize,
    ins_content: SmartString,
}

impl MergableSpan for SimpleTextOp {
    fn can_append(&self, other: &Self) -> bool {
        // Don't concatenate inserts and deletes.
        if self.del_len > 0 {
            self.pos == other.pos
                && other.ins_content.is_empty()
        } else {
            self.pos + self.ins_content.chars().count() == other.pos
                && other.del_len == 0
        }
    }

    fn append(&mut self, other: Self) {
        self.del_len += other.del_len;
        self.ins_content.push_str(other.ins_content.as_str());
    }
}

impl Serialize for SimpleTextOp {
    // This is an accident of history. SimpleTextOp is serialized as a tuple of [pos, del_len, ins_content, timestamp].
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer, {
        let mut state = Serializer::serialize_tuple_struct(serializer, "SimpleTextOp", 4)?;
        SerializeTupleStruct::serialize_field(&mut state, &self.pos)?;
        SerializeTupleStruct::serialize_field(&mut state, &self.del_len)?;
        SerializeTupleStruct::serialize_field(&mut state, &self.ins_content)?;
        // SerializeTupleStruct::serialize_field(&mut state, &self.timestamp)?;
        SerializeTupleStruct::end(state)
    }
}

impl From<TextOperation> for SimpleTextOp {
    fn from(op: TextOperation) -> Self {
        match op.kind {
            ListOpKind::Ins => {
                if !op.loc.fwd {
                    // If inserts are reversed, we should emit a series of operations for each
                    // (reversed) keystroke.
                    todo!("Not reversing op");
                }
                SimpleTextOp {
                    pos: op.start(),
                    del_len: 0,
                    ins_content: op.content.unwrap(),
                }
            },
            ListOpKind::Del => SimpleTextOp {
                pos: op.start(),
                del_len: op.len(),
                ins_content: SmartString::new(),
            },
        }
    }
}

impl Into<TextOperation> for &SimpleTextOp {
    fn into(self) -> TextOperation {
        let SimpleTextOp { pos, del_len, ins_content, .. } = self;
        assert_ne!((*del_len == 0), !ins_content.is_empty());
        if *del_len > 0 {
            TextOperation {
                kind: ListOpKind::Del,
                loc: (*pos..*pos + *del_len).into(),
                content: None,
            }
        } else {
            let content_len = ins_content.chars().count();
            TextOperation {
                kind: ListOpKind::Ins,
                loc: (*pos..*pos + content_len).into(),
                content: Some(ins_content.clone()),
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceExportData {
    kind: &'static str,
    end_content: String,
    num_agents: usize,

    txns: Vec<TraceExportTxn>,
}

/// A Txn represents a single user edit in the document.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceExportTxn {
    parents: SmallVec<[usize; 2]>,
    num_children: usize, // TODO: Consider taking this out.
    agent: usize,
    time: DateTime<FixedOffset>,
    // op: TextOperation,
    patches: SmallVec<[SimpleTextOp; 2]>,

    // This isn't formally part of the spec, but its useful sometimes.
    #[serde(rename = "_dt_span")]
    _dt_span: [usize; 2],
}

#[derive(Clone, Debug)]
pub struct ExportTraceProblems {
    pub has_conflicts: bool,
    pub agent_ops_not_fully_ordered: bool,
    pub multiple_roots: bool,
}
impl ExportTraceProblems {
    pub fn is_ok(&self) -> bool {
        !self.has_conflicts && !self.agent_ops_not_fully_ordered && !self.multiple_roots
    }
}

pub fn check_trace_invariants(oplog: &ListOpLog) -> ExportTraceProblems {
    let mut agent_ops_not_fully_ordered = false;
    let mut num_roots = 0;

    for entry in oplog.cg.iter() {
        if entry.parents.is_root() { num_roots += 1; }
    }

    for agent in 0..oplog.cg.num_agents() {
        let mut last_lv = 0;
        // We expect the lv returned here to be in order.
        for (_, lv, len) in oplog.cg.agent_assignment.iter_lv_map_for_agent(agent as AgentId) {
            // Its not enough to compare last_lv < lv because the operations could be concurrent.
            if lv != 0 && oplog.cg.graph.version_cmp(last_lv, lv) != Some(Ordering::Less) {
                // println!("Agent {} ({}) ops {} / {} not fully ordered", agent, oplog.cg.agent_assignment.get_agent_name(agent), last_lv, lv);
                agent_ops_not_fully_ordered = true;
            }
            last_lv = lv + len - 1;
        }
    }

    ExportTraceProblems {
        has_conflicts: oplog.has_conflicts_when_merging(),
        agent_ops_not_fully_ordered,
        multiple_roots: num_roots > 1,
    }
}


// For timestamps I could use a vec of (seq_start, timestamp) and then use binary_search to find the
// nearest timestamp for any given seq. But this is fine in practice - its just for generating
// testing data.
struct Timestamps(HashMap<SmartString, Vec<DateTime<FixedOffset>>>);

// Agent, seq, timestamp.
#[derive(Debug, Clone, Deserialize)]
struct TimestampEntry(SmartString, usize, SmartString);

impl Timestamps {
    fn from_file(filename: OsString) -> Self {
        let mut result = HashMap::new();

        let file = BufReader::new(File::open(&filename).unwrap());

        for e in file.lines() {
            let e = e.unwrap();
            let TimestampEntry(agent, seq, timestamp) = serde_json::from_str(e.as_str()).unwrap();
            let ts = DateTime::parse_from_rfc3339(timestamp.as_str()).unwrap();
            // let ts = ts.trunc_subsecs(0);
            // dbg!(ts);

            let entry: &mut Vec<_> = result.entry(agent).or_default();
            if entry.len() < seq {
                // Just lazily extend out the timestamp field.
                let last = entry.last().copied().unwrap_or_default();
                entry.resize_with(seq, || last);
            }

            entry.push(ts);
        }

        Timestamps(result)
    }

    fn get_raw(&self, agent: &str, seq: usize) -> DateTime<FixedOffset> {
        self.0.get(agent).and_then(|t| {
            t.get(seq).or(t.last()).copied()
        }).unwrap_or_default()
    }

    // fn get_lv(&self, lv: LV, agent_assignment: &AgentAssignment) -> DateTime<FixedOffset> {
    //     let rv = agent_assignment.local_to_remote_version(lv);
    //     self.get(rv.0, rv.1)
    // }

    fn get_rv_range(&self, mut av_span: RemoteVersionSpan) -> (DateTime<FixedOffset>, usize) {
        assert!(!av_span.is_empty());

        let ts = self.get_raw(av_span.0, av_span.1.take_first().unwrap());
        let mut len = 1;

        for seq in av_span.1.iter() {
            let ts2 = self.get_raw(av_span.0, seq);
            if ts2 != ts {
                break;
            }

            len += 1;
        }

        // The timestamp we actually use is truncated.
        // ts = ts.trunc_subsecs(0);
        (ts, len)
    }

    /// Get a range of versions which all have the same timestamp
    fn get_lv_range(&self, lv: DTRange, agent_assignment: &AgentAssignment) -> (DateTime<FixedOffset>, usize) {
        let av_span = agent_assignment.local_to_remote_version_span(lv);
        self.get_rv_range(av_span)
    }
}

/// Returns a map from agent_seq -> slot and the number of used slots.
fn safe_assignments_needed_for_agent(oplog: &ListOpLog, agent: AgentId) -> (RleVec<KVPair<RleRun<usize>>>, usize) {
    let mut last_lv = vec![];
    let mut map = RleVec::new();

    for (seq, lv, len) in oplog.cg.agent_assignment.iter_lv_map_for_agent(agent) {
        // Find the first item in last_lv which is strictly before lv.
        let slot = if let Some(slot) = last_lv.iter().position(|other_lv| {
            oplog.cg.graph.version_cmp(*other_lv, lv) == Some(Ordering::Less)
        }) {
            last_lv[slot] = lv + len - 1;
            slot
        } else {
            let slot = last_lv.len();
            last_lv.push(lv + len - 1);
            slot
        };

        // map.push(KVPair(lv, RleRun::new(slot, len)));
        map.push(KVPair(seq, RleRun::new(slot, len)));
    }

    // dbg!(map);
    // todo!()

    (map, last_lv.len())
}


pub fn export_trace_to_json(oplog: &ListOpLog, timestamp_filename: Option<OsString>, shatter: bool, safe: bool) -> TraceExportData {
    let timestamps = timestamp_filename.map(Timestamps::from_file);

    // TODO: A hashmap is overkill here. A vec + binary search would be fine. Eh.
    // Each chunk of operations has an ID so other ops can refer to it.
    let mut idx_for_v = HashMap::new();
    let mut last_version_from_agent = HashMap::new();

    // Editing traces *should* be non-conflicting, but its still convenient sometimes to export and
    // use editing traces which contain editing conflicts. In the trace editing format, agents are
    // referred to by number. Locally we use strings and sort the strings lexicographically to order
    // concurrent edits.
    //
    // Anyway, long and short of it is - we'll map each local agent to a number in agent ID order.
    let raw_num_agents = oplog.cg.num_agents();
    let mut sorted_agents: Vec<AgentId> = (0..raw_num_agents).collect();
    sorted_agents.sort_unstable_by(|a, b| {
        let a_name = oplog.cg.agent_assignment.get_agent_name(*a);
        let b_name = oplog.cg.agent_assignment.get_agent_name(*b);
        a_name.cmp(b_name)
    });

    // Agent_map maps from local agent_id (int) -> output agent_id (int). If we're in safe mode,
    // each local agent might map to multiple output agents. In this case, agent_map names the
    // base (first) slot.
    let mut agent_map: Vec<usize> = vec![0; raw_num_agents as usize];
    let (num_agents, mappings) = if !safe {
        // sorted_agents maps from order -> agent_id. We need a map from agent_id -> order, so we'll
        // make another list and invert sorted_agents.
        for (i, agent) in sorted_agents.iter().enumerate() {
            agent_map[*agent as usize] = i;
        }
        (raw_num_agents as usize, None)
    } else {
        let mut mappings = vec![];
        let mut num_agents = 0;

        for agent in 0..oplog.num_agents() {
            let (map, slots_used) = safe_assignments_needed_for_agent(oplog, agent);
            assert!(slots_used >= 1);
            num_agents += slots_used;
            mappings.push((map, slots_used));
        }

        let mut next = 0;
        for &agent in sorted_agents.iter() {
            agent_map[agent as usize] = next;
            next += mappings[agent as usize].1;
        }

        (num_agents, Some(mappings))
    };

    let mut txns = vec![];

    let mut iter = oplog.as_chunked_operation_vec().into_iter().take_max();
    // for (i, entry) in oplog.as_chunked_operation_vec().into_iter().enumerate() {
    while iter.peek().is_some() {
        let (timestamp, entry) = if let Some(ts) = timestamps.as_ref() {
            // Take as many items from the entry that have the same exact timestamp.
            let (ts, ts_len) = ts.get_lv_range(iter.peek().unwrap().span, &oplog.cg.agent_assignment);
            (ts.trunc_subsecs(0), iter.next(ts_len).unwrap())
        } else {
            // Might be cleaner to have a dedicated method for this.
            //
            // When there is no timestamp information, I'm splitting each patch into its own
            // transaction because thats more accurate than doing the opposite.
            (Default::default(), iter.next(if shatter { 1 } else { usize::MAX }).unwrap())
        };

        // if let Some(last_v) = last_version_from_agent.get(&entry.agent_span.agent) {
        //     if !force {
        //         assert_eq!(Some(Ordering::Less), oplog.cg.graph.version_cmp(*last_v, entry.span.start), "Operations are not fully ordered from each agent");
        //     }
        // }
        last_version_from_agent.insert(entry.agent_span.agent, entry.span.last());

        // if !force {
        //     assert_eq!(i == 0, entry.parents.is_empty(), "Cannot export trace: ROOT entry has multiple children");
        // }

        // I'm not sure how this can happen, but its cheap to check just in case.
        assert_eq!(entry.ops.is_empty(), false, "Transaction cannot have empty op list");

        // let agent = agent_map[entry.agent_span.agent as usize];
        let base = agent_map[entry.agent_span.agent as usize];
        let agent = if let Some(mappings) = mappings.as_ref() {
            let (m, _) = &mappings[entry.agent_span.agent as usize];
            let slot_entry = m.find_packed(entry.agent_span.seq_range.start);
            assert!(slot_entry.end() >= entry.agent_span.seq_range.end);
            let slot = slot_entry.1.val;
            // if slot >= 1 {
            //     dbg!(&slot_entry, &entry.agent_span);
            // }
            base + slot
        } else {
            base
        };

        let patches: SmallVec<[SimpleTextOp; 2]> = entry.ops.into_iter().map(|op| op.into()).merge_spans().collect();

        // if patches.iter().map(|p| p.ins_content.len() + p.del_len).sum::<usize>() > 1 {
        //     dbg!(&patches);
        // }

        let i = txns.len();
        txns.push(TraceExportTxn {
            parents: entry.parents.iter().map(|v| *idx_for_v.get(v).unwrap()).collect(),
            num_children: 0,
            agent,
            time: timestamp,
            patches,
            _dt_span: [entry.span.start, entry.span.end],
        });

        for p in entry.parents.iter() {
            let parent_idx = *idx_for_v.get(p).unwrap();
            txns[parent_idx].num_children += 1;
        }

        let old_entry = idx_for_v.insert(entry.span.last(), i);
        assert!(old_entry.is_none());
    }

    if let Some((_, rest)) = txns.split_last_mut() {
        if rest.iter().any(|r| r.num_children == 0) {
            // The transaction list contains multiple items with no children. These items need to
            // be merged together in the final result. We will produce a "dummy" txn which merges
            // all previously un-merged children.
            let mut txn = TraceExportTxn {
                parents: smallvec![],
                num_children: 0,
                agent: 0,
                time: Default::default(),
                patches: smallvec![],
                _dt_span: [0, 0],
            };

            for (i, r) in rest.iter_mut().enumerate() {
                if r.num_children == 0 {
                    r.num_children += 1;
                    txn.parents.push(i);
                }
            }

            assert!(txn.parents.len() >= 2);
            txns.push(txn);
        }
    }

    let end_content = oplog.checkout_tip().into_inner().to_string();
    TraceExportData {
        kind: "concurrent",
        end_content,
        num_agents,
        txns,
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DTExportTxn {
    /// The LV span of the txn. Note the agent seq span is not exported.
    span: DTRange,
    parents: SmallVec<[usize; 2]>,
    agent: SmartString,
    seq_start: usize,
    // op: TextOperation,
    ops: SmallVec<[SimpleTextOp; 2]>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DTExport {
    txns: Vec<DTExportTxn>,
    end_content: String,
}

fn export_oplog_to_json(oplog: &ListOpLog) -> Vec<DTExportTxn> {
    let mut txns = vec![];

    for entry in oplog.as_chunked_operation_vec().into_iter() {
        txns.push(DTExportTxn {
            span: entry.span,
            parents: entry.parents.0.clone(),
            agent: oplog.get_agent_name(entry.agent_span.agent).into(),
            seq_start: entry.agent_span.seq_range.start,
            ops: entry.ops.into_iter().map(|op| op.into()).collect(),
        });
    }

    txns
}

pub fn export_full_to_json(oplog: &ListOpLog) -> DTExport {
    DTExport {
        txns: export_oplog_to_json(oplog),
        end_content: oplog.checkout_tip().content().to_string(),
    }
}

// pub fn run_export(data: &DTExport) {
//     // First make an oplog from the exported data.
//     let mut oplog = ListOpLog::new();
//     for txn in &data.txns {
//         let ops: Vec<TextOperation> = txn.ops.iter().map(|op| op.into()).collect();
//         let agent = oplog.get_or_create_agent_id(txn.agent.as_str());
//         oplog.add_operations_at(agent, txn.parents.as_slice(), &ops);
//     }
//
//     assert_eq!(oplog.checkout_tip().content(), data.end_content);
// }

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSimpleExportData {
    start_content: SmartString,
    end_content: String,
    txns: Vec<TraceSimpleExportTxn>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSimpleExportTxn {
    time: DateTime<FixedOffset>,
    patches: SmallVec<[SimpleTextOp; 4]>,
}

pub fn export_transformed(oplog: &ListOpLog, timestamp_filename: Option<OsString>, shatter: bool) -> TraceSimpleExportData {
    // The file format stores a set of transactions, and each transaction stores a list of patches.
    // It would be really simple to just export everything into one big transaction, but thats a bit
    // lazy.
    //
    // Instead, I'm splitting up the transactions along user agent boundaries.
    //
    // Note that the order that we traverse the operations here may be different from the order
    // that we export things in the export function above.
    let timestamps = timestamp_filename.map(Timestamps::from_file);

    let mut txns = vec![];
    // let timestamp: SmartString = timestamp.into();

    let mut current_txn = TraceSimpleExportTxn {
        // time: timestamp.clone(),
        time: Default::default(),
        patches: smallvec![],
    };
    let mut last_agent: Option<&str> = None;
    // let mut last_timestamp: Option<()> = None;

    for (range, op) in oplog.iter_xf_operations() {
        let Some(mut op) = op else { continue; };

        // oplog.cg.agent_assignment.g

        let mut iter = oplog.cg.agent_assignment.iter_remote_mappings_range(range).take_max();

        while iter.peek().is_some() {
            // let x = iter.next()
            let (timestamp, RemoteVersionSpan(agent, seq_range)) = if let Some(ts) = timestamps.as_ref() {
                // Take as many items from the entry that have the same exact timestamp.
                let (ts, ts_len) = ts.get_rv_range(iter.peek().unwrap().clone());
                (ts, iter.next(ts_len).unwrap())
            } else {
                (Default::default(), iter.next(if shatter { 1 } else { usize::MAX }).unwrap())
            };

            if current_txn.time == DateTime::<FixedOffset>::default() {
                current_txn.time = timestamp;
            }

            let can_append = (current_txn.patches.is_empty() || timestamps.is_some() || !shatter)
                && (last_agent == Some(agent) || last_agent == None)
                && current_txn.time == timestamp;

            let op_here = op.truncate_keeping_right(seq_range.len());

            if !can_append {
                // Flush current_txn to the txns list and clear it.
                assert!(!current_txn.patches.is_empty());
                txns.push(current_txn);
                current_txn = TraceSimpleExportTxn {
                    time: timestamp,
                    patches: smallvec![],
                };
            }

            current_txn.patches.push_rle(op_here.into());

            last_agent = Some(agent);
        }
    }

    if !current_txn.patches.is_empty() {
        txns.push(current_txn);
    }

    for t in txns.iter_mut() {
        t.time = t.time.trunc_subsecs(0);
    }

    // for x in txns.iter().filter(|txn| txn.patches.iter().map(|p| p.del_len + p.ins_content.len()).sum::<usize>() > 1) {
    //     dbg!(x);
    // }

    let end_content = oplog.checkout_tip().into_inner().to_string();
    TraceSimpleExportData {
        start_content: Default::default(),
        end_content,
        txns,
    }
}
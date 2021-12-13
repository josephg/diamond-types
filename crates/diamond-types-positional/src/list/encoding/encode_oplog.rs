use std::cmp::Ordering::*;
use smallvec::SmallVec;
use rle::HasLength;
use rle::zip::rle_zip;
use crate::list::encoding::*;
use crate::list::history::{HistoryEntry, MinimalHistoryEntry};
use crate::list::operation::{InsDelTag, Operation};
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::list::{Frontier, OpLog, Time};
use crate::list::frontier::frontier_is_root;
use crate::rle::{KVPair, RleVec};
use crate::{AgentId, ROOT_TIME};
use crate::localtime::TimeSpan;


// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
// pub struct EditRun {
//     cursor_diff: isize, // Cursor movement from previous item
//     len: usize,
//     tag: InsDelTag,
//     reversed: bool,
//     has_content: bool,
// }
//
// impl HasLength for EditRun {
//     fn len(&self) -> usize { self.len }
// }
//
// impl MergableSpan for EditRun {
//     fn can_append(&self, other: &Self) -> bool {
//         self.tag == other.tag && self.has_content == other.has_content
//     }
//
//     fn append(&mut self, other: Self) {
//         todo!()
//     }
// }

fn write_op(dest: &mut Vec<u8>, op: &Operation, cursor: &mut usize) {
    // Note I'm relying on the operation log itself to be iter_merged, which simplifies things here
    // greatly.

    // This is a bit of a tradeoff. Sometimes when items get split, they retain their reversed tag.
    // We could store .reversed for all operations (including when length=1) and pick a reversed
    // flag here which minimizes the cursor deltas. But that approach results in more complexity and
    // worse filesize overall.
    // let reversed = !op.fwd && op.len > 1;
    let fwd = op.fwd || op.len == 1;

    // let reversed = op.reversed;
    // if op.len == 1 { assert!(!op.reversed); }

    // let op_start = op.pos;
    let op_start = if op.tag == Del && !fwd {
        op.pos + op.len
    } else {
        op.pos
    };

    // let op_end = op.pos;
    let op_end = if op.tag == Ins && fwd {
        op.pos + op.len
    } else {
        op.pos
    };

    let cursor_diff = isize::wrapping_sub(op_start as isize, *cursor as isize);
    *cursor = op_end;

    // println!("pos {} diff {} {:?} rev {} len {}", op.pos cursor_movement, op.tag, reversed, op.len);

    // if op.len != 1 { len_total += op.len as u64; }
    // diff_zig_total += num_encode_zigzag_i64(cursor_movement);
    // diff_zig_total += cursor_diff.abs() as u64;
    // num_ops += 1;

    // So generally about 40% of changes have length of 1, and about 40% of changes (even
    // after RLE) happen without the cursor moving.
    let mut buf = [0u8; 20];
    let mut pos = 0;

    // TODO: Make usize variants of all of this and use that rather than u64 / i64.
    let mut n = if op.len != 1 {
        let mut n = op.len;
        // When len == 1, the item is never considered reversed.
        if op.tag == Del { n = mix_bit_usize(n, fwd) };
        n
    } else if cursor_diff != 0 {
        num_encode_zigzag_isize(cursor_diff)
    } else {
        0
    };

    n = mix_bit_usize(n, op.tag == Del);
    n = mix_bit_usize(n, cursor_diff != 0);
    n = mix_bit_usize(n, op.len != 1);
    pos += encode_usize(n, &mut buf[pos..]);

    if op.len != 1 && cursor_diff != 0 {
        let mut n2 = num_encode_zigzag_isize(cursor_diff);
        pos += encode_usize(n2, &mut buf[pos..]);
    }

    dest.extend_from_slice(&buf[..pos]);
}

// We need to name the full branch in the output in a few different settings.
//
// TODO: Should this store strings or IDs?
fn write_full_frontier(oplog: &OpLog, dest: &mut Vec<u8>, frontier: &[Time]) {
    if frontier_is_root(frontier) {
        // The root is written as a single item.
        push_str(dest, "ROOT");
        push_usize(dest, 0);
    } else {
        let mut iter = frontier.iter().peekable();
        while let Some(t) = iter.next() {
            let has_more = iter.peek().is_some();
            let id = oplog.time_to_crdt_id(*t);

            push_str(dest, oplog.client_data[id.agent as usize].name.as_str());

            let n = mix_bit_usize(id.seq, has_more);
            push_usize(dest, n);
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodeOptions {
    // NYI.
    // pub from_frontier: Frontier,

    pub store_inserted_content: bool,

    // NYI.
    pub store_deleted_content: bool,

    pub verbose: bool,
}

impl Default for EncodeOptions {
    fn default() -> Self {
        Self {
            store_inserted_content: true,
            store_deleted_content: false,
            verbose: false
        }
    }
}

#[derive(Debug, Clone)]
struct AgentMapping {
    map: Vec<Option<AgentId>>,
    next_mapped_agent: AgentId,
    output: Vec<u8>,
}

impl AgentMapping {
    fn new(oplog: &OpLog) -> Self {
        let client_len = oplog.client_data.len();
        let mut result = Self {
            map: Vec::with_capacity(client_len),
            next_mapped_agent: 0,
            output: Vec::new()
        };
        result.map.resize(client_len, None);
        result
    }

    fn map(&mut self, oplog: &OpLog, agent: AgentId) -> AgentId {
        let agent = agent as usize;
        self.map[agent].unwrap_or_else(|| {
            let mapped = self.next_mapped_agent;
            self.map[agent] = Some(mapped);
            push_str(&mut self.output, oplog.client_data[agent].name.as_str());
            // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
            self.next_mapped_agent += 1;
            mapped
        })
    }
}

impl OpLog {
    /// Encode the data stored in the OpLog into a (custom) compact binary form suitable for saving
    /// to disk, or sending over the network.
    pub fn encode_from(&self, opts: EncodeOptions, from_frontier: &[Time]) -> Vec<u8> {
        let mut result = Vec::new();
        // The file starts with MAGIC_BYTES
        result.extend_from_slice(&MAGIC_BYTES_SMALL);

        // And contains a series of chunks. Each chunk has a chunk header (chunk type, length).
        // The first chunk is always the FileInfo chunk - which names the file format.
        let mut write_chunk = |c: Chunk, data: &[u8]| {
            if opts.verbose {
                println!("{:?} length {}", c, data.len());
            }
            // dbg!(&data);
            push_chunk(&mut result, c, &data);
        };

        // TODO: The fileinfo chunk should specify DT version, encoding version and information
        // about the data types we're encoding.
        write_chunk(Chunk::FileInfo, &[]);

        let mut buf = Vec::new();

        // We'll name the starting frontier for the file. TODO: Support partial data sets.
        // TODO: Consider moving this into the FileInfo chunk.
        write_full_frontier(self, &mut buf, from_frontier);
        write_chunk(Chunk::StartFrontier, &buf);
        buf.clear();

        // // TODO: This is redundant. Do I want to keep this or what?
        // write_full_frontier(self, &mut buf, &self.frontier);
        // write_chunk(Chunk::EndFrontier, &buf);
        // buf.clear();



        // *** Inserted (text) content and operations ***

        // There's two ways I could iterate through the operations:
        //
        // 1. In local operation order. Each operation at that operation's local time. This is much
        //    simpler and faster - since we're essentially just copying oplog into the file.
        // 2. In optimized order. This would use txn_trace to reorder the operations in the
        //    operation log to maximize runs (and thus minimize file size). At some point I'd like
        //    to do this optimization - but I'm not sure where. (Maybe we should optimize in-place?)

        // Note I'm going to push the text of all insert operations separately from the operation
        // data itself.
        //
        // Note for now this includes text that was later deleted. It is also in time-order not
        // document-order.
        //
        // Another way of storing this content would be to interleave it with the operations
        // themselves. That would work fine but:
        //
        // - The interleaved approach would be a bit more complex when dealing with other (non-text)
        //   data types.
        // - Interleaved would result in a slightly smaller file size (tens of bytes smaller)
        // - Interleaved would be easier to consume, because we wouldn't need to match up inserts
        //   with the text
        // - Interleaved it would compress much less well with snappy / lz4.


        let mut inserted_text = String::new();
        let mut deleted_text = String::new();

        // Map from old agent ID -> new agent ID in the file.
        //
        // (Agent ID 0 is reserved for ROOT, to make special parents slightly simpler.)
        let mut agent_mapping = AgentMapping::new(self);

        // let mut agent_assignment_chunk = SpanWriter::new(push_run_u32);
        let mut agent_assignment_chunk = Vec::new();
        let mut agent_assignment_writer = Merger::new(|run, _| {
            push_run_u32(&mut agent_assignment_chunk, run);
        });

        let mut ops_chunk = Vec::new();
        let mut last_cursor_pos: usize = 0;
        let mut ops_writer = Merger::new(|op, _| {
            write_op(&mut ops_chunk, &op, &mut last_cursor_pos);
        });

        // Parents are always smaller than the item itself (txn.span.start). So we can build a txn
        // map while we go showing how the incoming txns and outgoing txns connect together.
        //
        // Each entry's key is the internal local time, and the value (entry.1) is the range in the
        // output.
        let mut txn_map = RleVec::<KVPair<TimeSpan>>::new();
        let mut next_output_time = 0;
        let mut txns_chunk = Vec::new();
        let mut txns_writer = Merger::new(|txn: MinimalHistoryEntry, agent_mapping: &mut AgentMapping| {
            // println!("Upstream {}-{}", txn.span.start, txn.span.end);
            // First add this entry to the txn map.
            let len = txn.span.len();
            let output_range = (next_output_time .. next_output_time + len).into();
            // txn_map.push(KVPair(txn.span.start, output_range));
            txn_map.insert(KVPair(txn.span.start, output_range));
            next_output_time = output_range.end;

            push_usize(&mut txns_chunk, len);

            // Then the parents.
            let mut iter = txn.parents.iter().peekable();
            while let Some(&p) = iter.next() {
                let p = p; // intellij bug
                let has_more = iter.peek().is_some();

                let mut write_parent_diff = |mut n: usize, is_foreign: bool| {
                    n = mix_bit_usize(n, has_more);
                    n = mix_bit_usize(n, is_foreign);
                    push_usize(&mut txns_chunk, n);
                };

                // Parents are either local or foreign. Local changes are changes we've written
                // (already) to the file. And foreign changes are changes that point outside the
                // local part of the DAG we're sending.
                //
                // Most parents will be local.
                if p == ROOT_TIME {
                    // ROOT is special cased, since its foreign but we don't put the root item in
                    // the agent list. (Though we could!)
                    // This is written as "agent 0", and with no seq value (since thats not needed).
                    write_parent_diff(0, true);
                } else if let Some((map, offset)) = txn_map.find_with_offset(p) {
                    // Local change!
                    let mapped_parent = map.1.start + offset;

                    write_parent_diff(output_range.start - mapped_parent, false);
                } else {
                    // Foreign change
                    // println!("Region does not contain parent for {}", p);

                    let item = self.time_to_crdt_id(p);
                    let mapped_agent = agent_mapping.map(self, item.agent);

                    // There are probably more compact ways to do this, but the txn data set is
                    // usually quite small anyway, even in large histories. And most parents objects
                    // will be in the set anyway. So I'm not too concerned about a few extra bytes
                    // here.
                    //
                    // I'm adding 1 to the mapped agent to make room for ROOT. This is quite dirty!
                    write_parent_diff(mapped_agent as usize + 1, true);
                    push_usize(&mut txns_chunk, item.seq);
                }
            }
        });


        // If we just iterate in the current order, this code would be way simpler :p
        // let iter = self.history.optimized_txns_between(from_frontier, &self.frontier);
        // for walk in iter {
        for walk in self.history.optimized_txns_between(from_frontier, &self.frontier) {
            // We only care about walk.consume and parents.

            // We need to update *lots* of stuff in here!!

            // 1. Agent names and agent assignment
            for span in self.client_with_localtime.iter_range_packed(walk.consume) {
                // Mark the agent as in-use (if we haven't already)
                let mapped_agent = agent_mapping.map(self, span.1.agent);
                // dbg!(&span, mapped_agent);

                // agent_assignment is a list of (agent, len) pairs.
                agent_assignment_writer.push(Run {
                    val: mapped_agent,
                    len: span.len()
                });
            }

            // 2. Operations!
            for ops in self.operations.iter_range_packed(walk.consume) {
                let op = ops.1;

                if op.tag == Ins && opts.store_inserted_content {
                    assert!(op.content_known);
                    inserted_text.push_str(&op.content);
                }

                if op.tag == Del && op.content_known && opts.store_deleted_content {
                    deleted_text.push_str(&op.content);
                }

                ops_writer.push(op);
            }

            // 3. Parents!
            txns_writer.push2(MinimalHistoryEntry {
                span: walk.consume,
                parents: walk.parents
            }, &mut agent_mapping);
        }

        agent_assignment_writer.flush();
        ops_writer.flush();
        txns_writer.flush2(&mut agent_mapping);

        write_chunk(Chunk::AgentNames, &agent_mapping.output);
        write_chunk(Chunk::AgentAssignment, &agent_assignment_chunk);

        if opts.store_inserted_content {
            write_chunk(Chunk::InsertedContent, &inserted_text.as_bytes());
        }
        if opts.store_deleted_content {
            write_chunk(Chunk::DeletedContent, &deleted_text.as_bytes());
        }
        write_chunk(Chunk::PositionalPatches, &ops_chunk);
        write_chunk(Chunk::TimeDAG, &txns_chunk);

        if opts.verbose {
            println!("== Total length {}", result.len());
        }

        result
    }

    pub fn encode(&self, opts: EncodeOptions) -> Vec<u8> {
        self.encode_from(opts, &[ROOT_TIME])
    }

    /// Encode the data stored in the OpLog into a (custom) compact binary form suitable for saving
    /// to disk, or sending over the network.
    pub fn encode_old(&self, opts: EncodeOptions) -> Vec<u8> {
        let mut result = Vec::new();
        // The file starts with MAGIC_BYTES
        result.extend_from_slice(&MAGIC_BYTES_SMALL);

        // And contains a series of chunks. Each chunk has a chunk header (chunk type, length).
        // The first chunk is always the FileInfo chunk - which names the file format.
        let mut write_chunk = |c: Chunk, data: &[u8]| {
            if opts.verbose {
                println!("{:?} length {}", c, data.len());
            }
            push_chunk(&mut result, c, &data);
        };

        // TODO: The fileinfo chunk should specify DT version, encoding version and information
        // about the data types we're encoding.
        write_chunk(Chunk::FileInfo, &[]);

        let mut buf = Vec::new();

        // We'll name the starting frontier for the file. TODO: Support partial data sets.
        // TODO: Consider moving this into the FileInfo chunk.
        write_full_frontier(self, &mut buf, &[ROOT_TIME]);
        write_chunk(Chunk::StartFrontier, &buf);
        buf.clear();

        // // TODO: This is redundant. Do I want to keep this or what?
        // write_full_frontier(self, &mut buf, &self.frontier);
        // write_chunk(Chunk::EndFrontier, &buf);
        // buf.clear();

        // The AgentAssignment data indexes into the agents named here.
        for client_data in self.client_data.iter() {
            push_str(&mut buf, client_data.name.as_str());
        }
        write_chunk(Chunk::AgentNames, &buf);
        buf.clear();

        // List of (agent, len) pairs for all changes.
        for KVPair(_, span) in self.client_with_localtime.iter() {
            push_run_u32(&mut buf, Run { val: span.agent, len: span.len() });
        }
        write_chunk(Chunk::AgentAssignment, &buf);
        buf.clear();

        // *** Inserted (text) content and operations ***

        // There's two ways I could iterate through the operations:
        //
        // 1. In local operation order. Each operation at that operation's local time. This is much
        //    simpler and faster - since we're essentially just copying oplog into the file.
        // 2. In optimized order. This would use txn_trace to reorder the operations in the
        //    operation log to maximize runs (and thus minimize file size). At some point I'd like
        //    to do this optimization - but I'm not sure where. (Maybe we should optimize in-place?)

        // Note I'm going to push the text of all insert operations separately from the operation
        // data itself.
        //
        // Note for now this includes text that was later deleted. It is also in time-order not
        // document-order.
        //
        // Another way of storing this content would be to interleave it with the operations
        // themselves. That would work fine but:
        //
        // - The interleaved approach would be a bit more complex when dealing with other (non-text)
        //   data types.
        // - Interleaved would result in a slightly smaller file size (tens of bytes smaller)
        // - Interleaved would be easier to consume, because we wouldn't need to match up inserts
        //   with the text
        // - Interleaved it would compress much less well with snappy / lz4.
        let mut inserted_text = String::new();
        let mut deleted_text = String::new();

        // The cursor position of the previous edit. We're differential, baby.
        let mut last_cursor_pos: usize = 0;
        for KVPair(_, op) in self.operations.iter_merged() {
            // For now I'm ignoring known content in delete operations.
            if op.tag == Ins {
                assert!(op.content_known);
                inserted_text.push_str(&op.content);
            }

            if op.tag == Del && op.content_known && opts.store_deleted_content {
                deleted_text.push_str(&op.content);
            }

            write_op(&mut buf, &op, &mut last_cursor_pos);
        }
        if opts.store_inserted_content {
            write_chunk(Chunk::InsertedContent, &inserted_text.as_bytes());
        }
        if opts.store_deleted_content {
            write_chunk(Chunk::DeletedContent, &deleted_text.as_bytes());
        }
        write_chunk(Chunk::PositionalPatches, &buf);

        // println!("{}", inserted_text);

        // if opts.verbose {
            // dbg!(len_total, diff_zig_total, num_ops);
            // println!("op_data.len() {}", &op_data.len());
            // println!("inserted text length {}", inserted_text.len());
            // println!("deleted text length {}", deleted_text.len());
        // }

        buf.clear();

        for txn in self.history.entries.iter() {
            // First add this entry to the txn map.
            push_usize(&mut buf, txn.len());

            // Then the parents.
            let mut iter = txn.parents.iter().peekable();
            while let Some(&p) = iter.next() {
                let p = p; // intellij bug
                let has_more = iter.peek().is_some();

                let mut write_parent_diff = |mut n: usize, is_foreign: bool| {
                    n = mix_bit_usize(n, has_more);
                    n = mix_bit_usize(n, is_foreign);
                    push_usize(&mut buf, n);
                };

                // Parents are either local or foreign. Local changes are changes we've written
                // (already) to the file. And foreign changes are changes that point outside the
                // local part of the DAG we're sending.
                //
                // Most parents will be local.
                if p == ROOT_TIME {
                    // ROOT is special cased, since its foreign but we don't put the root item in
                    // the agent list. (Though we could!)
                    // This is written as "agent 0", and with no seq value (since thats not needed).
                    write_parent_diff(0, true);
                } else {
                    // Local change!
                    write_parent_diff(txn.span.start - p, false);
                }
            }
            // write_history_entry(&mut buf, txn);
        }
        write_chunk(Chunk::TimeDAG, &buf);
        buf.clear();

        if opts.verbose {
            println!("== Total length {}", result.len());
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::list::encoding::EncodeOptions;
    use crate::list::ListCRDT;
    use crate::ROOT_TIME;

    #[test]
    fn encoding_smoke_test() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");

        let d1 = doc.ops.encode_old(EncodeOptions::default());
        let d2 = doc.ops.encode(EncodeOptions::default());
        assert_eq!(d1, d2);
        // let data = doc.ops.encode_old(EncodeOptions::default());
        // dbg!(data.len(), data);
    }

    #[test]
    fn encode_from_version() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        let _t1 = doc.local_insert(0, 0, "hi from seph!\n");
        let _t2 = doc.local_insert(1, 0, "hi from mike!\n");

        // let data = doc.ops.encode_from(EncodeOptions::default(), &[ROOT_TIME]);
        let data = doc.ops.encode_from(EncodeOptions::default(), &[_t1]);
        // let data = doc.ops.encode_from(EncodeOptions::default(), &[_t2]);
        dbg!(data);
        // let data = doc.ops.encode_old(EncodeOptions::default());
        // dbg!(data.len(), data);
    }
}
use jumprope::JumpRope;
use rle::{HasLength, RleRun};
use crate::list::encoding::*;
use crate::list::history::MinimalHistoryEntry;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::list::{Branch, OpLog, switch, Time};
use crate::rle::{KVPair, RleVec};
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use crate::list::frontier::local_version_is_root;
use crate::list::internal_op::OperationInternal;
use crate::list::operation::InsDelTag;
use crate::localtime::TimeSpan;

const ALLOW_VERBOSE: bool = false;

/// Write an operation to the passed writer.
fn write_op(dest: &mut Vec<u8>, op: &OperationInternal, cursor: &mut usize) {
    // Note I'm relying on the operation log itself to be iter_merged, which simplifies things here
    // greatly.

    // This is a bit of a tradeoff. Sometimes when items get split, they retain their reversed tag.
    // We could store .reversed for all operations (including when length=1) and pick a reversed
    // flag here which minimizes the cursor deltas. But that approach results in more complexity and
    // worse filesize overall.
    // let reversed = !op.fwd && op.len > 1;
    let fwd = op.span.fwd || op.len() == 1;

    // let reversed = op.reversed;
    // if op.len == 1 { assert!(!op.reversed); }

    // For some reason the compiler slightly prefers this code to the match below. O_o
    let op_start = if op.tag == Del && !fwd {
        op.end()
    } else {
        op.start()
    };

    let op_end = if op.tag == Ins && fwd {
        op.end()
    } else {
        op.start()
    };

    // Code above is equivalent to this:
    // let (op_start, op_end) = match (op.tag, fwd) {
    //     (Ins, true) => (op.start(), op.end()),
    //     (Del, false) => (op.end(), op.start()),
    //     (_, _) => (op.start(), op.start()),
    // };


    let cursor_diff = isize::wrapping_sub(op_start as isize, *cursor as isize);
    // dbg!((op, op_start, op_end, *cursor, cursor_diff));
    *cursor = op_end;

    // println!("pos {} diff {} {:?} rev {} len {}", op.pos cursor_movement, op.tag, reversed, op.len);

    // So generally about 40% of changes have length of 1, and about 40% of changes (even
    // after RLE) happen without the cursor moving.
    let mut buf = [0u8; 20];
    let mut pos = 0;

    // TODO: Make usize variants of all of this and use that rather than u64 / i64.
    let len = op.len();
    let mut n = if len != 1 {
        let mut n = len;
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
    n = mix_bit_usize(n, len != 1);
    pos += encode_usize(n, &mut buf[pos..]);

    if len != 1 && cursor_diff != 0 {
        let n2 = num_encode_zigzag_isize(cursor_diff);
        pos += encode_usize(n2, &mut buf[pos..]);
    }

    dest.extend_from_slice(&buf[..pos]);
}


#[derive(Debug, Clone)]
pub struct EncodeOptions<'a> {
    pub user_data: Option<&'a [u8]>,

    // NYI.
    // pub from_version: LocalVersion,

    pub store_start_branch_content: bool,

    pub store_inserted_content: bool,
    pub store_deleted_content: bool,

    pub verbose: bool,
}

pub const ENCODE_PATCH: EncodeOptions = EncodeOptions {
    user_data: None,
    store_start_branch_content: false,
    store_inserted_content: true,
    store_deleted_content: false,
    verbose: false
};

pub const ENCODE_FULL: EncodeOptions = EncodeOptions {
    user_data: None,
    store_start_branch_content: true,
    store_inserted_content: true,
    store_deleted_content: false, // ?? Not sure about this one!
    verbose: false
};

// impl<'a> EncodeOptions<'a> {
//     pub fn full
// }

impl<'a> Default for EncodeOptions<'a> {
    fn default() -> Self {
        ENCODE_FULL
    }
}

#[derive(Debug, Copy, Clone)]
struct AgentAssignmentRun {
    agent: AgentId,
    delta: isize,
    len: usize,
}

impl MergableSpan for AgentAssignmentRun {
    fn can_append(&self, other: &Self) -> bool {
        self.agent == other.agent && other.delta == 0
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

impl HasLength for AgentAssignmentRun {
    fn len(&self) -> usize {
        self.len
    }
}

fn write_assignment_run(dest: &mut Vec<u8>, run: AgentAssignmentRun) {
    // Its rare, but possible for the agent assignment sequence to jump around a little.
    // This can happen when:
    // - The sequence numbers are shared with other documents, and hence the seqs are sparse
    // - Or the same agent made concurrent changes to multiple branches. The operations may
    //   be reordered to any order which obeys the time dag's partial order.
    let mut buf = [0u8; 25];
    let mut pos = 0;

    // I tried adding an extra bit field to mark len != 1 - so we can skip encoding the
    // length. But in all the data sets I've looked at, len is so rarely 1 that it increased
    // filesize.
    let has_jump = run.delta != 0;

    // dbg!(run);
    let n = mix_bit_u32(run.agent, has_jump);
    pos += encode_u32(n, &mut buf);
    pos += encode_usize(run.len, &mut buf[pos..]);

    if has_jump {
        pos += encode_i64(run.delta as i64, &mut buf[pos..]);
    }

    dest.extend_from_slice(&buf[..pos]);
}

#[derive(Debug, Clone)]
struct AgentMapping {
    /// Map from oplog's agent ID to the agent id in the file. Paired with the last assigned agent
    /// ID, to support agent IDs bouncing around.
    map: Vec<Option<(AgentId, usize)>>,
    next_mapped_agent: AgentId,
    output: Vec<u8>,
}

impl AgentMapping {
    fn new(oplog: &OpLog) -> Self {
        let client_len = oplog.client_data.len();
        let mut result = Self {
            map: Vec::with_capacity(client_len),
            next_mapped_agent: 1, // 0 is implicitly assigned to ROOT.
            output: Vec::new()
        };
        result.map.resize(client_len, None);
        result
    }

    fn map(&mut self, oplog: &OpLog, agent: AgentId) -> AgentId {
        // 0 is implicitly ROOT.
        if agent == ROOT_AGENT { return 0; }
        let agent = agent as usize;

        self.map[agent].map_or_else(|| {
            let mapped = self.next_mapped_agent;
            self.map[agent] = Some((mapped, 0));
            push_str(&mut self.output, oplog.client_data[agent].name.as_str());
            // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
            self.next_mapped_agent += 1;
            mapped
        }, |v| v.0)
    }

    fn seq_delta(&mut self, agent: AgentId, span: TimeSpan) -> isize {
        let item = self.map[agent as usize].as_mut().unwrap();
        let old_seq = item.1;
        item.1 = span.end;
        (span.start as isize) - (old_seq as isize)
    }

    fn consume(self) -> Vec<u8> {
        self.output
    }
}

// // We need to name the full branch in the output in a few different settings.
// //
// // TODO: Should this store strings or IDs?
// fn write_full_frontier(oplog: &OpLog, dest: &mut Vec<u8>, frontier: &[Time]) {
//     if frontier_is_root(frontier) {
//         // The root is written as a single item.
//         push_str(dest, "ROOT");
//         push_usize(dest, 0);
//     } else {
//         let mut iter = frontier.iter().peekable();
//         while let Some(t) = iter.next() {
//             let has_more = iter.peek().is_some();
//             let id = oplog.time_to_crdt_id(*t);
//
//             push_str(dest, oplog.client_data[id.agent as usize].name.as_str());
//
//             let n = mix_bit_usize(id.seq, has_more);
//             push_usize(dest, n);
//         }
//     }
// }


fn write_frontier(dest: &mut Vec<u8>, frontier: &[Time], map: &mut AgentMapping, oplog: &OpLog) {
    // I'm sad that I need the buf here + copying. It'd be faster if it was zero-copy.
    let mut buf = Vec::new();
    let mut iter = frontier.iter().peekable();
    while let Some(t) = iter.next() {
        let has_more = iter.peek().is_some();
        let id = oplog.time_to_crdt_id(*t);

        // (Mapped agent ID, seq) pairs. Agent id has mixed in bit for has_more.
        let mapped = map.map(oplog, id.agent);
        let n = mix_bit_usize(mapped as _, has_more);
        push_usize(&mut buf, n);
        push_usize(&mut buf, id.seq);
    }
    push_chunk(dest, ChunkType::Version, &buf);
    buf.clear();
}

fn write_content_rope(dest: &mut Vec<u8>, rope: &JumpRope) {
    // This content type uses up the entire chunk, so there's no need to store a length along with
    // the string.
    let mut buf = Vec::new(); // :(
    push_u32(&mut buf, DataType::PlainText as _);
    // push_usize(&mut buf, rope.len_bytes());
    for (str, _) in rope.chunks() {
        buf.extend_from_slice(str.as_bytes());
    }
    push_chunk(dest, ChunkType::Content, &buf);
}

fn write_chunk_str(dest: &mut Vec<u8>, s: &str, chunk_type: ChunkType) {
    let mut buf = Vec::new(); // :(
    push_u32(&mut buf, DataType::PlainText as _);
    // push_str(&mut buf, s);
    buf.extend_from_slice(s.as_bytes());
    push_chunk(dest, chunk_type, &buf);
}

// TODO:
// #[allow(unused)]
// fn write(&self, dest: &mut Vec<u8>, map: &mut AgentMapping, oplog: &OpLog, write_content: bool) {
//     // Frontier
//     Self::write_frontier(dest, &self.frontier, map, oplog);
//
//     // Content
//     if write_content {
//         Self::write_content_rope(dest, &self.content);
//     }
// }

fn write_bit_run(run: RleRun<bool>, into: &mut Vec<u8>) {
    // dbg!(run);
    let mut n = run.len;
    n = mix_bit_usize(n, run.val);
    push_usize(into, n);
}

/// Simple helper struct for content (ins / del) chunks. These have two parts:
/// - A RLE bit vector describing which elements of the specified type have known lengths
/// - The data itself
///
/// Its gross that I need to pass a generic parameter here, since it'll always be write_bit_run.
/// I wish there were a cleaner way to write this.
struct ContentChunk<F: FnMut(RleRun<bool>, &mut Vec<u8>)> {
    tag: InsDelTag,
    known_out: Vec<u8>,
    bit_writer: Merger<RleRun<bool>, F, Vec<u8>>,
    content: String
}

// impl<F: FnMut(S, &mut Vec<u8>)> ContentChunk<F> {
impl<F: FnMut(RleRun<bool>, &mut Vec<u8>)> ContentChunk<F> {
    fn new(f: F, tag: InsDelTag) -> Self {
        Self {
            tag,
            known_out: Vec::new(),
            bit_writer: Merger::new(f),
            content: String::new(),
        }
    }

    fn push(&mut self, content: Option<&str>, len: usize) {
        let known = if let Some(content) = content {
            self.content.push_str(content);
            true
        } else {
            false
        };

        self.bit_writer.push2(RleRun::new(known, len), &mut self.known_out);
    }

    fn flush(mut self) -> Option<Vec<u8>> {
        self.bit_writer.flush2(&mut self.known_out);

        if self.content.is_empty() {
            None
        } else {
            let mut buf = Vec::new();
            // Operation type
            push_u32(&mut buf, match self.tag { Ins => 0, Del => 1 });

            // This writes a length-prefixed string, which it really doesn't need to do.
            write_chunk_str(&mut buf, &self.content, ChunkType::Content);
            // push_chunk(&mut buf, ChunkType::Content, self.content.as_bytes());
            push_chunk(&mut buf, ChunkType::ContentKnown, &self.known_out);
            Some(buf)
        }
    }
}

impl OpLog {
    /// Encode the data stored in the OpLog into a (custom) compact binary form suitable for saving
    /// to disk, or sending over the network.
    pub fn encode_from(&self, opts: EncodeOptions, from_version: &[Time]) -> Vec<u8> {
        // if !frontier_is_root(from_frontier) {
        //     unimplemented!("Encoding from a non-root frontier is not implemented");
        // }
        let verbose = ALLOW_VERBOSE && opts.verbose;

        let mut result = Vec::new();
        // The file starts with MAGIC_BYTES
        result.extend_from_slice(&MAGIC_BYTES);
        push_usize(&mut result, PROTOCOL_VERSION);

        // And contains a series of chunks. Each chunk has a chunk header (chunk type, length).
        // The first chunk is always the FileInfo chunk - which names the file format.
        let mut write_chunk = |c: ChunkType, data: &mut Vec<u8>| {
            if verbose {
                println!("{:?} length {}", c, data.len());
            }
            // dbg!(&data);
            push_chunk(&mut result, c, data.as_slice());
            data.clear();
        };

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

        let mut inserted_content = if opts.store_inserted_content {
            Some(ContentChunk::new(write_bit_run, Ins))
        } else { None };
        let mut deleted_content = if opts.store_deleted_content {
            Some(ContentChunk::new(write_bit_run, Del))
        } else { None };

        // Map from old agent ID -> new agent ID in the file.
        //
        // (Agent ID 0 is reserved for ROOT, to make special parents slightly simpler.)
        let mut agent_mapping = AgentMapping::new(self);

        // let mut agent_assignment_chunk = SpanWriter::new(push_run_u32);
        let mut agent_assignment_chunk = Vec::new();
        let mut agent_assignment_writer = Merger::new(|run: AgentAssignmentRun, _| {
            write_assignment_run(&mut agent_assignment_chunk, run);
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
                // let p = p; // intellij bug
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
                    // TODO: There's a sort of bug here. Local parents should (probably?) be sorted
                    // in the file, but this mapping doesn't guarantee that. Currently I'm
                    // re-sorting after reading - which is necessary for external parents anyway.
                    // But allowing unsorted local parents is vaguely upsetting.
                    let mapped_parent = map.1.start + offset;

                    write_parent_diff(output_range.start - mapped_parent, false);
                } else {
                    // Foreign change
                    // println!("Region does not contain parent for {}", p);

                    let item = self.time_to_crdt_id(p);
                    let mapped_agent = agent_mapping.map(self, item.agent);
                    debug_assert!(mapped_agent >= 1);

                    // There are probably more compact ways to do this, but the txn data set is
                    // usually quite small anyway, even in large histories. And most parents objects
                    // will be in the set anyway. So I'm not too concerned about a few extra bytes
                    // here.
                    //
                    // I'm adding 1 to the mapped agent to make room for ROOT. This is quite dirty!
                    write_parent_diff(mapped_agent as usize, true);
                    push_usize(&mut txns_chunk, item.seq);
                }
            }
        });


        // If we just iterate in the current order, this code would be way simpler :p
        // let iter = self.history.optimized_txns_between(from_frontier, &self.frontier);
        // for walk in iter {
        for walk in self.history.optimized_txns_between(from_version, &self.version) {
            // We only care about walk.consume and parents.

            // We need to update *lots* of stuff in here!!

            // 1. Agent names and agent assignment
            for span in self.client_with_localtime.iter_range_packed_ctx(walk.consume, &()) {
                // Mark the agent as in-use (if we haven't already)
                let mapped_agent = agent_mapping.map(self, span.1.agent);

                // dbg!(&span);

                // agent_assignment is a list of (agent, len) pairs.
                // dbg!(span);
                agent_assignment_writer.push(AgentAssignmentRun {
                    agent: mapped_agent,
                    delta: agent_mapping.seq_delta(span.1.agent, span.1.seq_range),
                    len: span.len()
                });
            }

            // 2. Operations!
            // for ops in self.operations.iter_range_packed(walk.consume) {
            for (op, content) in self.iter_range_simple(walk.consume) {
                let op = op.1;

                // DANGER!! Its super important we pull out the content here rather than in
                // ops_writer somehow. The reason is that the content_pos field on the merged
                // OperationInternal objects will be invalid! Total foot gun there :p

                if op.tag == Ins && opts.store_inserted_content {
                    // For now at least, we can't skip inserted content for inserts.
                    // TODO: Reconsider this at some point.
                    assert!(content.is_some());
                }

                let content_chunk = switch(op.tag,
                    &mut inserted_content,
                    &mut deleted_content
                );
                if let Some(content_chunk) = content_chunk {
                    content_chunk.push(content, op.len());
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

        // This nominally needs to happen before we write out agent_mapping.
        // TODO: Support partial data sets. (from_frontier)
        let mut start_branch = Vec::new();
        write_frontier(&mut start_branch, from_version, &mut agent_mapping, self);
        if opts.store_start_branch_content {
            if local_version_is_root(from_version) {
                // Optimization. TODO: Check if this is worth it.
                write_chunk_str(&mut start_branch, "", ChunkType::Content);
            } else {
                let branch_here = Branch::new_at_local_version(self, from_version);
                // dbg!(&branch_here);
                write_content_rope(&mut start_branch, &branch_here.content);
            }
        }
        // Branch::write_content_str(&mut start_branch, ""); // TODO - support non-root!

        // TODO: The fileinfo chunk should specify encoding version and information
        // about the data types we're encoding.

        // *** FileInfo ***
        let mut fileinfo_buf = Vec::new();

        // DocId
        if let Some(name) = self.doc_id.as_ref() {
            write_chunk_str(&mut fileinfo_buf, name.as_str(), ChunkType::DocId);
        }

        // agent names
        push_chunk(&mut fileinfo_buf, ChunkType::AgentNames, &agent_mapping.consume());

        // User data
        if let Some(data) = opts.user_data {
            push_chunk(&mut fileinfo_buf, ChunkType::UserData, data);
        }

        write_chunk(ChunkType::FileInfo, &mut fileinfo_buf);

        // *** Start Branch - which was filled in above. ***
        write_chunk(ChunkType::StartBranch, &mut start_branch);

        // *** Patches ***
        // I'll just assemble it in buf. There's a lot of sloppy use of vec<u8>'s in here.
        let mut patches_buf = fileinfo_buf;

        if let Some(inserted_content) = inserted_content {
            // let max_compressed_size = lz4_flex::block::get_maximum_output_size(inserted_text.len());
            // let mut compressed = Vec::with_capacity(5 + max_compressed_size);
            // compressed.resize(compressed.capacity(), 0);
            // let mut pos = encode_usize(inserted_text.len(), &mut compressed);
            // pos += lz4_flex::compress_into(inserted_text.as_bytes(), &mut compressed[pos..]).unwrap();
            // write_chunk(Chunk::InsertedContent, &compressed[..pos]);
            if verbose {
                println!("Inserted text length {}", inserted_content.content.len());
            }
            // dbg!(ins_content_bytes);

            if let Some(bytes) = inserted_content.flush() {
                push_chunk(&mut patches_buf, ChunkType::PatchContent, &bytes);
            }
        }
        if let Some(deleted_content) = deleted_content {
            if verbose {
                println!("Deleted text length {}", deleted_content.content.len());
            }
            if let Some(bytes) = deleted_content.flush() {
                push_chunk(&mut patches_buf, ChunkType::PatchContent, &bytes);
            }
        }

        push_chunk(&mut patches_buf, ChunkType::OpVersions, &agent_assignment_chunk);
        push_chunk(&mut patches_buf, ChunkType::OpTypeAndPosition, &ops_chunk);
        push_chunk(&mut patches_buf, ChunkType::OpParents, &txns_chunk);

        write_chunk(ChunkType::Patches, &mut patches_buf);

        // TODO (later): Final branch content.

        // println!("checksum {checksum}");
        let checksum = checksum(&result);
        push_u32_le(&mut patches_buf, checksum);
        push_chunk(&mut result, ChunkType::Crc, &patches_buf);
        // write_chunk(Chunk::CRC, &mut buf);
        // push_u32(&mut result, checksum);

        if verbose {
            println!("== Total length {}", result.len());
        }

        result
    }

    pub fn encode(&self, opts: EncodeOptions) -> Vec<u8> {
        self.encode_from(opts, &[ROOT_TIME])
    }

    /// Encode the data stored in the OpLog into a (custom) compact binary form suitable for saving
    /// to disk, or sending over the network.
    pub fn encode_simple(&self, _opts: EncodeOptions) -> Vec<u8> {
        unimplemented!()
    }
    // pub fn encode_simple(&self, opts: EncodeOptions) -> Vec<u8> {
    //     let mut result = Vec::new();
    //     // The file starts with MAGIC_BYTES
    //     result.extend_from_slice(&MAGIC_BYTES);
    //
    //     // And contains a series of chunks. Each chunk has a chunk header (chunk type, length).
    //     // The first chunk is always the FileInfo chunk - which names the file format.
    //     let mut write_chunk = |c: Chunk, data: &[u8]| {
    //         if opts.verbose {
    //             println!("{:?} length {}", c, data.len());
    //         }
    //         push_chunk(&mut result, c, data);
    //     };
    //
    //     // TODO: The fileinfo chunk should specify DT version, encoding version and information
    //     // about the data types we're encoding.
    //     write_chunk(Chunk::FileInfo, &[]);
    //
    //     let mut buf = Vec::new();
    //
    //     // We'll name the starting frontier for the file. TODO: Support partial data sets.
    //     // TODO: Consider moving this into the FileInfo chunk.
    //     write_full_frontier(self, &mut buf, &[ROOT_TIME]);
    //     write_chunk(Chunk::StartFrontier, &buf);
    //     buf.clear();
    //
    //     // // TODO: This is redundant. Do I want to keep this or what?
    //     // write_full_frontier(self, &mut buf, &self.frontier);
    //     // write_chunk(Chunk::EndFrontier, &buf);
    //     // buf.clear();
    //
    //     // The AgentAssignment data indexes into the agents named here.
    //     for client_data in self.client_data.iter() {
    //         push_str(&mut buf, client_data.name.as_str());
    //     }
    //     write_chunk(Chunk::AgentNames, &buf);
    //     buf.clear();
    //
    //     // List of (agent, len) pairs for all changes.
    //     for KVPair(_, span) in self.client_with_localtime.iter() {
    //         push_run_u32(&mut buf, Run { val: span.agent, len: span.len() });
    //     }
    //     write_chunk(Chunk::AgentAssignment, &buf);
    //     buf.clear();
    //
    //     // *** Inserted (text) content and operations ***
    //
    //     // There's two ways I could iterate through the operations:
    //     //
    //     // 1. In local operation order. Each operation at that operation's local time. This is much
    //     //    simpler and faster - since we're essentially just copying oplog into the file.
    //     // 2. In optimized order. This would use txn_trace to reorder the operations in the
    //     //    operation log to maximize runs (and thus minimize file size). At some point I'd like
    //     //    to do this optimization - but I'm not sure where. (Maybe we should optimize in-place?)
    //
    //     // Note I'm going to push the text of all insert operations separately from the operation
    //     // data itself.
    //     //
    //     // Note for now this includes text that was later deleted. It is also in time-order not
    //     // document-order.
    //     //
    //     // Another way of storing this content would be to interleave it with the operations
    //     // themselves. That would work fine but:
    //     //
    //     // - The interleaved approach would be a bit more complex when dealing with other (non-text)
    //     //   data types.
    //     // - Interleaved would result in a slightly smaller file size (tens of bytes smaller)
    //     // - Interleaved would be easier to consume, because we wouldn't need to match up inserts
    //     //   with the text
    //     // - Interleaved it would compress much less well with snappy / lz4.
    //     let mut inserted_text = String::new();
    //     let mut deleted_text = String::new();
    //
    //     // The cursor position of the previous edit. We're differential, baby.
    //     let mut last_cursor_pos: usize = 0;
    //     for (KVPair(_, op), content) in self.iter_fast() {
    //     // for KVPair(_, op) in self.iter_metrics() {
    //         // For now I'm ignoring known content in delete operations.
    //         if op.tag == Ins && opts.store_inserted_content {
    //         //     assert!(op.content_known);
    //             inserted_text.push_str(content.unwrap());
    //         }
    //
    //         if op.tag == Del && opts.store_deleted_content {
    //             if let Some(s) = content {
    //                 deleted_text.push_str(s);
    //             }
    //         }
    //
    //         write_op(&mut buf, &op, &mut last_cursor_pos);
    //     }
    //     if opts.store_inserted_content {
    //         write_chunk(Chunk::InsertedContent, inserted_text.as_bytes());
    //         // write_chunk(Chunk::InsertedContent, &self.ins_content.as_bytes());
    //     }
    //     if opts.store_deleted_content {
    //         write_chunk(Chunk::DeletedContent, deleted_text.as_bytes());
    //         // write_chunk(Chunk::DeletedContent, &self.del_content.as_bytes());
    //     }
    //     write_chunk(Chunk::PositionalPatches, &buf);
    //
    //     // println!("{}", inserted_text);
    //
    //     // if opts.verbose {
    //         // dbg!(len_total, diff_zig_total, num_ops);
    //         // println!("op_data.len() {}", &op_data.len());
    //         // println!("inserted text length {}", inserted_text.len());
    //         // println!("deleted text length {}", deleted_text.len());
    //     // }
    //
    //     buf.clear();
    //
    //     for txn in self.history.entries.iter() {
    //         // First add this entry to the txn map.
    //         push_usize(&mut buf, txn.len());
    //
    //         // Then the parents.
    //         let mut iter = txn.parents.iter().peekable();
    //         while let Some(&p) = iter.next() {
    //             let p = p; // intellij bug
    //             let has_more = iter.peek().is_some();
    //
    //             let mut write_parent_diff = |mut n: usize, is_foreign: bool| {
    //                 n = mix_bit_usize(n, has_more);
    //                 n = mix_bit_usize(n, is_foreign);
    //                 push_usize(&mut buf, n);
    //             };
    //
    //             // Parents are either local or foreign. Local changes are changes we've written
    //             // (already) to the file. And foreign changes are changes that point outside the
    //             // local part of the DAG we're sending.
    //             //
    //             // Most parents will be local.
    //             if p == ROOT_TIME {
    //                 // ROOT is special cased, since its foreign but we don't put the root item in
    //                 // the agent list. (Though we could!)
    //                 // This is written as "agent 0", and with no seq value (since thats not needed).
    //                 write_parent_diff(0, true);
    //             } else {
    //                 // Local change!
    //                 write_parent_diff(txn.span.start - p, false);
    //             }
    //         }
    //         // write_history_entry(&mut buf, txn);
    //     }
    //     write_chunk(Chunk::TimeDAG, &buf);
    //     buf.clear();
    //
    //     if opts.verbose {
    //         println!("== Total length {}", result.len());
    //     }
    //
    //     result
    // }
}

#[cfg(test)]
mod tests {
    use crate::list::encoding::EncodeOptions;
    use crate::list::ListCRDT;

    #[test]
    #[ignore]
    fn encoding_smoke_test() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.insert(0, 0, "hi there");

        let d1 = doc.oplog.encode_simple(EncodeOptions::default());
        let d2 = doc.oplog.encode(EncodeOptions::default());
        assert_eq!(d1, d2);
        // let data = doc.ops.encode_old(EncodeOptions::default());
        // dbg!(data.len(), data);
    }

    #[test]
    fn encode_from_version() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph"); // 0
        doc.get_or_create_agent_id("mike"); // 1
        let _t1 = doc.insert(0, 0, "hi from seph!\n");
        let mut ops2 = doc.oplog.clone();

        let _t2 = doc.insert(1, 0, "hi from mike!\n");

        // let data = doc.ops.encode_from(EncodeOptions::default(), &[ROOT_TIME]);
        let data = doc.oplog.encode_from(EncodeOptions::default(), &[_t1]);
        ops2.decode_and_add(&data).unwrap();
        assert_eq!(ops2, doc.oplog);
        // let data = doc.ops.encode_from(EncodeOptions::default(), &[_t2]);
        // dbg!(data);
        // let data = doc.ops.encode_old(EncodeOptions::default());
        // dbg!(data.len(), data);
    }
}
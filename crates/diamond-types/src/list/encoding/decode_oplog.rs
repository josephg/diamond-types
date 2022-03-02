use smallvec::SmallVec;
use crate::list::encoding::*;
use crate::list::encoding::varint::*;
use crate::list::{Frontier, OpLog, switch, Time};
use crate::list::frontier::{frontier_eq, frontier_is_sorted};
use crate::list::internal_op::OperationInternal;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::rev_span::TimeSpanRev;
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use crate::unicount::{consume_chars, count_chars, split_at_char};
use crate::list::encoding::ParseError::*;
use rle::AppendRle;
use rle::take_max_iter::TakeMaxFns;
use crate::list::encoding::ChunkType::{Content, ContentKnown, FileInfo, Patches, StartBranch};
use crate::list::history::MinimalHistoryEntry;
use crate::list::operation::InsDelTag;
use crate::localtime::{TimeSpan, UNDERWATER_START};
use crate::remotespan::{CRDTId, CRDTSpan};
use crate::rle::{KVPair, RleKeyedAndSplitable, RleSpanHelpers, RleVec};

#[derive(Debug, Clone)]
struct BufReader<'a>(&'a [u8]);

// If this is set to false, the compiler can optimize out the verbose printing code. This makes the
// compiled output slightly smaller.
const ALLOW_VERBOSE: bool = false;
// const ALLOW_VERBOSE: bool = true;

impl<'a> BufReader<'a> {
    // fn check_has_bytes(&self, num: usize) {
    //     assert!(self.0.len() >= num);
    // }

    #[inline]
    fn check_not_empty(&self) -> Result<(), ParseError> {
        self.check_has_bytes(1)
    }

    #[inline]
    fn check_has_bytes(&self, num: usize) -> Result<(), ParseError> {
        if self.0.len() < num { Err(UnexpectedEOF) } else { Ok(()) }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn expect_empty(&self) -> Result<(), ParseError> {
        if self.is_empty() { Ok(()) } else { Err(InvalidLength) }
    }

    #[allow(unused)]
    fn len(&self) -> usize {
        self.0.len()
    }

    fn consume(&mut self, num: usize) {
        self.0 = unsafe { self.0.get_unchecked(num..) };
    }

    fn read_magic(&mut self) -> Result<(), ParseError> {
        self.check_has_bytes(8)?;
        if self.0[..MAGIC_BYTES.len()] != MAGIC_BYTES {
            return Err(InvalidMagic);
        }
        self.consume(8);
        Ok(())
    }

    fn peek_u32(&self) -> Result<Option<u32>, ParseError> {
        if self.is_empty() { return Ok(None); }
        // Some(decode_u32(self.0))
        Ok(Some(decode_u32(self.0)?.0))
    }

    fn next_u32(&mut self) -> Result<u32, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u32(self.0)?;
        self.consume(count);
        Ok(val)
    }

    fn next_u32_le(&mut self) -> Result<u32, ParseError> {
        // self.check_has_bytes(size_of::<u32>())?;
        let val = u32::from_le_bytes(self.0[0..4].try_into().map_err(|_| UnexpectedEOF)?);
        self.consume(size_of::<u32>());
        Ok(val)
    }

    #[allow(unused)]
    fn next_u64(&mut self) -> Result<u64, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u64(self.0)?;
        self.consume(count);
        Ok(val)
    }

    fn next_usize(&mut self) -> Result<usize, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_usize(self.0)?;
        self.consume(count);
        Ok(val)
    }

    fn next_zigzag_isize(&mut self) -> Result<isize, ParseError> {
        let n = self.next_usize()?;
        Ok(num_decode_zigzag_isize(n))
    }

    fn next_n_bytes(&mut self, num_bytes: usize) -> Result<&'a [u8], ParseError> {
        if num_bytes > self.0.len() { return Err(UnexpectedEOF); }

        let (data, remainder) = self.0.split_at(num_bytes);
        self.0 = remainder;
        Ok(data)
    }

    // fn peek_u32(&self) -> Result<u32, ParseError> {
    //     self.check_not_empty()?;
    //     Ok(decode_u32(self.0).0)
    // }
    //
    // fn peek_chunk_type(&self) -> Result<Chunk, ParseError> {
    //     Ok(Chunk::try_from(self.peek_u32()?).map_err(|_| InvalidChunkHeader)?)
    // }

    // TODO: Remove this?
    #[allow(unused)]
    fn peek_chunk(&self) -> Result<Option<ChunkType>, ParseError> {
        // TODO: There's probably a way to write this more cleanly?? Clippy halp
        if let Some(num) = self.peek_u32()? {
            let chunk_type = ChunkType::try_from(num)
                .map_err(|_| UnknownChunk)?;
            Ok(Some(chunk_type))
        } else {
            Ok(None)
        }
    }

    fn next_chunk(&mut self) -> Result<(ChunkType, BufReader<'a>), ParseError> {
        let chunk_type = ChunkType::try_from(self.next_u32()?)
            .map_err(|_| UnknownChunk);

        // This in no way guarantees we're good.
        let len = self.next_usize()?;
        if len > self.0.len() {
            return Err(InvalidLength);
        }

        let reader = BufReader(self.next_n_bytes(len)?);

        // Note we're try-ing chunk_type here so we still read all the bytes if we can, even if
        // the chunk type is unknown.
        Ok((chunk_type?, reader))
    }

    /// Read a chunk with the named type. Returns None if the next chunk isn't the specified type,
    /// or we hit EOF.
    fn read_chunk(&mut self, expect_chunk_type: ChunkType) -> Result<Option<BufReader<'a>>, ParseError> {
        if let Some(actual_chunk_type) = self.peek_u32()? {
            if actual_chunk_type != (expect_chunk_type as _) {
                // Chunk doesn't match requested type.
                return Ok(None);
            }
            self.next_chunk().map(|(_type, c)| Some(c))
        } else {
            // EOF.
            Ok(None)
        }
    }

    fn expect_chunk(&mut self, expect_chunk_type: ChunkType) -> Result<BufReader<'a>, ParseError> {
        // Scan chunks until we find expect_chunk_type. Error if the chunk is missing from the file.
        while !self.is_empty() {
            let chunk = self.next_chunk();

            // Ignore unknown chunks for forwards compatibility.
            if let Err(UnknownChunk) = chunk { continue; }

            // Otherwise we'll just try unwrap as usual.
            let (actual_chunk_type, r) = chunk?;
            if expect_chunk_type == actual_chunk_type {
                // dbg!(expect_chunk_type, actual_chunk_type);
                return Ok(r);
            }
        }
        Err(MissingChunk(expect_chunk_type as _))
    }

    // Note the result is attached to the lifetime 'a, not the lifetime of self.
    fn next_str(&mut self) -> Result<&'a str, ParseError> {
        if self.0.is_empty() { return Err(UnexpectedEOF); }

        let len = self.next_usize()?;
        if len > self.0.len() { return Err(InvalidLength); }

        let bytes = self.next_n_bytes(len)?;
        std::str::from_utf8(bytes).map_err(InvalidUTF8)
    }

    /// Read the next string thats encoded in this content chunk
    fn read_content_str(&mut self) -> Result<&'a str, ParseError> {
        // dbg!(&self.0);
        let data_type = self.next_u32()?;
        if data_type != (DataType::PlainText as u32) {
            return Err(UnknownChunk);
        }
        // let len = self.next_usize()?;
        // if len > self.0.len() {
        //     return Err(InvalidLength);
        // }
        std::str::from_utf8(&self.0).map_err(InvalidUTF8)
    }

    fn read_next_agent_assignment(&mut self, map: &mut [(AgentId, usize)]) -> Result<Option<CRDTSpan>, ParseError> {
        // Agent assignments are almost always (but not always) linear. They can have gaps, and
        // they can be reordered if the same agent ID is used to contribute to multiple branches.
        //
        // I'm still not sure if this is a good idea.

        if self.0.is_empty() { return Ok(None); }

        let mut n = self.next_usize()?;
        let has_jump = strip_bit_usize2(&mut n);
        let len = self.next_usize()?;

        let jump = if has_jump {
            self.next_zigzag_isize()?
        } else { 0 };

        // The agent mapping uses 0 to refer to ROOT, but no actual operations can be assigned to
        // the root agent.
        if n == 0 {
            return Err(ParseError::InvalidLength);
        }

        let inner_agent = n - 1;
        if inner_agent >= map.len() {
            return Err(ParseError::InvalidLength);
        }

        let entry = &mut map[inner_agent];
        let agent = entry.0;

        // TODO: Error if this overflows.
        let start = (entry.1 as isize + jump) as usize;
        let end = start + len;
        entry.1 = end;

        Ok(Some(CRDTSpan {
            agent,
            seq_range: (start..end).into(),
        }))
    }

    fn read_frontier(&mut self, oplog: &OpLog, agent_map: &[(AgentId, usize)]) -> Result<Frontier, ParseError> {
        let mut result = Frontier::new();
        // All frontiers contain at least one item.
        loop {
            // let agent = self.next_str()?;
            let (mapped_agent, has_more) = strip_bit_usize(self.next_usize()?);
            let seq = self.next_usize()?;

            let id = if mapped_agent == 0 {
                CRDTId { agent: ROOT_AGENT, seq: 0 }
            } else {
                let agent = agent_map[mapped_agent - 1].0;
                CRDTId { agent, seq }
            };

            let time = oplog.crdt_id_to_time(id);
            result.push(time);

            if !has_more { break; }
        }

        if !frontier_is_sorted(result.as_slice()) {
            // TODO: Check how this effects wasm bundle size.
            result.sort_unstable();
        }

        Ok(result)
    }
    // fn read_full_frontier(&mut self, oplog: &OpLog) -> Result<Frontier, ParseError> {
    //     let mut result = Frontier::new();
    //     // All frontiers contain at least one item.
    //     loop {
    //         let agent = self.next_str()?;
    //         let n = self.next_usize()?;
    //         let (seq, has_more) = strip_bit_usize(n);
    //
    //         let time = oplog.try_remote_id_to_time(&RemoteId {
    //             agent: agent.into(),
    //             seq
    //         }).map_err(InvalidRemoteID)?;
    //
    //         result.push(time);
    //
    //         if !has_more { break; }
    //     }
    //
    //     if !frontier_is_sorted(result.as_slice()) {
    //         // TODO: Check how this effects wasm bundle size.
    //         result.sort_unstable();
    //     }
    //
    //     Ok(result)
    // }

    fn read_parents(&mut self, oplog: &OpLog, next_time: Time, agent_map: &[(AgentId, usize)]) -> Result<SmallVec<[usize; 2]>, ParseError> {
        let mut parents = SmallVec::<[usize; 2]>::new();
        loop {
            let mut n = self.next_usize()?;
            let is_foreign = strip_bit_usize2(&mut n);
            let has_more = strip_bit_usize2(&mut n);

            let parent = if is_foreign {
                if n == 0 {
                    ROOT_TIME
                } else {
                    let agent = agent_map[n - 1].0;
                    let seq = self.next_usize()?;
                    if let Some(c) = oplog.client_data.get(agent as usize) {
                        // Adding UNDERWATER_START for foreign parents in a horrible hack.
                        // I'm so sorry. This gets pulled back out in history_entry_map_and_truncate
                        c.try_seq_to_time(seq).ok_or(InvalidLength)?
                    } else {
                        return Err(InvalidLength);
                    }
                }
            } else {
                // Local parents (parents inside this chunk of data) are stored using their
                // local time offset.
                next_time - n
            };

            parents.push(parent);
            if !has_more { break; }
        }
        Ok(parents)
    }

    fn next_history_entry(&mut self, oplog: &OpLog, next_time: Time, agent_map: &[(AgentId, usize)]) -> Result<MinimalHistoryEntry, ParseError> {
        let len = self.next_usize()?;
        let parents = self.read_parents(oplog, next_time, agent_map)?;

        // Bleh its gross passing a &[Time] into here when we have a Frontier already.
        Ok(MinimalHistoryEntry {
            span: (next_time..next_time + len).into(),
            parents,
        })
    }

    fn read_fileinfo(&mut self, oplog: &mut OpLog) -> Result<(Option<BufReader>, Vec<(AgentId, usize)>), ParseError> {
        let mut fileinfo = self.expect_chunk(ChunkType::FileInfo)?;
        // fileinfo has UserData and AgentNames.

        let userdata = fileinfo.read_chunk(ChunkType::UserData)?;
        let mut agent_names_chunk = fileinfo.expect_chunk(ChunkType::AgentNames)?;

        // Map from agent IDs in the file (idx) to agent IDs in self, and the seq cursors.
        //
        // This will usually just be 0,1,2,3,4...
        //
        // 0 implicitly maps to ROOT.
        // let mut file_to_self_agent_map = vec![(ROOT_AGENT, 0)];
        let mut agent_map = Vec::new();
        while !agent_names_chunk.0.is_empty() {
            let name = agent_names_chunk.next_str()?;
            let id = oplog.get_or_create_agent_id(name);
            agent_map.push((id, 0));
        }

        Ok((userdata, agent_map))
    }

    fn dbg_print_chunk_tree_internal(mut self) -> Result<(), ParseError> {
        println!("Total file size {}", self.len());
        let total_len = self.len();
        println!("magic at {}", total_len - self.len());
        self.read_magic()?;
        let protocol_version = self.next_usize()?;
        println!("Protocol version {protocol_version}");

        loop { // gross
            let position = total_len - self.len();
            if let Ok((chunk, mut inner_reader)) = self.next_chunk() {
                println!("Chunk {:?} at {} ({} bytes)", chunk, position, inner_reader.len());

                let inner_len = inner_reader.len();
                if chunk == FileInfo || chunk == StartBranch || chunk == Patches {
                    loop {
                        let inner_position = position + inner_len - inner_reader.len();
                        if let Ok((chunk, inner_inner_reader)) = inner_reader.next_chunk() {
                            println!("  Chunk {:?} at {} ({} bytes)", chunk, inner_position, inner_inner_reader.len());
                        } else { break; }
                    }
                }
            } else { break; }
        }
        Ok(())
    }

    fn dbg_print_chunk_tree(self) {
        if let Err(e) = self.dbg_print_chunk_tree_internal() {
            eprintln!("-> Error parsing ({:?})", e);
        }
    }
}


/// Returns (mapped span, remainder).
/// The returned remainder is *NOT MAPPED*. This allows this method to be called in a loop.
fn history_entry_map_and_truncate(mut hist_entry: MinimalHistoryEntry, version_map: &RleVec<KVPair<TimeSpan>>) -> (MinimalHistoryEntry, Option<MinimalHistoryEntry>) {
    let (map_entry, offset) = version_map.find_packed_with_offset(hist_entry.span.start);

    let mut map_entry = map_entry.1;
    map_entry.truncate_keeping_right(offset);

    let remainder = if hist_entry.len() > map_entry.len() {
        Some(hist_entry.truncate(map_entry.len()))
    } else {
        None
    };

    // Keep entire history entry. Just map it.
    let len = hist_entry.len(); // hist_entry <= map_entry here.
    hist_entry.span.start = map_entry.start;
    hist_entry.span.end = hist_entry.span.start + len;

    // dbg!(&hist_entry.parents);

    // Map parents. Parents are underwater when they're local to the file, and need mapping.
    // const UNDERWATER_LAST: usize = ROOT_TIME - 1;
    for p in hist_entry.parents.iter_mut() {
        if *p >= UNDERWATER_START && *p != ROOT_TIME {
            let (span, offset) = version_map.find_packed_with_offset(*p);
            *p = span.1.start + offset;
        }
    }

    (hist_entry, remainder)
}

// This is a simple wrapper to give us an iterator for agent assignments. The
#[derive(Debug)]
struct AgentAssignments<'a>(BufReader<'a>, &'a mut [(AgentId, usize)]);

impl<'a> Iterator for AgentAssignments<'a> {
    type Item = Result<CRDTSpan, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Convert Result<Option<...>> to Option<Result<...>>. There's probably a better way to do
        // this.
        match self.0.read_next_agent_assignment(self.1) {
            Ok(Some(val)) => Some(Ok(val)),
            Ok(None) => None,
            Err(err) => Some(Err(err))
        }
    }
}

// I could just pass &mut last_cursor_pos to a flat read() function. Eh. Once again, generators
// would make this way cleaner.
#[derive(Debug)]
struct ReadPatchesIter<'a> {
    buf: BufReader<'a>,
    last_cursor_pos: usize,
}

impl<'a> ReadPatchesIter<'a> {
    fn new(buf: BufReader<'a>) -> Self {
        Self {
            buf,
            last_cursor_pos: 0,
        }
    }

    // The actual next function. The only reason I did it like this is so I can take advantage of
    // the ergonomics of try?.
    fn next_internal(&mut self) -> Result<OperationInternal, ParseError> {
        let mut n = self.buf.next_usize()?;
        // This is in the opposite order from write_op.
        let has_length = strip_bit_usize2(&mut n);
        let diff_not_zero = strip_bit_usize2(&mut n);
        let tag = if strip_bit_usize2(&mut n) { Del } else { Ins };

        let (len, diff, fwd) = if has_length {
            // n encodes len.
            let fwd = if tag == Del {
                strip_bit_usize2(&mut n)
            } else { true };

            let diff = if diff_not_zero {
                self.buf.next_zigzag_isize()?
            } else { 0 };

            (n, diff, fwd)
        } else {
            // n encodes diff.
            let diff = num_decode_zigzag_isize(n);
            (1, diff, true)
        };

        // dbg!(self.last_cursor_pos, diff);
        let raw_start = isize::wrapping_add(self.last_cursor_pos as isize, diff) as usize;

        let (start, raw_end) = match (tag, fwd) {
            (Ins, true) => (raw_start, raw_start + len),
            (Ins, false) => (raw_start, raw_start),
            (Del, true) => (raw_start, raw_start),
            (Del, false) => (raw_start - len, raw_start - len),
        };
        // dbg!((raw_start, tag, fwd, len, start, raw_end));

        let end = start + len;

        // dbg!(pos);
        self.last_cursor_pos = raw_end;
        // dbg!(self.last_cursor_pos);

        Ok(OperationInternal {
            span: TimeSpanRev { // TODO: Probably a nicer way to construct this.
                span: (start..end).into(),
                fwd,
            },
            tag,
            content_pos: None,
        })
    }
}

impl<'a> Iterator for ReadPatchesIter<'a> {
    type Item = Result<OperationInternal, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { None } else { Some(self.next_internal()) }
    }
}

#[derive(Debug)]
struct ReadPatchContentIter<'a> {
    run_chunk: BufReader<'a>,
    content: &'a str,
}

#[derive(Debug, Clone)]
struct ContentItem<'a> {
    len: usize,
    content: Option<&'a str>,
}

impl<'a> SplitableSpan for ContentItem<'a> {
    fn truncate(&mut self, at: usize) -> Self {
        let content_remainder = if let Some(str) = self.content.as_mut() {
            let (here, remainder) = split_at_char(*str, at);
            *str = here;
            Some(remainder)
        } else { None };

        let remainder = ContentItem {
            len: self.len - at,
            content: content_remainder,
        };
        self.len = at;
        remainder
    }
}

impl<'a> HasLength for ContentItem<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> ReadPatchContentIter<'a> {
    fn new(mut chunk: BufReader<'a>) -> Result<(InsDelTag, Self), ParseError> {
        let tag = match chunk.next_u32()? {
            0 => Ins,
            1 => Del,
            _ => { return Err(InvalidContent); }
        };

        let mut content_chunk = chunk.expect_chunk(Content)?;
        let content = content_chunk.read_content_str()?;

        let run_chunk = chunk.expect_chunk(ContentKnown)?;

        Ok((tag, Self { run_chunk, content }))
    }

    fn next_internal(&mut self) -> Result<ContentItem<'a>, ParseError> {
        let n = self.run_chunk.next_usize()?;
        let (len, known) = strip_bit_usize(n);
        let content = if known {
            let content = consume_chars(&mut self.content, len);
            if count_chars(content) != len { // Having a duplicate strlen here is gross.
                // We couldn't pull as many chars as requested from self.content.
                return Err(UnexpectedEOF);
            }
            Some(content)
        } else { None };

        Ok(ContentItem { len, content })
    }
}

impl<'a> Iterator for ReadPatchContentIter<'a> {
    type Item = Result<ContentItem<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.run_chunk.is_empty(), self.content.is_empty()) {
            (false, _) => Some(self.next_internal()),
            (true, true) => None,
            (true, false) => Some(Err(ParseError::UnexpectedEOF)),
        }
    }
}


#[derive(Debug, Clone)]
pub struct DecodeOptions {
    /// Ignore CRC check failures. This is mostly used for debugging.
    ignore_crc: bool,

    verbose: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            ignore_crc: false,
            verbose: false,
        }
    }
}

impl OpLog {
    pub fn load_from(data: &[u8]) -> Result<Self, ParseError> {
        let mut oplog = Self::new();
        oplog.merge_data_internal(data, DecodeOptions::default())?;
        Ok(oplog)
    }

    pub fn load_from_opts(data: &[u8], opts: DecodeOptions) -> Result<Self, ParseError> {
        let mut oplog = Self::new();
        oplog.merge_data_internal(data, opts)?;
        Ok(oplog)
    }

    pub fn merge_data(&mut self, data: &[u8]) -> Result<(), ParseError> {
        self.merge_data_opts(data, DecodeOptions::default())
    }

    pub fn merge_data_opts(&mut self, data: &[u8], opts: DecodeOptions) -> Result<(), ParseError> {
        // In order to merge data safely, when an error happens we need to unwind all the merged
        // operations before returning. Otherwise self is in an invalid state.
        //
        // The merge_data method is append-only, so really we just need to trim back all the data
        // that has been (partially) added.

        // Number of operations before merging happens
        let len = self.len();

        // We could regenerate the frontier, but this is much lazier.
        let old_frontier = self.frontier.clone();
        let num_known_agents = self.client_data.len();
        let ins_content_length = self.ins_content.len();
        let del_content_length = self.del_content.len();

        let result = self.merge_data_internal(data, opts);

        if result.is_err() {
            // Unwind changes back to len.
            // This would be nicer with an RleVec iterator, but the iter implementation doesn't
            // support iterating backwards.
            while let Some(last) = self.client_with_localtime.0.last_mut() {
                debug_assert!(len <= last.end());
                if len == last.end() { break; }
                else {
                    // Truncate!
                    let KVPair(_, removed) = if len <= last.0 {
                        // Drop entire entry
                        self.client_with_localtime.0.pop().unwrap()
                    } else {
                        last.truncate(len - last.0)
                    };

                    let client_data = &mut self.client_data[removed.agent as usize];
                    client_data.item_times.remove(removed.seq_range);
                }
            }

            let num_operations = self.operations.end();
            if num_operations > len {
                self.operations.remove((len..num_operations).into());
            }

            // Trim history
            let hist_entries = &mut self.history.entries;
            let history_length = hist_entries.end();
            if history_length > len {
                // We can't use entries.remove because HistoryEntry doesn't support SplitableSpan.
                // And also because we need to update child_indexes.
                let del_span_start = len;

                let first_idx = hist_entries.find_index(len).unwrap();

                let e = &mut hist_entries.0[first_idx];
                let first_truncated_idx = if del_span_start > e.span.start {
                    // The first entry just needs to be trimmed down.
                    e.span.truncate_from(del_span_start);
                    first_idx + 1
                } else {
                    first_idx
                };

                let mut idx = first_truncated_idx;

                // Go through and unwind from idx.
                while idx < hist_entries.num_entries() {
                    // Cloning here is an ugly and kinda slow hack to work around the borrow
                    // checker. But this whole case is rare anyway, so idk.
                    let parents = hist_entries.0[idx].parents.clone();

                    for p in parents {
                        if p < len {
                            let parent_entry = hist_entries.find_mut(p).unwrap().0;
                            while let Some(&c_idx) = parent_entry.child_indexes.last() {
                                if c_idx >= first_truncated_idx {
                                    parent_entry.child_indexes.pop();
                                } else { break; }
                            }
                        }
                    }

                    idx += 1;
                }

                self.history.entries.0.truncate(idx);

                while let Some(&last_idx) = self.history.root_child_indexes.last() {
                    if last_idx >= self.history.entries.num_entries() {
                        self.history.root_child_indexes.pop();
                    } else { break; }
                }
            }

            // Remove excess agents
            self.client_data.truncate(num_known_agents);

            self.ins_content.truncate(ins_content_length);
            self.del_content.truncate(del_content_length);

            self.frontier = old_frontier;
        }

        result
    }

    /// Merge data from the remote source into our local document state.
    ///
    /// NOTE: This code is quite new.
    /// TODO: Currently if this method returns an error, the local state is undefined & invalid.
    /// Until this is fixed, the signature of the method will stay kinda weird to prevent misuse.
    fn merge_data_internal(&mut self, data: &[u8], opts: DecodeOptions) -> Result<(), ParseError> {
        // Written to be symmetric with encode functions.
        let mut reader = BufReader(data);

        let verbose = ALLOW_VERBOSE && opts.verbose;
        if verbose {
            reader.clone().dbg_print_chunk_tree();
        }

        reader.read_magic()?;
        let protocol_version = reader.next_usize()?;
        if protocol_version != PROTOCOL_VERSION {
            return Err(UnsupportedProtocolVersion);
        }

        // *** FileInfo ***
        // fileinfo has UserData and AgentNames.
        // The agent_map is a map from agent_id in the file to agent_id in self.
        // self is &mut because this method adds missing agents to self.
        let (_userdata, mut agent_map) = reader.read_fileinfo(self)?;

        // *** StartBranch ***
        let start_frontier = if let Some(mut start_branch) = reader.read_chunk(ChunkType::StartBranch)? {
            let mut start_frontier_chunk = start_branch.expect_chunk(ChunkType::Frontier)?;
            let frontier = start_frontier_chunk.read_frontier(self, &agent_map).map_err(|e| {
                // We can't read a frontier if it names agents or sequence numbers we haven't seen
                // before. If this happens, its because we're trying to load a data set from the future.
                if let InvalidRemoteID(_) = e {
                    DataMissing
                } else { e }
            })?;

            Some(frontier)
        } else { None };

        // Usually the version data will be strictly separated. Either we're loading data into an
        // empty document, or we've been sent catchup data from a remote peer. If the data set
        // overlaps, we need to actively filter out operations & txns from that data set.
        let patches_overlap = start_frontier.map_or(true, |f|
            !frontier_eq(&f, &self.frontier),
        );
        // dbg!(patches_overlap);

        // *** Patches ***
        {
            // This chunk contains the actual set of edits to the document.
            let mut patch_chunk = reader.expect_chunk(ChunkType::Patches)?;

            let mut ins_content = None;
            let mut del_content = None;

            while let Some(chunk) = patch_chunk.read_chunk(ChunkType::PatchContent)? {
                let (tag, content_chunk) = ReadPatchContentIter::new(chunk)?;
                let iter = content_chunk.take_max();
                match tag {
                    Ins => { ins_content = Some(iter); }
                    Del => { del_content = Some(iter); }
                }
            }

            // if let Some(chunk) = patch_chunk.read_chunk(ChunkType::InsertedContent)? {
            //     // let decompressed_len = content_chunk.next_usize()?;
            //     // let decompressed_data = lz4_flex::decompress(content_chunk.0, decompressed_len).unwrap();
            //     // let content = String::from_utf8(decompressed_data).unwrap();
            //     //     // .map_err(InvalidUTF8)?;
            //
            //     let content = std::str::from_utf8(chunk.0).map_err(InvalidUTF8)?;
            //     let len = count_chars(content);
            //     ins_content = Some((content, len));
            // }
            //
            // // TODO: DRY.
            // if let Some(chunk) = patch_chunk.read_chunk(ChunkType::DeletedContent)? {
            //     let content = std::str::from_utf8(chunk.0).map_err(InvalidUTF8)?;
            //     let len = count_chars(content);
            //
            //     del_content = Some((content, len));
            // }

            // So note that the file we're loading from may contain changes we already have locally.
            // We (may) need to filter out operations from the patch stream, which we read from
            // below. To do that without extra need to read both the agent assignments and patches together.
            let mut agent_assignment_chunk = patch_chunk.expect_chunk(ChunkType::Version)?;
            let pos_patches_chunk = patch_chunk.expect_chunk(ChunkType::OpTypeAndPosition)?;
            let mut history_chunk = patch_chunk.expect_chunk(ChunkType::Parents)?;

            let mut patches_iter = ReadPatchesIter::new(pos_patches_chunk)
                .take_max();

            let first_new_time = self.len();
            let mut next_patch_time = first_new_time;

            // The file we're loading has a list of operations. The list's item order is shared in a
            // handful of lists of data - agent assignment, operations, content and txns.

            // Only used for new (not overlapped) operations.
            let mut next_assignment_time = first_new_time;
            let new_op_start = if patches_overlap { UNDERWATER_START } else { first_new_time };
            let mut next_file_time = new_op_start;

            // Mapping from "file order" (numbered from 0) to the resulting local order. Using a
            // smallvec here because it'll almost always just be a single entry, and that prevents
            // an allocation in the common case. This is needed for merging overlapped file data.
            // let mut version_map: SmallVec<[KVPair<TimeSpan>; 1]> = SmallVec::new();

            // TODO: Replace with SmallVec to avoid an allocation in the common case here.
            let mut version_map = RleVec::new();

            // Take and merge the next exactly n patches
            let mut parse_next_patches = |oplog: &mut OpLog, mut n: usize, keep: bool| -> Result<(), ParseError> {
                while n > 0 {
                    let mut max_len = n;

                    // This is way longer than it should be. Gross!
                    if let Some(Ok(op)) = patches_iter.peek() {
                        // let op = op;
                        // dbg!(op.tag);
                        max_len = max_len.min(op.len());

                        if let Some(content) = switch(op.tag, &mut ins_content, &mut del_content) {
                            if let Some(Ok(item)) = content.peek() {
                                max_len = max_len.min(item.len());
                            }
                        }
                    } // If this returns anything else we're erroring, but we'll handle that below.

                    if let Some(op) = patches_iter.next(max_len) {
                        let op = op?;
                        // dbg!((n, &op));
                        let len = op.len();
                        assert!(op.len() > 0);
                        n -= len;

                        let content_here = if let Some(content_iter) = switch(op.tag, &mut ins_content, &mut del_content) {
                            if let Some(content) = content_iter.next(max_len) {
                                let content = content?;
                                assert_eq!(content.len(), max_len);
                                content.content
                            } else {
                                return Err(InvalidLength);
                            }
                        } else { None };

                        // dbg!(content, len, content_here);

                        // self.operations.push(KVPair(next_time, op));
                        if keep {
                            oplog.push_op_internal(next_patch_time, op.span, op.tag, content_here);
                            next_patch_time += len;
                        }
                    } else {
                        return Err(InvalidLength);
                    }
                }

                Ok(())
            };

            while let Some(mut crdt_span) = agent_assignment_chunk.read_next_agent_assignment(&mut agent_map)? {
                // let mut crdt_span = crdt_span; // TODO: Remove me. Blerp clion.
                // dbg!(crdt_span);
                if crdt_span.agent as usize >= self.client_data.len() {
                    return Err(ParseError::InvalidLength);
                }

                if patches_overlap {
                    // Sooo, if the current document overlaps with the data we're loading, we need
                    // to filter out all the operations we already have from the stream.
                    while !crdt_span.seq_range.is_empty() {
                        // dbg!(&crdt_span);
                        let client = &self.client_data[crdt_span.agent as usize];
                        let (span, _offset) = client.item_times.find_sparse(crdt_span.seq_range.start);
                        let (span_end, overlap) = match span {
                            // Skip the entry.
                            Ok(entry) => (entry.end(), Some(entry.1)),
                            // Consume the entry
                            Err(empty_span) => (empty_span.end, None),
                        };

                        let end = crdt_span.seq_range.end.min(span_end);
                        let consume_here = crdt_span.seq_range.truncate_keeping_right_from(end);
                        let len = consume_here.len();

                        let keep = if let Some(overlap) = overlap {
                            // There's overlap. We'll filter out this item.
                            version_map.push_rle(KVPair(next_file_time, overlap));
                            // println!("push overlap {:?}", KVPair(next_file_time, overlap));
                            false
                        } else {
                            self.assign_time_to_crdt_span(next_assignment_time, CRDTSpan {
                                agent: crdt_span.agent,
                                seq_range: consume_here,
                            });
                            // println!("push end {:?}", KVPair(
                            //     next_file_time,
                            //     TimeSpan::from(next_assignment_time..next_assignment_time + len),
                            // ));

                            version_map.push_rle(KVPair(
                                next_file_time,
                                (next_assignment_time..next_assignment_time + len).into(),
                            ));
                            next_assignment_time += len;
                            true
                        };
                        next_file_time += len;

                        // dbg!(&file_to_local_version_map);

                        parse_next_patches(self, len, keep)?;

                        // And deal with history.
                        // parse_next_history(&mut self, &file_to_self_agent_map, &version_map, len, keep)?;
                    }
                    // dbg!(span);
                } else {
                    // Optimization - don't bother with the filtering code above if loaded changes
                    // follow local changes. Most calls to this function load into an empty
                    // document, and this is the case.
                    self.assign_time_to_crdt_span(next_assignment_time, crdt_span);
                    let len = crdt_span.len();
                    // file_to_local_version_map.push_rle((next_assignment_time..next_assignment_time + len).into());
                    version_map.push_rle(KVPair(
                        next_file_time,
                        (next_assignment_time..next_assignment_time + len).into(),
                    ));
                    parse_next_patches(self, len, true)?;
                    // parse_next_history(&mut self, &file_to_self_agent_map, &version_map, len, true)?;

                    next_assignment_time += len;
                    next_file_time += len;
                }
            }

            next_file_time = new_op_start;
            // dbg!(&version_map);
            let mut next_history_time = first_new_time;

            while !history_chunk.is_empty() {
                let mut entry = history_chunk.next_history_entry(self, next_file_time, &agent_map)?;
                // So at this point the entry has underwater entry spans, and parents are underwater
                // when they're local to the file (and non-underwater when they refer to our items).
                // This makes the entry safe to truncate(), but we need to map it before we can use
                // it.

                next_file_time += entry.len();
                // dbg!(&entry);

                // If patches don't overlap, this code can be simplified to this:
                //     self.insert_history(&entry.parents, entry.span);
                //     self.advance_frontier(&entry.parents, entry.span);
                //     next_history_time += entry.len();
                // But benchmarks show it doesn't make any real difference in practice, so I'm not
                // going to sweat it.

                loop {
                    let (mut mapped, remainder) = history_entry_map_and_truncate(entry, &version_map);
                    // dbg!(&mapped);
                    assert!(mapped.span.start <= next_history_time);

                    if mapped.span.end > next_history_time {
                        // We'll merge items from mapped.

                        // This is needed because the overlapping & new items aren't strictly
                        // separated in version_map. Its kinda ugly though - I'd like a better way
                        // to deal with this case.
                        if mapped.span.start < next_history_time {
                            mapped.truncate_keeping_right(next_history_time - mapped.span.start);
                        }

                        self.insert_history(&mapped.parents, mapped.span);
                        self.advance_frontier(&mapped.parents, mapped.span);
                        next_history_time += mapped.len();
                    } // else we already have these entries. Filter them out.

                    if let Some(remainder) = remainder {
                        entry = remainder;
                    } else {
                        break;
                    }
                }
            }

            // We'll count the lengths in each section to make sure they all match up with each other.
            if next_patch_time != next_assignment_time { return Err(InvalidLength); }
            if next_patch_time != next_history_time { return Err(InvalidLength); }

            // dbg!(&patch_chunk);
            patch_chunk.expect_empty()?;
            history_chunk.expect_empty()?;

            if let Some(mut iter) = ins_content {
                if iter.peek().is_some() {
                    return Err(InvalidContent);
                }
            }

            if let Some(mut iter) = del_content {
                if iter.peek().is_some() {
                    return Err(InvalidContent);
                }
            }

            // dbg!(&version_map);
        } // End of patches

        // TODO: Move checksum check to the start, so if it fails we don't modify the document.
        let reader_len = reader.0.len();
        if let Some(mut crc_reader) = reader.read_chunk(ChunkType::Crc)? {
            // So this is a bit dirty. The bytes which have been checksummed is everything up to
            // (but NOT INCLUDING) the CRC chunk. I could adapt BufReader to store the offset /
            // length. But we can just subtract off the remaining length from the original data??
            // O_o
            if !opts.ignore_crc {
                let expected_crc = crc_reader.next_u32_le()?;
                let checksummed_data = &data[..data.len() - reader_len];

                // TODO: Add flag to ignore invalid checksum.
                if checksum(checksummed_data) != expected_crc {
                    return Err(ChecksumFailed);
                }
            }
        }

        // self.frontier = end_frontier_chunk.read_full_frontier(&self)?;

        Ok(())
    }
}

#[allow(unused)]
fn dbg_print_chunks_in(bytes: &[u8]) {
    BufReader(bytes).dbg_print_chunk_tree();
}

#[cfg(test)]
mod tests {
    use crate::list::{ListCRDT, OpLog};
    use super::*;

    fn simple_doc() -> ListCRDT {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");
        doc.local_delete(0, 3, 4); // 'hi e'
        doc.local_insert(0, 3, "m");
        doc
    }

    fn check_encode_decode_matches(oplog: &OpLog) {
        let data = oplog.encode(EncodeOptions {
            user_data: None,
            store_start_branch_content: true,
            store_inserted_content: true,
            store_deleted_content: true,
            verbose: false,
        });

        let oplog2 = OpLog::load_from(&data).unwrap();

        // dbg!(oplog, &oplog2);

        assert_eq!(oplog, &oplog2);
    }

    #[test]
    fn encode_decode_smoke_test() {
        let doc = simple_doc();
        let data = doc.ops.encode(EncodeOptions::default());

        let result = OpLog::load_from(&data).unwrap();
        // dbg!(&result);

        assert_eq!(&result, &doc.ops);
        // dbg!(&result);
    }

    #[test]
    fn decode_in_parts() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.get_or_create_agent_id("mike");
        doc.local_insert(0, 0, "hi there");

        let data_1 = doc.ops.encode(EncodeOptions::default());
        let f1 = doc.ops.frontier.clone();

        doc.local_delete(1, 3, 4); // 'hi e'
        doc.local_insert(0, 3, "m");

        let data_2 = doc.ops.encode_from(EncodeOptions::default(), &f1);

        let mut d2 = OpLog::new();
        d2.merge_data(&data_1).unwrap();
        d2.merge_data(&data_2).unwrap();

        assert_eq!(&d2, &doc.ops);
        // dbg!(&doc.ops, &d2);
    }

    #[test]
    fn merge_parts() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        oplog.push_insert(0, 0, "hi");
        let data_1 = oplog.encode(EncodeOptions::default());
        oplog.push_insert(0, 2, " there");
        let data_2 = oplog.encode(EncodeOptions::default());

        let mut log2 = OpLog::load_from(&data_1).unwrap();
        println!("\n------\n");
        log2.merge_data(&data_2).unwrap();
        assert_eq!(&oplog, &log2);
    }

    #[test]
    #[ignore]
    fn merge_parts_2() {
        let mut oplog_a = OpLog::new();
        oplog_a.get_or_create_agent_id("a");
        oplog_a.get_or_create_agent_id("b");

        let t1 = oplog_a.push_insert(0, 0, "aa");
        let data_a = oplog_a.encode(EncodeOptions::default());

        oplog_a.push_insert_at(1, &[ROOT_TIME], 0, "bbb");
        let data_b = oplog_a.encode_from(EncodeOptions::default(), &[t1]);

        // Now we should be able to merge a then b, or b then a and get the same result.
        let mut a_then_b = OpLog::new();
        a_then_b.merge_data(&data_a).unwrap();
        a_then_b.merge_data(&data_b).unwrap();
        assert_eq!(a_then_b, oplog_a);

        println!("\n------\n");

        let mut b_then_a = OpLog::new();
        b_then_a.merge_data(&data_b).unwrap();
        b_then_a.merge_data(&data_a).unwrap();
        assert_eq!(b_then_a, oplog_a);
    }

    #[test]
    fn with_deleted_content() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "abcd");
        doc.local_delete_with_content(0, 1, 2); // delete "bc"

        check_encode_decode_matches(&doc.ops);
    }

    #[test]
    fn encode_reordered() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        oplog.get_or_create_agent_id("mike");
        let a = oplog.push_insert_at(0, &[ROOT_TIME], 0, "a");
        oplog.push_insert_at(1, &[ROOT_TIME], 0, "b");
        oplog.push_insert_at(0, &[a], 1, "c");

        // dbg!(&oplog);
        check_encode_decode_matches(&oplog);
    }

    #[test]
    fn encode_with_agent_shared_between_branches() {
        // Same as above, but only one agent ID.
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        let a = oplog.push_insert_at(0, &[ROOT_TIME], 0, "a");
        oplog.push_insert_at(0, &[ROOT_TIME], 0, "b");
        oplog.push_insert_at(0, &[a], 1, "c");

        // dbg!(&oplog);
        check_encode_decode_matches(&oplog);
    }

    #[test]
    #[ignore]
    fn decode_example() {
        let bytes = std::fs::read("../../node_nodecc.dt").unwrap();
        let oplog = OpLog::load_from(&bytes).unwrap();

        // for c in &oplog.client_data {
        //     println!("{} .. {}", c.name, c.get_next_seq());
        // }
        dbg!(oplog.operations.0.len());
        dbg!(oplog.history.entries.0.len());
    }

    #[test]
    #[ignore]
    fn crazy() {
        let bytes = std::fs::read("../../node_nodecc.dt").unwrap();
        let mut reader = BufReader(&bytes);
        reader.read_magic().unwrap();

        loop {
            let (chunk, mut r) = reader.next_chunk().unwrap();
            if chunk == ChunkType::Parents {
                println!("Found it");
                while !r.is_empty() {
                    let n = r.next_usize().unwrap();
                    println!("n {}", n);
                }
                break;
            }
        }
    }

    fn check_unroll_works(dest: &OpLog, src: &OpLog) {
        // So we're going to decode the oplog with all the different bytes corrupted. The result
        // should always fail if we check the CRC.

        let encoded_proper = src.encode(EncodeOptions {
            user_data: None,
            store_start_branch_content: true,
            store_inserted_content: true,
            store_deleted_content: true,
            verbose: false
        });

        // dbg!(encoded_proper.len());
        for i in 0..encoded_proper.len() {
            // let i = 57;
            // println!("{i}");
            // We'll corrupt that byte and try to read the document back.
            let mut corrupted = encoded_proper.clone();
            corrupted[i] = !corrupted[i];
            // dbg!(corrupted[i]);

            let mut actual_output = dest.clone();

            // In theory, we should always get an error here. But we don't, because the CRC check
            // is optional and the corrupted data can just remove the CRC check entirely!

            let result = actual_output.merge_data_opts(&corrupted, DecodeOptions {
                ignore_crc: false,
                verbose: true,
            });

            if let Err(_err) = result {
                assert_eq!(&actual_output, dest);
            } else {
                dbg!(&actual_output);
                dbg!(src);
                assert_eq!(&actual_output, src);
            }
            // Otherwise the data loaded correctly!

        }
    }

    #[test]
    fn error_unrolling() {
        let doc = simple_doc();

        check_unroll_works(&OpLog::new(), &doc.ops);
    }

    #[test]
    fn save_load_save_load() {
        let oplog1 = simple_doc().ops;
        let bytes = oplog1.encode(EncodeOptions {
            user_data: None,
            store_start_branch_content: true,
            // store_inserted_content: true,
            // store_deleted_content: true,
            store_inserted_content: false,
            store_deleted_content: false,
            verbose: false
        });
        dbg_print_chunks_in(&bytes);
        let oplog2 = OpLog::load_from(&bytes).unwrap();
        // dbg!(&oplog2);

        let bytes2 = oplog2.encode(EncodeOptions {
            user_data: None,
            store_start_branch_content: true,
            store_inserted_content: false, // Need to say false here to avoid an assert for this.
            store_deleted_content: true,
            verbose: false
        });
        let oplog3 = OpLog::load_from(&bytes2).unwrap();

        // dbg!(oplog3);
        assert_eq!(oplog2, oplog3);
    }
}
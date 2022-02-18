use std::str::Utf8Error;
use smallvec::SmallVec;
use crate::list::encoding::*;
use crate::list::encoding::varint::*;
use crate::list::{Frontier, OpLog, switch, Time};
use crate::list::remote_ids::ConversionError;
use crate::list::frontier::{frontier_eq, frontier_is_sorted};
use crate::list::internal_op::OperationInternal;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::rev_span::TimeSpanRev;
use crate::{AgentId, ROOT_AGENT, ROOT_TIME};
use crate::unicount::consume_chars;
use ParseError::*;
use rle::AppendRle;
use rle::take_max_iter::TakeMaxFns;
use crate::list::history::MinimalHistoryEntry;
use crate::localtime::{TimeSpan, UNDERWATER_START};
use crate::remotespan::{CRDTId, CRDTSpan};
use crate::rle::{KVPair, RleKeyedAndSplitable, RleSpanHelpers, RleVec};

#[derive(Debug)]
struct BufReader<'a>(&'a [u8]);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ParseError {
    InvalidMagic,
    UnsupportedProtocolVersion,
    InvalidChunkHeader,
    MissingChunk(u32),
    // UnexpectedChunk {
    //     // I could use Chunk here, but I'd rather not expose them publicly.
    //     // expected: Chunk,
    //     // actual: Chunk,
    //     expected: u32,
    //     actual: u32,
    // },
    InvalidLength,
    UnexpectedEOF,
    // TODO: Consider elidiing the details here to keep the wasm binary small.
    InvalidUTF8(Utf8Error),
    InvalidRemoteID(ConversionError),
    InvalidContent,

    ChecksumFailed,

    /// This error is interesting. We're loading a chunk but missing some of the data. In the future
    /// I'd like to explicitly support this case, and allow the oplog to contain a somewhat- sparse
    /// set of data, and load more as needed.
    DataMissing,
}

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

    fn peek_u32(&self) -> Option<u32> {
        if self.is_empty() { return None; }
        // Some(decode_u32(self.0))
        Some(decode_u32(self.0).0)
    }

    fn next_u32(&mut self) -> Result<u32, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u32(self.0);
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
        let (val, count) = decode_u64(self.0);
        self.consume(count);
        Ok(val)
    }

    fn next_usize(&mut self) -> Result<usize, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_usize(self.0);
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

    fn next_chunk(&mut self) -> Result<(Chunk, BufReader<'a>), ParseError> {
        let chunk_type = Chunk::try_from(self.next_u32()?)
            .map_err(|_| InvalidChunkHeader)?;

        // This in no way guarantees we're good.
        let len = self.next_usize()?;
        if len > self.0.len() {
            return Err(InvalidLength);
        }

        Ok((chunk_type, BufReader(self.next_n_bytes(len)?)))
    }

    /// Read a chunk with the named type. Returns None if the next chunk isn't the specified type,
    /// or we hit EOF.
    fn read_chunk(&mut self, expect_chunk_type: Chunk) -> Result<Option<BufReader<'a>>, ParseError> {
        if let Some(actual_chunk_type) = self.peek_u32() {
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

    fn expect_chunk(&mut self, expect_chunk_type: Chunk) -> Result<BufReader<'a>, ParseError> {
        // Scan chunks until we find expect_chunk_type. Error if the chunk is missing from the file.
        while !self.is_empty() {
            let (actual_chunk_type, r) = self.next_chunk()?;
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
                        UNDERWATER_START + c.try_seq_to_time(seq).ok_or(InvalidLength)?
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
        let mut fileinfo = self.expect_chunk(Chunk::FileInfo)?;
        // fileinfo has UserData and AgentNames.

        let userdata = fileinfo.read_chunk(Chunk::UserData)?;
        let mut agent_names_chunk = fileinfo.expect_chunk(Chunk::AgentNames)?;

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
    let len = hist_entry.len();
    hist_entry.span.start = map_entry.start;
    hist_entry.span.end = hist_entry.span.start + len;

    // Map parents.
    const UNDERWATER_LAST: usize = ROOT_TIME - 1;
    for p in hist_entry.parents.iter_mut() {
        match *p {
            ROOT_TIME => {},
            UNDERWATER_START..=UNDERWATER_LAST => {
                // Foreign parents. Just use what we got.
                *p -= UNDERWATER_START;
            }
            _ => {
                let (span, offset) = version_map.find_packed_with_offset(*p);
                *p = span.1.start + offset;
            }
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
        let start = isize::wrapping_add(self.last_cursor_pos as isize, diff) as usize;
        let end = start + len;

        // dbg!(pos);
        self.last_cursor_pos = if tag == Ins && fwd {
            end
        } else {
            start
        };

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


impl OpLog {
    pub fn load_from(data: &[u8]) -> Result<Self, ParseError> {
        Self::new().merge_data(data)
    }

    /// Merge data from the remote source into our local document state.
    ///
    /// NOTE: This code is quite new.
    /// TODO: Currently if this method returns an error, the local state is undefined & invalid.
    /// Until this is fixed, the signature of the method will stay kinda weird to prevent misuse.
    pub fn merge_data(mut self, data: &[u8]) -> Result<Self, ParseError> {
        // Written to be symmetric with encode functions.
        let mut reader = BufReader(data);
        reader.read_magic()?;
        let protocol_version = reader.next_usize()?;
        if protocol_version != PROTOCOL_VERSION {
            return Err(UnsupportedProtocolVersion);
        }

        // *** FileInfo ***
        // fileinfo has UserData and AgentNames.
        // The agent_map is a map from agent_id in the file to agent_id in self.
        // This method adds missing agents to self.
        let (_userdata, mut agent_map) = reader.read_fileinfo(&mut self)?;

        // *** StartBranch ***
        let start_frontier = if let Some(mut start_branch) = reader.read_chunk(Chunk::StartBranch)? {
            let mut start_frontier_chunk = start_branch.expect_chunk(Chunk::Frontier)?;
            let frontier = start_frontier_chunk.read_frontier(&self, &agent_map).map_err(|e| {
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
            let mut patch_chunk = reader.expect_chunk(Chunk::Patches)?;

            let mut ins_content = None;
            let mut del_content = None;

            if let Some(chunk) = patch_chunk.read_chunk(Chunk::InsertedContent)? {
                // let decompressed_len = content_chunk.next_usize()?;
                // let decompressed_data = lz4_flex::decompress(content_chunk.0, decompressed_len).unwrap();
                // let content = String::from_utf8(decompressed_data).unwrap();
                //     // .map_err(InvalidUTF8)?;

                ins_content = Some(std::str::from_utf8(chunk.0)
                    .map_err(InvalidUTF8)?);
            }

            if let Some(chunk) = patch_chunk.read_chunk(Chunk::DeletedContent)? {
                del_content = Some(std::str::from_utf8(chunk.0)
                    .map_err(InvalidUTF8)?);
            }

            // So note that the file we're loading from may contain changes we already have locally.
            // We (may) need to filter out operations from the patch stream, which we read from
            // below. To do that without extra need to read both the agent assignments and patches together.
            let mut agent_assignment_chunk = patch_chunk.expect_chunk(Chunk::AgentAssignment)?;
            let pos_patches_chunk = patch_chunk.expect_chunk(Chunk::PositionalPatches)?;
            let mut history_chunk = patch_chunk.expect_chunk(Chunk::TimeDAG)?;

            let mut patches_iter = ReadPatchesIter::new(pos_patches_chunk)
                .take_max();

            let first_new_time = self.len();
            let mut next_patch_time = first_new_time;

            // The file we're loading has a list of operations. The list's item order is shared in a
            // handful of lists of data - agent assignment, operations, content and txns.

            // Only used for new (not overlapped) operations.
            let mut next_assignment_time = first_new_time;
            let mut next_history_time = first_new_time;
            let mut next_file_time = 0;

            // Mapping from "file order" (numbered from 0) to the resulting local order. Using a
            // smallvec here because it'll almost always just be a single entry, and that prevents
            // an allocation in the common case. This is needed for merging overlapped file data.
            // let mut version_map: SmallVec<[KVPair<TimeSpan>; 1]> = SmallVec::new();

            // TODO: Replace with SmallVec to avoid an allocation in the common case here.
            let mut version_map = RleVec::new();

            // Take and merge the next exactly n patches
            let mut parse_next_patches = |oplog: &mut OpLog, mut n: usize, keep: bool| -> Result<(), ParseError> {
                while n > 0 {
                    if let Some(op) = patches_iter.next(n) {
                        let op = op?;
                        // dbg!((n, &op));
                        let len = op.len();
                        assert!(op.len() > 0);
                        n -= len;

                        let content = switch(op.tag, &mut ins_content, &mut del_content);

                        // TODO: Check this split point is valid.
                        let content_here = content.as_mut()
                            .map(|content| consume_chars(content, len));

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
                            false
                        } else {
                            self.assign_next_time_to_crdt_span(next_assignment_time, CRDTSpan {
                                agent: crdt_span.agent,
                                seq_range: consume_here,
                            });
                            version_map.push_rle(KVPair(
                                next_file_time,
                                (next_assignment_time..next_assignment_time + len).into(),
                            ));
                            next_assignment_time += len;
                            true
                        };
                        next_file_time += len;

                        // dbg!(&file_to_local_version_map);

                        parse_next_patches(&mut self, len, keep)?;

                        // And deal with history.
                        // parse_next_history(&mut self, &file_to_self_agent_map, &version_map, len, keep)?;
                    }
                    // dbg!(span);
                } else {
                    // Optimization - don't bother with the filtering code above if loaded changes
                    // follow local changes. Most calls to this function load into an empty
                    // document, and this is the case.
                    self.assign_next_time_to_crdt_span(next_assignment_time, crdt_span);
                    let len = crdt_span.len();
                    // file_to_local_version_map.push_rle((next_assignment_time..next_assignment_time + len).into());
                    version_map.push_rle(KVPair(
                        next_file_time,
                        (next_assignment_time..next_assignment_time + len).into(),
                    ));
                    parse_next_patches(&mut self, len, true)?;
                    // parse_next_history(&mut self, &file_to_self_agent_map, &version_map, len, true)?;

                    next_assignment_time += len;
                    next_file_time += len;
                }
            }

            next_file_time = 0;
            // dbg!(&version_map);
            while !history_chunk.is_empty() {
                let mut entry = history_chunk.next_history_entry(&self, next_file_time, &agent_map)?;
                next_file_time += entry.len();
                // dbg!(&entry);

                loop {
                    let (mut mapped, remainder) = history_entry_map_and_truncate(entry, &version_map);
                    // dbg!(&mapped);
                    assert!(mapped.span.start <= next_history_time);

                    if mapped.span.end > next_history_time {
                        // We'll merge items from mapped.
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

            if let Some(content) = ins_content {
                if !content.is_empty() {
                    return Err(InvalidContent);
                }
            }

            // dbg!(&version_map);
        } // End of patches

        // TODO: Move checksum check to the start, so if it fails we don't modify the document.
        let reader_len = reader.0.len();
        if let Some(mut crc_reader) = reader.read_chunk(Chunk::CRC)? {
            // So this is a bit dirty. The bytes which have been checksummed is everything up to
            // (but NOT INCLUDING) the CRC chunk. I could adapt BufReader to store the offset /
            // length. But we can just subtract off the remaining length from the original data??
            // O_o
            let expected_crc = crc_reader.next_u32_le()?;
            let checksummed_data = &data[..data.len() - reader_len];

            // TODO: Add flag to ignore invalid checksum.
            if checksum(checksummed_data) != expected_crc {
                return Err(ChecksumFailed);
            }
        }

        // self.frontier = end_frontier_chunk.read_full_frontier(&self)?;

        Ok(self)
    }
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
        d2 = d2.merge_data(&data_1).unwrap();
        d2 = d2.merge_data(&data_2).unwrap();

        assert_eq!(&d2, &doc.ops);
        // dbg!(&doc.ops, &d2);
    }

    #[test]
    // #[ignore]
    fn merge_parts() {
        let mut oplog = OpLog::new();
        oplog.get_or_create_agent_id("seph");
        oplog.push_insert(0, 0, "hi");
        let data_1 = oplog.encode(EncodeOptions::default());
        oplog.push_insert(0, 2, " there");
        let data_2 = oplog.encode(EncodeOptions::default());

        let log2 = OpLog::load_from(&data_1).unwrap();
        println!("\n------\n");
        let log2 = log2.merge_data(&data_2).unwrap();
        assert_eq!(&oplog, &log2);
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
        dbg!(oplog.operations.len());
        dbg!(oplog.history.entries.len());
    }

    #[test]
    #[ignore]
    fn crazy() {
        let bytes = std::fs::read("../../node_nodecc.dt").unwrap();
        let mut reader = BufReader(&bytes);
        reader.read_magic().unwrap();

        loop {
            let (chunk, mut r) = reader.next_chunk().unwrap();
            if chunk == Chunk::TimeDAG {
                println!("Found it");
                while !r.is_empty() {
                    let n = r.next_usize().unwrap();
                    println!("n {}", n);
                }
                break;
            }
        }
    }
}
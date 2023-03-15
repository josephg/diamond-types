use smallvec::{smallvec, SmallVec};
use crate::list::encoding::*;
use crate::list::{ListOpLog, switch};
use crate::frontier::*;
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::ListOpKind::{Del, Ins};
use crate::rev_range::RangeRev;
use crate::{AgentId, Frontier, LV};
use crate::unicount::*;
use rle::*;
use crate::list::buffered_iter::Buffered;
use crate::list::encoding::ListChunkType::*;
use crate::causalgraph::graph::GraphEntrySimple;
use crate::list::operation::ListOpKind;
use crate::dtrange::{DTRange, UNDERWATER_START};
use crate::list::encoding::decode_tools::{BufReader, ChunkReader};
use crate::causalgraph::agent_span::AgentSpan;
use crate::rle::{KVPair, RleKeyedAndSplitable, RleSpanHelpers, RleVec};
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::calc_checksum;
use crate::list::encoding::leb::num_decode_zigzag_isize_old;

// If this is set to false, the compiler can optimize out the verbose printing code. This makes the
// compiled output slightly smaller.
const ALLOW_VERBOSE: bool = false;
// const ALLOW_VERBOSE: bool = true;

impl<'a> BufReader<'a> {
    fn read_next_agent_assignment(&mut self, map: &mut [(AgentId, usize)]) -> Result<Option<AgentSpan>, ParseError> {
        // Agent assignments are almost always (but not always) linear. They can have gaps, and
        // they can be reordered if the same agent ID is used to contribute to multiple branches.
        //
        // I'm still not sure if this is a good idea.

        if self.0.is_empty() { return Ok(None); }

        let mut n = self.next_usize()?;
        let has_jump = strip_bit_usize_2(&mut n);
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

        Ok(Some(AgentSpan {
            agent,
            seq_range: (start..end).into(),
        }))
    }

    fn read_version(mut self, oplog: &ListOpLog, agent_map: &[(AgentId, usize)]) -> Result<Frontier, ParseError> {
        let mut result = smallvec![];
        // All frontiers contain at least one item.
        loop {
            // let agent = reader.next_str()?;
            let (mapped_agent, has_more) = strip_bit_usize(self.next_usize()?);
            let seq = self.next_usize()?; // Bleh. Skip me when root!
            if mapped_agent == 0 { break; } // Root.

            let agent = agent_map[mapped_agent - 1].0;

            let time = oplog.try_crdt_id_to_time((agent, seq))
                .ok_or(ParseError::BaseVersionUnknown)?;
            result.push(time);

            if !has_more { break; }
        }

        sort_frontier(&mut result);

        self.expect_empty()?;

        Ok(Frontier(result))
    }

    fn read_parents(&mut self, oplog: &ListOpLog, next_time: LV, agent_map: &[(AgentId, usize)]) -> Result<Frontier, ParseError> {
        let mut parents = SmallVec::<[usize; 2]>::new();
        loop {
            let mut n = self.next_usize()?;
            let is_foreign = strip_bit_usize_2(&mut n);
            let has_more = strip_bit_usize_2(&mut n);

            let parent = if is_foreign {
                if n == 0 {
                    // The parents list is empty (ie, our parent is ROOT).
                    break;
                } else {
                    let agent = agent_map[n - 1].0;
                    let seq = self.next_usize()?;
                    // dbg!((agent, seq));
                    if let Some(c) = oplog.cg.agent_assignment.client_data.get(agent as usize) {
                        // Adding UNDERWATER_START for foreign parents in a horrible hack.
                        // I'm so sorry. This gets pulled back out in history_entry_map_and_truncate
                        c.try_seq_to_lv(seq).ok_or(ParseError::InvalidLength)?
                    } else {
                        return Err(ParseError::InvalidLength);
                    }
                }
            } else {
                // Local parents (parents inside this chunk of data) are stored using their
                // local time offset.
                next_time - n
            };

            parents.push(parent);
            // debug_assert!(frontier_is_sorted(&parents));

            if !has_more { break; }
        }

        // So this is awkward. There's two reasons parents could end up unsorted:
        // 1. The file is invalid. All local (non-foreign) changes should be in order).
        // or 2. We have foreign items - and they're not sorted based on the local versions.
        // This is fine and we should just re-sort.
        sort_frontier(&mut parents);

        Ok(Frontier(parents))
    }

    fn next_history_entry(&mut self, oplog: &ListOpLog, next_time: LV, agent_map: &[(AgentId, usize)]) -> Result<GraphEntrySimple, ParseError> {
        let len = self.next_usize()?;
        let parents = self.read_parents(oplog, next_time, agent_map)?;

        // Bleh its gross passing a &[Time] into here when we have a Frontier already.
        Ok(GraphEntrySimple {
            span: (next_time..next_time + len).into(),
            parents,
        })
    }

}

impl<'a> ChunkReader<'a> {
    fn read_version(&mut self, oplog: &ListOpLog, agent_map: &[(AgentId, usize)]) -> Result<Frontier, ParseError> {
        let chunk = self.read_chunk_if_eq(ListChunkType::Version)?;
        if let Some(chunk) = chunk {
            chunk.read_version(oplog, agent_map).map_err(|e| {
                // We can't read a frontier if it names agents or sequence numbers we haven't seen
                // before. If this happens, its because we're trying to load a data set from the
                // future.
                //
                // That should be possible - if we prune history, we should be able to load a
                // data set from some future version and just set start_version and start_content
                // properties on the oplog. But thats NYI!

                // TODO: Remove this!
                if let ParseError::InvalidRemoteID(_) = e {
                    ParseError::DataMissing
                } else { e }
            })
        } else {
            // If the start_frontier chunk is missing, it means we're reading from ROOT.
            Ok(Frontier::root())
        }
    }

    fn expect_content_str(&mut self, compressed: Option<&mut BufReader<'a>>) -> Result<&'a str, ParseError> {
        let (c, mut r) = self.expect_chunk_pred(|c| c == Content || c == ContentCompressed, Content)?;

        if c == Content {
            // Just read the string straight out.
            r.into_content_str()
        } else {
            let data_type = r.next_u32()?;
            if data_type != (DataType::PlainText as u32) {
                return Err(ParseError::UnknownChunk);
            }
            // The uncompressed length
            let len = r.next_usize()?;

            let bytes = compressed.ok_or(ParseError::CompressedDataMissing)?
                .next_n_bytes(len)?;

            std::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUTF8)
        }
    }

    fn read_fileinfo(&mut self, oplog: &mut ListOpLog) -> Result<FileInfoData, ParseError> {
        let mut fileinfo = self.expect_chunk(ListChunkType::FileInfo)?.chunks();

        let doc_id = fileinfo.read_chunk_if_eq(ListChunkType::DocId)?;
        let mut agent_names_chunk = fileinfo.expect_chunk(ListChunkType::AgentNames)?;
        let userdata = fileinfo.read_chunk_if_eq(ListChunkType::UserData)?;

        let doc_id = if let Some(doc_id) = doc_id {
            Some(doc_id.into_content_str()?)
        } else { None };

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

        Ok(FileInfoData {
            userdata,
            doc_id,
            agent_map,
        })
    }
}


// Returning a tuple was getting too unwieldy.
#[derive(Debug)]
struct FileInfoData<'a> {
    userdata: Option<BufReader<'a>>,
    doc_id: Option<&'a str>,
    agent_map: Vec<(AgentId, usize)>,
}


/// Returns (mapped span, remainder).
/// The returned remainder is *NOT MAPPED*. This allows this method to be called in a loop.
fn history_entry_map_and_truncate(mut hist_entry: GraphEntrySimple, version_map: &RleVec<KVPair<DTRange>>) -> (GraphEntrySimple, Option<GraphEntrySimple>) {
    let (map_entry, offset) = version_map.find_packed_with_offset(hist_entry.span.start);

    let mut map_entry = map_entry.1;
    map_entry.truncate_keeping_right(offset);

    let remainder = hist_entry.trim(map_entry.len());

    // Keep entire history entry. Just map it.
    let len = hist_entry.len(); // hist_entry <= map_entry here.
    hist_entry.span.start = map_entry.start;
    hist_entry.span.end = hist_entry.span.start + len;

    // dbg!(&hist_entry.parents);

    // Map parents. Parents are underwater when they're local to the file, and need mapping.
    // const UNDERWATER_LAST: usize = ROOT_TIME - 1;
    for p in hist_entry.parents.0.iter_mut() {
        if *p >= UNDERWATER_START {
            let (span, offset) = version_map.find_packed_with_offset(*p);
            *p = span.1.start + offset;
        }
    }

    // Parents can become unsorted here because they might not map cleanly. Thanks, fuzzer.
    sort_frontier(&mut hist_entry.parents.0);

    (hist_entry, remainder)
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
    fn next_internal(&mut self) -> Result<ListOpMetrics, ParseError> {
        let mut n = self.buf.next_usize()?;
        // This is in the opposite order from write_op.
        let has_length = strip_bit_usize_2(&mut n);
        let diff_not_zero = strip_bit_usize_2(&mut n);
        let tag = if strip_bit_usize_2(&mut n) { Del } else { Ins };

        let (len, diff, fwd) = if has_length {
            // n encodes len.
            let fwd = if tag == Del {
                strip_bit_usize_2(&mut n)
            } else { true };

            let diff = if diff_not_zero {
                self.buf.next_zigzag_isize()?
            } else { 0 };

            (n, diff, fwd)
        } else {
            // n encodes diff.
            let diff = num_decode_zigzag_isize_old(n);
            (1, diff, true)
        };

        // dbg!(self.last_cursor_pos, diff);
        let raw_start = isize::wrapping_add(self.last_cursor_pos as isize, diff) as usize;

        let (start, raw_end) = match (tag, fwd) {
            (Ins, true) => (raw_start, raw_start + len),
            (Ins, false) | (Del, true) => (raw_start, raw_start), // Weird symmetry!
            (Del, false) => (raw_start - len, raw_start - len),
        };
        // dbg!((raw_start, tag, fwd, len, start, raw_end));

        let end = start + len;

        // dbg!(pos);
        self.last_cursor_pos = raw_end;
        // dbg!(self.last_cursor_pos);

        Ok(ListOpMetrics {
            loc: RangeRev { // TODO: Probably a nicer way to construct this.
                span: (start..end).into(),
                fwd,
            },
            kind: tag,
            content_pos: None,
        })
    }
}

impl<'a> Iterator for ReadPatchesIter<'a> {
    type Item = Result<ListOpMetrics, ParseError>;

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

impl<'a> SplitableSpanHelpers for ContentItem<'a> {
    fn truncate_h(&mut self, at: usize) -> Self {
        let content_remainder = if let Some(str) = self.content.as_mut() {
            let (here, remainder) = split_at_char(str, at);
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
    fn new(mut chunk: BufReader<'a>, compressed: Option<&mut BufReader<'a>>) -> Result<(ListOpKind, Self), ParseError> {
        let tag = match chunk.next_u32()? {
            0 => Ins,
            1 => Del,
            _ => { return Err(ParseError::InvalidContent); }
        };

        let mut chunk = chunk.chunks();
        let content = chunk.expect_content_str(compressed)?;

        let run_chunk = chunk.expect_chunk(ContentIsKnown)?;

        Ok((tag, Self { run_chunk, content }))
    }

    fn next_internal(&mut self) -> Result<ContentItem<'a>, ParseError> {
        let n = self.run_chunk.next_usize()?;
        let (len, known) = strip_bit_usize(n);
        let content = if known {
            let content = consume_chars(&mut self.content, len);
            if count_chars(content) != len { // Having a duplicate strlen here is gross.
                // We couldn't pull as many chars as requested from self.content.
                return Err(ParseError::UnexpectedEOF);
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
    pub ignore_crc: bool,

    pub verbose: bool,
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

impl ListOpLog {
    pub fn load_from(data: &[u8]) -> Result<Self, ParseError> {
        let mut oplog = Self::new();
        oplog.decode_internal(data, DecodeOptions::default())?;
        Ok(oplog)
    }

    pub fn load_from_opts(data: &[u8], opts: DecodeOptions) -> Result<Self, ParseError> {
        let mut oplog = Self::new();
        oplog.decode_internal(data, opts)?;
        Ok(oplog)
    }

    /// Add all operations from a binary chunk into this document.
    ///
    /// Any duplicate operations are ignored.
    ///
    /// This method is a convenience method for calling
    /// [`oplog.decode_and_add_opts(data, DecodeOptions::default())`](OpLog::decode_and_add_opts).
    pub fn decode_and_add(&mut self, data: &[u8]) -> Result<Frontier, ParseError> {
        self.decode_and_add_opts(data, DecodeOptions::default())
    }

    /// Add all operations from a binary chunk into this document.
    ///
    /// If successful, returns the version of the loaded data (which could be different from the
    /// local version!)
    ///
    /// This method takes an options object, which for now doesn't do much. Most users should just
    /// call [`OpLog::decode_and_add`](OpLog::decode_and_add)
    pub fn decode_and_add_opts(&mut self, data: &[u8], opts: DecodeOptions) -> Result<Frontier, ParseError> {
        // In order to merge data safely, when an error happens we need to unwind all the merged
        // operations before returning. Otherwise self is in an invalid state.
        //
        // The merge_data method is append-only, so really we just need to trim back all the data
        // that has been (partially) added.

        // Total (unmerged) number of operations before this data is merged in.
        let len = self.len();

        // We could regenerate the frontier, but this is much lazier.
        let doc_id = self.doc_id.clone();
        let old_frontier = self.cg.version.clone();
        let num_known_agents = self.cg.agent_assignment.client_data.len();
        let ins_content_length = self.operation_ctx.ins_content.len();
        let del_content_length = self.operation_ctx.del_content.len();

        let result = self.decode_internal(data, opts);

        if result.is_err() {
            // Unwind changes back to len.
            // This would be nicer with an RleVec iterator, but the iter implementation doesn't
            // support iterating backwards.
            self.doc_id = doc_id;

            while let Some(last) = self.cg.agent_assignment.client_with_localtime.0.last_mut() {
                debug_assert!(len <= last.end());
                if len == last.end() { break; }
                else {
                    // Truncate!
                    let KVPair(_, removed) = if len <= last.0 {
                        // Drop entire entry
                        self.cg.agent_assignment.client_with_localtime.0.pop().unwrap()
                    } else {
                        last.truncate(len - last.0)
                    };

                    let client_data = &mut self.cg.agent_assignment.client_data[removed.agent as usize];
                    client_data.item_times.remove_ctx(removed.seq_range, &());
                }
            }

            let num_operations = self.operations.end();
            if num_operations > len {
                self.operations.remove_ctx((len..num_operations).into(), &self.operation_ctx);
            }

            // Trim history
            let hist_entries = &mut self.cg.graph.entries;
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
                        if p < len { // If p >= len, the target will be discarded anyway.
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

                self.cg.graph.entries.0.truncate(first_truncated_idx);

                while let Some(&last_idx) = self.cg.graph.root_child_indexes.last() {
                    if last_idx >= self.cg.graph.entries.num_entries() {
                        self.cg.graph.root_child_indexes.pop();
                    } else { break; }
                }
            }

            // Remove excess agents
            self.cg.agent_assignment.client_data.truncate(num_known_agents);

            self.operation_ctx.ins_content.truncate(ins_content_length);
            self.operation_ctx.del_content.truncate(del_content_length);

            self.cg.version = old_frontier;
        }

        result
    }

    /// Merge data from the remote source into our local document state.
    ///
    /// NOTE: This code is quite new.
    /// TODO: Currently if this method returns an error, the local state is undefined & invalid.
    /// Until this is fixed, the signature of the method will stay kinda weird to prevent misuse.
    fn decode_internal(&mut self, data: &[u8], opts: DecodeOptions) -> Result<Frontier, ParseError> {
        // Written to be symmetric with encode functions.
        let mut reader = BufReader(data);

        let verbose = ALLOW_VERBOSE && opts.verbose;
        if verbose {
            reader.clone().dbg_print_chunk_tree();
        }

        reader.read_magic()?;
        let protocol_version = reader.next_usize()?;
        if protocol_version != PROTOCOL_VERSION {
            return Err(ParseError::UnsupportedProtocolVersion);
        }

        // The rest of the file is made of chunks!
        let mut reader = reader.chunks();

        // *** Compressed data ***
        // If there is a compressed chunk, it can contain data for other fields, all mushed
        // together.
        let mut compressed_chunk;

        #[cfg(not(feature = "lz4"))] {
            compressed_chunk = None;
            if reader.read_chunk_if_eq(ListChunkType::CompressedFieldsLZ4)?.is_some() {
                return Err(ParseError::LZ4DecoderNeeded);
            }
        }

        let _compressed_chunk_raw: Option<Vec<u8>>; // Pulled out so its lifetime escapes the block.
        #[cfg(feature = "lz4")] {
            _compressed_chunk_raw = if let Some(mut c) = reader.read_chunk_if_eq(ListChunkType::CompressedFieldsLZ4)? {
                let uncompressed_len = c.next_usize()?;

                // The rest of the bytes contain lz4 compressed data.
                let data = lz4_flex::decompress(c.0, uncompressed_len)
                    .map_err(|_e| ParseError::LZ4DecompressionError)?;
                Some(data)
            } else { None };

            // To consume from compressed_chunk_raw, we'll make a slice that we can iterate through.
            compressed_chunk = _compressed_chunk_raw.as_ref().map(|b| BufReader(b));
        }

        // *** FileInfo ***
        // fileinfo has DocID, UserData and AgentNames.
        // The agent_map is a map from agent_id in the file to agent_id in self.
        let FileInfoData {
            userdata: _userdata, doc_id, mut agent_map,
        } = reader.read_fileinfo(self)?;

        // If we already have a doc_id, make sure they match before merging.
        if let Some(file_doc_id) = doc_id {
            if let Some(local_doc_id) = self.doc_id.as_ref() {
                if file_doc_id != local_doc_id && !self.is_empty() {
                    return Err(ParseError::DocIdMismatch);
                }
            }
            self.doc_id = Some(file_doc_id.into());
        }

        // *** StartBranch ***
        let mut start_branch = reader.expect_chunk(ListChunkType::StartBranch)?.chunks();

        // Start version - which if missing defaults to ROOT ([]).
        let start_version = start_branch.read_version(self, &agent_map)?;

        // The start branch also optionally contains the document content at this version. We can't
        // use it yet (NYI) but it needs to be parsed because it because it might be compressed.
        if !start_branch.is_empty() {
            let _start_content = start_branch.expect_content_str(compressed_chunk.as_mut())?;
            // dbg!(start_content);
            // TODO! Attach start_content if we're empty and start_version != ROOT.
        }

        // Usually the version data will be strictly separated. Either we're loading data into an
        // empty document, or we've been sent catchup data from a remote peer. If the data set
        // overlaps, we need to actively filter out operations & txns from that data set.
        // dbg!(&start_frontier, &self.frontier);
        let patches_overlap = !local_frontier_eq(start_version.as_ref(), self.cg.version.as_ref());
        // dbg!(patches_overlap);

        // *** Patches ***
        let file_frontier = {
            // This chunk contains the actual set of edits to the document.
            let mut patch_chunk = reader.expect_chunk(ListChunkType::Patches)?
                .chunks();

            let mut ins_content = None;
            let mut del_content = None;

            while let Some(chunk) = patch_chunk.read_chunk_if_eq(ListChunkType::PatchContent)? {
                let (tag, content_chunk) = ReadPatchContentIter::new(chunk, compressed_chunk.as_mut())?;
                // let iter = content_chunk.take_max();
                let iter = content_chunk.buffered();
                match tag {
                    Ins => { ins_content = Some(iter); }
                    Del => { del_content = Some(iter); }
                }
            }

            // So note that the file we're loading from may contain changes we already have locally.
            // We (may) need to filter out operations from the patch stream, which we read from
            // below. To do that without extra need to read both the agent assignments and patches together.
            let mut agent_assignment_chunk = patch_chunk.expect_chunk(ListChunkType::OpVersions)?;
            let pos_patches_chunk = patch_chunk.expect_chunk(ListChunkType::OpTypeAndPosition)?;
            let mut history_chunk = patch_chunk.expect_chunk(ListChunkType::OpParents)?;

            // We need an insert ctx in some situations, though it'll never be accessed.
            let dummy_ctx = ListOperationCtx::new();

            let mut patches_iter = ReadPatchesIter::new(pos_patches_chunk)
                .buffered();

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
            //
            // If the data (key) overlaps, the value is the location in the document where the
            // overlap happens.
            //
            // If the data does not overlap (so we're gonna merge & keep this data), this maps to
            // the set of local version numbers which will be used for this data.

            // TODO: Replace with SmallVec to avoid an allocation in the common case here.
            // let mut version_map: SmallVec<[KVPair<TimeSpan>; 1]> = SmallVec::new();
            let mut version_map = RleVec::new();

            // Take and merge the next exactly n patches
            let mut parse_next_patches = |oplog: &mut ListOpLog, mut n: usize, keep: bool| -> Result<(), ParseError> {
                while n > 0 {
                    let mut max_len = n;

                    if let Some(op) = patches_iter.next() {
                        let mut op = op?;
                        // dbg!((n, &op));
                        max_len = max_len.min(op.len());

                        // Trim down the operation to size.
                        let content_here = if let Some(iter) = switch(op.kind, &mut ins_content, &mut del_content) {
                            // There's probably a way to compact with Option helpers magic but ??
                            if let Some(content) = iter.next() {
                                let mut content = content?;
                                max_len = max_len.min(content.len);
                                // Put the rest (if any) back into the iterator.
                                if let Some(r) = content.trim(max_len) {
                                    iter.push_back(Ok(r));
                                }
                                content.content
                            } else {
                                return Err(ParseError::InvalidLength);
                            }
                        } else { None };

                        assert!(max_len > 0);
                        n -= max_len;

                        let remainder = op.trim_ctx(max_len, &dummy_ctx);

                        // dbg!(keep, (next_patch_time, &op, content_here));

                        // self.operations.push(KVPair(next_time, op));
                        if keep {
                            oplog.push_op_internal(next_patch_time, op.loc, op.kind, content_here);
                            next_patch_time += max_len;
                        }

                        if let Some(r) = remainder {
                            patches_iter.push_back(Ok(r));
                        }
                    } else {
                        return Err(ParseError::InvalidLength);
                    }
                }

                Ok(())
            };

            while let Some(mut crdt_span) = agent_assignment_chunk.read_next_agent_assignment(&mut agent_map)? {
                // let mut crdt_span = crdt_span; // TODO: Remove me. Blerp clion.
                // dbg!(crdt_span);
                if crdt_span.agent as usize >= self.cg.agent_assignment.client_data.len() {
                    return Err(ParseError::InvalidLength);
                }

                if patches_overlap {
                    // Sooo, if the current document overlaps with the data we're loading, we need
                    // to filter out all the operations we already have from the stream.
                    while !crdt_span.seq_range.is_empty() {
                        // dbg!(&crdt_span);
                        let client = &self.cg.agent_assignment.client_data[crdt_span.agent as usize];
                        let (span, offset) = client.item_times.find_sparse(crdt_span.seq_range.start);
                        // dbg!((crdt_span.seq_range, span, offset));
                        let (span_end, overlap_start) = match span {
                            // Skip the entry.
                            Ok(entry) => (entry.end(), Some(entry.1.start + offset)),
                            // Consume the entry
                            Err(empty_span) => (empty_span.end, None),
                        };

                        let end = crdt_span.seq_range.end.min(span_end);
                        let consume_here = crdt_span.seq_range.truncate_keeping_right_from(end);
                        let len = consume_here.len();

                        let keep = if let Some(overlap_start) = overlap_start {
                            let overlap = (overlap_start .. overlap_start + len).into();
                            // There's overlap. We'll filter out this item.
                            version_map.push_rle(KVPair(next_file_time, overlap));
                            // println!("push overlap {:?}", KVPair(next_file_time, overlap));
                            false
                        } else {
                            self.assign_time_to_crdt_span(next_assignment_time, AgentSpan {
                                agent: crdt_span.agent,
                                seq_range: consume_here,
                            });

                            // println!("push to end {:?}", KVPair(
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
                    let timespan = (next_assignment_time..next_assignment_time+len).into();
                    // file_to_local_version_map.push_rle((next_assignment_time..next_assignment_time + len).into());
                    version_map.push_rle(KVPair(next_file_time, timespan));
                    parse_next_patches(self, len, true)?;
                    // parse_next_history(&mut self, &file_to_self_agent_map, &version_map, len, true)?;

                    next_assignment_time += len;
                    next_file_time += len;
                }
            }

            next_file_time = new_op_start;
            // dbg!(&version_map);
            let mut next_history_time = first_new_time;

            let mut file_frontier = start_version;

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
                    let (mut mapped, remainder)
                        = history_entry_map_and_truncate(entry, &version_map);
                    // dbg!(&mapped);
                    mapped.parents.debug_check_sorted();
                    assert!(mapped.span.start <= next_history_time);

                    // We'll update merge parents even if nothing is merged.
                    // dbg!((&file_frontier, &mapped));
                    file_frontier.advance_by_known_run(mapped.parents.as_ref(), mapped.span);
                    // dbg!(&file_frontier);

                    if mapped.span.end > next_history_time {
                        // We'll merge items from mapped.

                        // This is needed because the overlapping & new items aren't strictly
                        // separated in version_map. Its kinda ugly though - I'd like a better way
                        // to deal with this case.
                        if mapped.span.start < next_history_time {
                            mapped.truncate_keeping_right(next_history_time - mapped.span.start);
                        }

                        self.cg.graph.push(mapped.parents.as_ref(), mapped.span);
                        self.cg.version.advance_by_known_run(mapped.parents.as_ref(), mapped.span);

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
            if next_patch_time != next_assignment_time { return Err(ParseError::InvalidLength); }
            if next_patch_time != next_history_time { return Err(ParseError::InvalidLength); }

            // dbg!(&patch_chunk);
            patch_chunk.expect_empty()?;
            history_chunk.expect_empty()?;

            if let Some(mut iter) = ins_content {
                if iter.next().is_some() {
                    return Err(ParseError::InvalidContent);
                }
            }

            if let Some(mut iter) = del_content {
                if iter.next().is_some() {
                    return Err(ParseError::InvalidContent);
                }
            }

            // dbg!(&version_map);
            file_frontier
        }; // End of patches

        // TODO: Move checksum check to the start, so if it fails we don't modify the document.
        let reader_len = reader.0.len();
        if let Some(mut crc_reader) = reader.read_chunk_if_eq(ListChunkType::Crc)? {
            // So this is a bit dirty. The bytes which have been checksummed is everything up to
            // (but NOT INCLUDING) the CRC chunk. I could adapt BufReader to store the offset /
            // length. But we can just subtract off the remaining length from the original data??
            // O_o
            if !opts.ignore_crc {
                let expected_crc = crc_reader.next_u32_le()?;
                let checksummed_data = &data[..data.len() - reader_len];

                // TODO: Add flag to ignore invalid checksum.
                if calc_checksum(checksummed_data) != expected_crc {
                    return Err(ParseError::ChecksumFailed);
                }
            }
        }

        // self.frontier = end_frontier_chunk.read_full_frontier(&self)?;

        Ok(file_frontier)
    }
}

#[allow(unused)]
pub(super) fn dbg_print_chunks_in(bytes: &[u8]) {
    BufReader(bytes).dbg_print_chunk_tree();
}

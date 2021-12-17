use std::str::Utf8Error;
use crate::list::encoding::*;
use crate::list::encoding::varint::*;
use crate::list::{Frontier, OpLog, switch};
use crate::list::remote_ids::{ConversionError, RemoteId};
use crate::list::frontier::{frontier_eq, frontier_is_sorted};
use crate::list::internal_op::OperationInternal;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::localtime::TimeSpan;
use crate::rev_span::TimeSpanRev;
use crate::{AgentId, ROOT_TIME};
use crate::unicount::consume_chars;
use ParseError::*;
use crate::remotespan::CRDTSpan;

#[derive(Debug)]
struct BufReader<'a>(&'a [u8]);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ParseError {
    InvalidMagic,
    InvalidChunkHeader,
    UnexpectedChunk {
        // I could use Chunk here, but I'd rather not expose them publicly.
        expected: Chunk,
        actual: Chunk,
        // expected: u32,
        // actual: u32,
    },
    InvalidLength,
    UnexpectedEOF,
    // TODO: Consider elidiing the details here to keep the wasm binary small.
    InvalidUTF8(Utf8Error),
    InvalidRemoteID(ConversionError),
    InvalidContent,

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
        if self.0.len() < num { Err(UnexpectedEOF) }
        else { Ok(()) }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
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
        if self.0[..MAGIC_BYTES_SMALL.len()] != MAGIC_BYTES_SMALL {
            return Err(InvalidMagic);
        }
        self.consume(8);
        Ok(())
    }

    fn next_u32(&mut self) -> Result<u32, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u32(self.0);
        self.consume(count);
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

    fn expect_chunk(&mut self, expect_chunk_type: Chunk) -> Result<BufReader<'a>, ParseError> {
        let (actual_chunk_type, r) = self.next_chunk()?;
        if expect_chunk_type != actual_chunk_type {
            dbg!(expect_chunk_type, actual_chunk_type);

            return Err(UnexpectedChunk {
                expected: expect_chunk_type as _,
                actual: actual_chunk_type as _,
            });
        }

        Ok(r)
    }

    // Note the result is attached to the lifetime 'a, not the lifetime of self.
    fn next_str(&mut self) -> Result<&'a str, ParseError> {
        if self.0.is_empty() { return Err(UnexpectedEOF); }

        let len = self.next_usize()?;
        if len > self.0.len() { return Err(InvalidLength); }

        let bytes = self.next_n_bytes(len)?;
        std::str::from_utf8(bytes).map_err(InvalidUTF8)
    }

    // fn next_run_u32(&mut self) -> Result<Option<Run<u32>>, ParseError> {
    //     if self.0.is_empty() { return Ok(None); }
    //
    //     let n = self.next_u32()?;
    //     let (val, has_len) = strip_bit_u32(n);
    //
    //     let len = if has_len {
    //         self.next_usize()?
    //     } else {
    //         1
    //     };
    //     Ok(Some(Run { val, len }))
    // }

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

        let inner_agent = n;
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
            seq_range: (start..end).into()
        }))
    }

    fn read_full_frontier(&mut self, oplog: &OpLog) -> Result<Frontier, ParseError> {
        let mut result = Frontier::new();
        // All frontiers contain at least one item.
        loop {
            let agent = self.next_str()?;
            let n = self.next_usize()?;
            let (seq, has_more) = strip_bit_usize(n);

            let time = oplog.try_remote_id_to_time(&RemoteId {
                agent: agent.into(),
                seq
            }).map_err(InvalidRemoteID)?;

            result.push(time);

            if !has_more { break; }
        }

        if !frontier_is_sorted(result.as_slice()) {
            // TODO: Check how this effects wasm bundle size.
            result.sort_unstable();
        }

        Ok(result)
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
            last_cursor_pos: 0
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
                fwd
            },
            tag,
            content_pos: None
        })
    }
}

impl<'a> Iterator for ReadPatchesIter<'a> {
    type Item = Result<OperationInternal, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { None }
        else { Some(self.next_internal()) }
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

        let _info = reader.expect_chunk(Chunk::FileInfo)?;

        let mut start_frontier_chunk = reader.expect_chunk(Chunk::StartFrontier)?;
        let frontier = start_frontier_chunk.read_full_frontier(&self).map_err(|e| {
            // We can't read a frontier if it names agents or sequence numbers we haven't seen
            // before. If this happens, its because we're trying to load a data set from the future.
            if let InvalidRemoteID(_) = e {
                DataMissing
            } else { e }
        })?;

        // Usually the version data will be strictly separated. Either we're loading data into an
        // empty document, or we've been sent catchup data from a remote peer. If the data set
        // overlaps, we need to actively filter out operations & txns from that data set.
        let data_overlapping = !frontier_eq(&frontier, &self.frontier);

        if data_overlapping { todo!("Overlapping data sets not implemented!"); }

        let first_new_time = self.len();

        let mut agent_names_chunk = reader.expect_chunk(Chunk::AgentNames)?;

        // Map from agent IDs in the file (idx) to agent IDs in self, and the seq cursors.
        //
        // This will quite often just be 0,1,2,3,4...
        let mut file_to_self_agent_map = Vec::new();

        while !agent_names_chunk.0.is_empty() {
            let name = agent_names_chunk.next_str()?;
            let id = self.get_or_create_agent_id(name);
            file_to_self_agent_map.push((id, 0));
        }

        let mut agent_assignment_chunk = reader.expect_chunk(Chunk::AgentAssignment)?;

        // The file we're loading has a list of operations. The list's item order is shared in a
        // handful of lists of data - agent assignment, operations, content and txns.

        let mut next_time = first_new_time;
        while let Some(crdt_span) = agent_assignment_chunk.read_next_agent_assignment(&mut file_to_self_agent_map)? {
            // dbg!(crdt_span);
            if crdt_span.agent as usize >= self.client_data.len() {
                return Err(ParseError::InvalidLength);
            }

            // TODO: When I enable partial loads, this will need to filter operations.
            // let span = TimeSpan { start: next_time, end: next_time + crdt_span.len };
            self.assign_next_time_to_crdt_span(next_time, crdt_span);
            // next_time = span.end;
            next_time += crdt_span.len();
        }

        // We'll count the lengths in each section to make sure they all match up with each other.
        let final_agent_assignment_len = next_time;

        // *** Content and Patches ***

        // Here there's a few options based on how the encoder was configured. We'll either
        // get a Content chunk followed by PositionalPatches or just PositionalPatches.

        let mut ins_content = None;
        let mut del_content = None;

        let patches_chunk = loop {
            let (chunk_type, chunk) = reader.next_chunk()?;
            match chunk_type {
                Chunk::InsertedContent => {
                    ins_content = Some(std::str::from_utf8(chunk.0)
                        .map_err(InvalidUTF8)?);
                    // let decompressed_len = content_chunk.next_usize()?;
                    // let decompressed_data = lz4_flex::decompress(content_chunk.0, decompressed_len).unwrap();
                    // let content = String::from_utf8(decompressed_data).unwrap();
                    //     // .map_err(InvalidUTF8)?;
                }
                Chunk::DeletedContent => {
                    del_content = Some(std::str::from_utf8(chunk.0)
                        .map_err(InvalidUTF8)?);
                }
                Chunk::PositionalPatches => {
                    break chunk;
                }
                _ => { panic!("Unexpected chunk {}", chunk_type as u32); }
            }
        };

        let mut next_time = first_new_time;
        for op in ReadPatchesIter::new(patches_chunk) {
            let op = op?;
            let len = op.len();
            let content = switch(op.tag, &mut ins_content, &mut del_content);

            // TODO: Check this split point is valid.
            let content_here = content.as_mut()
                .map(|content| consume_chars(content, len));

            // self.operations.push(KVPair(next_time, op));
            self.push_op_internal(next_time, op.span, op.tag, content_here);
            next_time += len;
        }

        let final_patches_len = next_time;
        if final_agent_assignment_len != final_patches_len { return Err(InvalidLength); }

        if let Some(content) = ins_content {
            if !content.is_empty() {
                return Err(InvalidContent);
            }
        }

        // *** History ***
        let mut history_chunk = reader.expect_chunk(Chunk::TimeDAG)?;

        let mut next_time = first_new_time;
        while !history_chunk.is_empty() {
            let len = history_chunk.next_usize()?;
            // println!("len {}", len);

            let mut parents = Frontier::new();
            // And read parents.
            loop {
                let mut n = history_chunk.next_usize()?;
                let is_foreign = strip_bit_usize2(&mut n);
                let has_more = strip_bit_usize2(&mut n);

                let parent = if is_foreign {
                    if n == 0 {
                        ROOT_TIME
                    } else {
                        let agent = file_to_self_agent_map[n - 1].0;
                        let seq = history_chunk.next_usize()?;
                        if let Some(c) = self.client_data.get(agent as usize) {
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

            // Bleh its gross passing a &[Time] into here when we have a Frontier already.
            let span: TimeSpan = (next_time..next_time + len).into();

            // println!("{}-{} parents {:?}", span.start, span.end, parents);

            self.insert_history(&parents, span);
            self.advance_frontier(&parents, span);

            next_time += len;
        }

        let final_history_len = next_time;
        if final_history_len != final_patches_len { return Err(InvalidLength); }

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
            store_inserted_content: true,
            store_deleted_content: true,
            verbose: false
        });

        let oplog2 = OpLog::load_from(&data).unwrap();

        dbg!(oplog, &oplog2);

        assert_eq!(oplog, &oplog2);
    }

    #[test]
    fn encode_decode_smoke_test() {
        let doc = simple_doc();
        let data = doc.ops.encode(EncodeOptions::default());

        let result = OpLog::load_from(&data).unwrap();

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
        dbg!(&doc.ops, &d2);
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
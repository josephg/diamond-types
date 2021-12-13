use std::str::Utf8Error;
use crate::list::encoding::*;
use crate::list::encoding::varint::*;
use crate::list::{Frontier, OpLog};
use crate::list::remote_ids::{ConversionError, RemoteId};

#[derive(Debug)]
struct BufReader<'a>(&'a [u8]);

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ParseError {
    InvalidMagic,
    InvalidChunkHeader,
    UnexpectedChunk {
        // I could use Chunk here, but I'd rather not expose them publicly.
        // expected: Chunk,
        // actual: Chunk,
        expected: u32,
        actual: u32,
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

use ParseError::*;
use crate::list::frontier::{advance_frontier_by_known_run, frontier_is_root, frontier_is_sorted};
use crate::list::history::HistoryEntry;
use crate::list::operation::{InsDelTag, Operation};
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::localtime::TimeSpan;
use crate::remotespan::{CRDTId, CRDTSpan};
use crate::rle::KVPair;
use crate::ROOT_TIME;
use crate::unicount::consume_chars;

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

    fn len(&self) -> usize {
        self.0.len()
    }

    fn consume(&mut self, num: usize) {
        self.0 = unsafe { self.0.get_unchecked(num..) };
    }

    fn read_magic(&mut self) -> Result<(), ParseError> {
        self.check_has_bytes(8)?;
        if &self.0[..MAGIC_BYTES_SMALL.len()] != MAGIC_BYTES_SMALL {
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
        std::str::from_utf8(bytes).map_err(|e| InvalidUTF8(e))
    }

    fn next_run_u32(&mut self) -> Result<Option<Run<u32>>, ParseError> {
        if self.0.is_empty() { return Ok(None); }

        let mut n = self.next_u32()?;
        let (val, has_len) = strip_bit_u32(n);

        let len = if has_len {
            self.next_usize()?
        } else {
            1
        };
        Ok(Some(Run { val, len }))
    }

    // fn next_u32_diff_run<const INC: bool>(&mut self, last: &mut u32) -> Option<Run<u32>> {
    //     let (diff, has_len) = num_decode_i64_with_extra_bit(self.next()?);
    //     *last = last.wrapping_add(diff as i32 as u32);
    //     let base_val = *last;
    //     let len = if has_len {
    //         self.next().unwrap()
    //     } else {
    //         1
    //     };
    //     // println!("LO order {} len {}", last, len);
    //     if INC {
    //         // This is kinda gross. Why -1?
    //         *last = last.wrapping_add(len as u32 - 1);
    //     }
    //     Some(Run {
    //         val: base_val,
    //         len: len as usize
    //     })
    // }

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
            }).map_err(|err| InvalidRemoteID(err))?;

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

#[derive(Debug)]
struct ReadPatchesIter<'a> {
    buf: BufReader<'a>,
    last_cursor_pos: usize,
}

impl<'a> ReadPatchesIter<'a> {
    fn next_internal(&mut self) -> Result<Operation, ParseError> {
        let mut n = self.buf.next_usize()?;
        // This is in the opposite order from write_op.
        let has_length = strip_bit_usize2(&mut n);
        let diff_not_zero = strip_bit_usize2(&mut n);
        let tag = if strip_bit_usize2(&mut n) { Del } else { Ins };

        let (len, diff, fwd) = if has_length {
            // n encodes len.
            let fwd = if tag == Del {
                strip_bit_usize2(&mut n)
            } else { false };

            let diff = if diff_not_zero {
                self.buf.next_zigzag_isize()?
            } else { 0 };

            (n, diff, fwd)
        } else {
            // n encodes diff.
            let diff = num_decode_zigzag_isize(n);
            (1, diff, false)
        };

        // dbg!(self.last_cursor_pos, diff);
        let pos = isize::wrapping_add(self.last_cursor_pos as isize, diff) as usize;
        // dbg!(pos);
        self.last_cursor_pos = if tag == Ins && fwd {
            pos + len
        } else {
            pos
        };

        Ok(Operation {
            pos,
            len,
            fwd,
            content_known: false,
            tag,
            content: Default::default()
        })
    }
}

impl<'a> Iterator for ReadPatchesIter<'a> {
    type Item = Result<Operation, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { None }
        else { Some(self.next_internal()) }
    }
}


impl OpLog {
    pub fn load_from(data: &[u8]) -> Result<Self, ParseError> {
        // Written to be symmetric with encode_operations_naively().
        let mut result = Self::new();

        let mut reader = BufReader(data);
        reader.read_magic()?;

        let _info = reader.expect_chunk(Chunk::FileInfo)?;

        let mut start_frontier_chunk = reader.expect_chunk(Chunk::StartFrontier)?;
        let frontier = start_frontier_chunk.read_full_frontier(&result)?;

        // The start header chunk should always be ROOT.
        if !frontier_is_root(&frontier) { return Err(DataMissing); }

        // This isn't read anyway.
        // let mut end_frontier_chunk = reader.expect_chunk(Chunk::EndFrontier)?;
        // Interestingly we can't read the end_frontier_chunk until we've parsed all the operations.

        let mut agent_names_chunk = reader.expect_chunk(Chunk::AgentNames)?;
        while !agent_names_chunk.0.is_empty() {
            let name = agent_names_chunk.next_str()?;
            result.get_or_create_agent_id(name);
        }

        let mut agent_assignment_chunk = reader.expect_chunk(Chunk::AgentAssignment)?;

        let mut next_time = 0;
        while let Some(run) = agent_assignment_chunk.next_run_u32()? {
            if run.val as usize >= result.client_data.len() {
                return Err(ParseError::InvalidLength);
            }

            let span = TimeSpan { start: next_time, end: next_time + run.len };
            result.assign_next_time_to_client(run.val, span);
            next_time = span.end;
        }

        // *** Content and Patches ***

        // Here there's a few options based on how the encoder was configured. We'll either
        // get a Content chunk followed by PositionalPatches or just PositionalPatches.

        let next_chunk = reader.next_chunk()?;
        let (mut ins_content, patches_chunk) = if next_chunk.0 == Chunk::InsertedContent {
            let patches_chunk = reader.expect_chunk(Chunk::PositionalPatches)?;
            let content = std::str::from_utf8(next_chunk.1.0)
                .map_err(|e| InvalidUTF8(e))?;
            (Some(content), patches_chunk)
        } else {
            (None, next_chunk.1)
        };

        let mut patches_iter = ReadPatchesIter {
            buf: patches_chunk,
            last_cursor_pos: 0
        };
        let mut next_time = 0;
        for op in patches_iter {
            let mut op = op?;
            let len = op.len;
            if op.tag == Ins {
                if let Some(content) = ins_content.as_mut() {
                    // TODO: Check this split point is valid.
                    op.content = consume_chars(content, len).into();
                    op.content_known = true;
                }
            }
            result.operations.push(KVPair(next_time, op));
            next_time += len;
        }

        if let Some(content) = ins_content {
            if !content.is_empty() {
                return Err(InvalidContent);
            }
        }

        // *** History ***
        let mut history_chunk = reader.expect_chunk(Chunk::TimeDAG)?;

        let mut next_time = 0usize;
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
                        let agent = n - 1;
                        let seq = history_chunk.next_usize()?;
                        if let Some(c) = result.client_data.get(agent) {
                            c.try_seq_to_time(seq)
                                .ok_or(InvalidLength)?
                        } else {
                            return Err(InvalidLength);
                        }
                    }
                } else {
                    next_time - n
                };

                parents.push(parent);
                if !has_more { break; }
            }

            // Bleh its gross passing a &[Time] into here when we have a Frontier already.
            let span: TimeSpan = (next_time..next_time + len).into();

            // println!("{}-{} parents {:?}", span.start, span.end, parents);

            result.insert_history(&parents, span);
            result.advance_frontier(&parents, span);

            next_time += len;
        }

        // result.frontier = end_frontier_chunk.read_full_frontier(&result)?;

        Ok(result)
    }
}


#[cfg(test)]
mod tests {
    use crate::list::{ListCRDT, OpLog};
    use super::*;

    #[test]
    fn encode_decode_smoke_test() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");
        doc.local_delete(0, 3, 4); // 'hi e'
        doc.local_insert(0, 3, "m");

        let data = doc.ops.encode(EncodeOptions::default());

        let result = OpLog::load_from(&data).unwrap();

        // assert_eq!(&result, &doc.ops);
        dbg!(&result);
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
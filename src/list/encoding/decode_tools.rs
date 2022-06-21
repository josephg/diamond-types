use std::mem::size_of;
use crate::encoding::parseerror::ParseError;
use crate::list::encoding::{ChunkType, DataType, MAGIC_BYTES};
use crate::encoding::varint::*;

#[derive(Debug, Clone)]
pub struct BufReader<'a>(pub(super) &'a [u8]);

impl<'a> BufReader<'a> {
    // fn check_has_bytes(&self, num: usize) {
    //     assert!(self.0.len() >= num);
    // }

    #[inline]
    pub(super) fn check_not_empty(&self) -> Result<(), ParseError> {
        self.check_has_bytes(1)
    }

    #[inline]
    pub(super) fn check_has_bytes(&self, num: usize) -> Result<(), ParseError> {
        if self.0.len() < num { Err(ParseError::UnexpectedEOF) } else { Ok(()) }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn expect_empty(&self) -> Result<(), ParseError> {
        if self.is_empty() { Ok(()) } else { Err(ParseError::InvalidLength) }
    }

    #[allow(unused)]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn consume(&mut self, num: usize) {
        self.0 = unsafe { self.0.get_unchecked(num..) };
    }

    pub(super) fn read_magic(&mut self) -> Result<(), ParseError> {
        self.check_has_bytes(8)?;
        if self.0[..MAGIC_BYTES.len()] != MAGIC_BYTES {
            return Err(ParseError::InvalidMagic);
        }
        self.consume(8);
        Ok(())
    }

    pub(super) fn peek_u32(&self) -> Result<Option<u32>, ParseError> {
        if self.is_empty() { return Ok(None); }
        // Some(decode_u32(self.0))
        Ok(Some(decode_u32(self.0)?.0))
    }

    pub(super) fn next_u32(&mut self) -> Result<u32, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u32(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(super) fn next_u32_le(&mut self) -> Result<u32, ParseError> {
        // self.check_has_bytes(size_of::<u32>())?;
        let val = u32::from_le_bytes(self.0[0..4].try_into().map_err(|_| ParseError::UnexpectedEOF)?);
        self.consume(size_of::<u32>());
        Ok(val)
    }

    #[allow(unused)]
    pub(super) fn next_u64(&mut self) -> Result<u64, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_u64(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(super) fn next_usize(&mut self) -> Result<usize, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_usize(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(super) fn next_zigzag_isize(&mut self) -> Result<isize, ParseError> {
        let n = self.next_usize()?;
        Ok(num_decode_zigzag_isize(n))
    }

    pub(super) fn next_n_bytes(&mut self, num_bytes: usize) -> Result<&'a [u8], ParseError> {
        if num_bytes > self.0.len() { return Err(ParseError::UnexpectedEOF); }

        let (data, remainder) = self.0.split_at(num_bytes);
        self.0 = remainder;
        Ok(data)
    }

    // fn split(self, num_bytes: usize) -> Result<(Self, Self), ParseError> {
    //     if num_bytes > self.0.len() { return Err(UnexpectedEOF); }
    //
    //     let (a, b) = self.0.split_at(num_bytes);
    //     Ok((BufReader(a), BufReader(b)))
    // }

    // fn peek_u32(&self) -> Result<u32, ParseError> {
    //     self.check_not_empty()?;
    //     Ok(decode_u32(self.0).0)
    // }
    //
    // fn peek_chunk_type(&self) -> Result<Chunk, ParseError> {
    //     Ok(Chunk::try_from(self.peek_u32()?).map_err(|_| InvalidChunkHeader)?)
    // }

    #[inline]
    pub(super) fn chunks(self) -> ChunkReader<'a> {
        ChunkReader(self)
    }

    // Note the result is attached to the lifetime 'a, not the lifetime of self.
    pub(super) fn next_str(&mut self) -> Result<&'a str, ParseError> {
        if self.0.is_empty() { return Err(ParseError::UnexpectedEOF); }

        let len = self.next_usize()?;
        if len > self.0.len() { return Err(ParseError::InvalidLength); }

        let bytes = self.next_n_bytes(len)?;
        // std::str::from_utf8(bytes).map_err(InvalidUTF8)
        std::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUTF8)
    }

    /// Read the next string thats encoded in this content chunk
    pub(super) fn into_content_str(mut self) -> Result<&'a str, ParseError> {
        // dbg!(&self.0);
        let data_type = self.next_u32()?;
        if data_type != (DataType::PlainText as u32) {
            return Err(ParseError::UnknownChunk);
        }
        // let len = self.next_usize()?;
        // if len > self.0.len() {
        //     return Err(InvalidLength);
        // }
        std::str::from_utf8(self.0).map_err(|_| ParseError::InvalidUTF8)
    }

    pub fn dbg_print_chunk_tree_internal(mut self) -> Result<(), ParseError> {
        println!("Total file size {}", self.len());
        let total_len = self.len();
        println!("magic at {}", total_len - self.len());
        self.read_magic()?;
        let protocol_version = self.next_usize()?;
        println!("Protocol version {protocol_version}");

        let mut chunks = self.chunks();
        loop { // gross
            let position = total_len - chunks.0.len();
            if let Ok((chunk, inner_reader)) = chunks.next_chunk() {
                println!("Chunk {:?} at {} ({} bytes)", chunk, position, inner_reader.len());

                let inner_len = inner_reader.len();
                if chunk == ChunkType::FileInfo || chunk == ChunkType::StartBranch || chunk == ChunkType::Patches {
                    let mut inner_chunks = inner_reader.chunks();
                    loop {
                        let inner_position = position + inner_len - inner_chunks.0.len();
                        if let Ok((chunk, inner_inner_reader)) = inner_chunks.next_chunk() {
                            println!("  Chunk {:?} at {} ({} bytes)", chunk, inner_position, inner_inner_reader.len());
                        } else { break; }
                    }
                }
            } else { break; }
        }
        Ok(())
    }

    pub fn dbg_print_chunk_tree(self) {
        if let Err(e) = self.dbg_print_chunk_tree_internal() {
            eprintln!("-> Error parsing ({:?})", e);
        }
    }
}


/// A ChunkReader is a wrapper around some bytes which just contain a series of chunks.
#[derive(Debug, Clone)]
pub(super) struct ChunkReader<'a>(pub BufReader<'a>);

impl<'a> Iterator for ChunkReader<'a> {
    type Item = Result<(ChunkType, BufReader<'a>), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.next_chunk())
        }
    }
}

impl<'a> ChunkReader<'a> {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn expect_empty(&self) -> Result<(), ParseError> {
        self.0.expect_empty()
    }

    fn next_chunk_raw(&mut self) -> Result<(ChunkType, BufReader<'a>), ParseError> {
        let chunk_type = ChunkType::try_from(self.0.next_u32()?)
            .map_err(|_| ParseError::UnknownChunk);

        // This in no way guarantees we're good.
        let len = self.0.next_usize()?;
        if len > self.0.len() {
            return Err(ParseError::InvalidLength);
        }

        let reader = BufReader(self.0.next_n_bytes(len)?);

        // Note we're try-ing chunk_type here so we still read all the bytes if we can, even if
        // the chunk type is unknown.
        Ok((chunk_type?, reader))
    }

    /// Read the next chunk, skipping unknown chunks for forwards compatibility.
    pub(super) fn next_chunk(&mut self) -> Result<(ChunkType, BufReader<'a>), ParseError> {
        loop {
            let c = self.next_chunk_raw();
            match c {
                Err(ParseError::UnknownChunk) => {}, // Keep scanning.
                _ => { return c; }
            }
        }
    }

    /// Read a chunk with the named type. Returns None if the next chunk isn't the specified type,
    /// or we hit EOF.
    pub(super) fn read_chunk_if_eq(&mut self, expect_chunk_type: ChunkType) -> Result<Option<BufReader<'a>>, ParseError> {
        if let Some(actual_chunk_type) = self.0.peek_u32()? {
            if actual_chunk_type != (expect_chunk_type as u32) {
                // Chunk doesn't match requested type.
                return Ok(None);
            }
            self.next_chunk().map(|(_type, c)| Some(c))
        } else {
            // EOF.
            Ok(None)
        }
    }

    #[inline]
    pub(super) fn expect_chunk_pred<P>(&mut self, pred: P, err_type: ChunkType) -> Result<(ChunkType, BufReader<'a>), ParseError>
        where P: FnOnce(ChunkType) -> bool
    {
        let (actual_chunk_type, r) = self.next_chunk()?;

        if pred(actual_chunk_type) {
            // dbg!(expect_chunk_type, actual_chunk_type);
            Ok((actual_chunk_type, r))
        } else {
            Err(ParseError::MissingChunk(err_type as _))
        }
    }

    pub(super) fn expect_chunk(&mut self, expect_chunk_type: ChunkType) -> Result<BufReader<'a>, ParseError> {
        self.expect_chunk_pred(|c| c == expect_chunk_type, expect_chunk_type)
            .map(|(_c, r)| r)
    }
}
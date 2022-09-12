use crate::encoding::bufparser::BufParser;
use crate::encoding::ChunkType;
use crate::encoding::parseerror::ParseError;

/// A ChunkReader is a wrapper around some bytes which just contain a series of chunks.
#[derive(Debug, Clone)]
pub(crate) struct ChunkReader<'a>(pub BufParser<'a>);

impl<'a> Iterator for ChunkReader<'a> {
    type Item = Result<(ChunkType, BufParser<'a>), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.next_chunk())
        }
    }
}

impl<'a> ChunkReader<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn expect_empty(&self) -> Result<(), ParseError> {
        self.0.expect_empty()
    }

    fn next_chunk_raw(&mut self) -> Result<(ChunkType, BufParser<'a>), ParseError> {
        let chunk_type = ChunkType::try_from(self.0.next_u32()?)
            .map_err(|_| ParseError::UnknownChunk);

        // This in no way guarantees we're good.
        let len = self.0.next_usize()?;
        if len > self.0.len() {
            return Err(ParseError::InvalidLength);
        }

        let reader = BufParser(self.0.next_n_bytes(len)?);

        // Note we're try-ing chunk_type here so we still read all the bytes if we can, even if
        // the chunk type is unknown.
        Ok((chunk_type?, reader))
    }

    /// Read the next chunk, skipping unknown chunks for forwards compatibility.
    pub fn next_chunk(&mut self) -> Result<(ChunkType, BufParser<'a>), ParseError> {
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
    pub fn read_chunk_if_eq(&mut self, expect_chunk_type: ChunkType) -> Result<Option<BufParser<'a>>, ParseError> {
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
    pub fn expect_chunk_pred<P>(&mut self, pred: P, err_type: ChunkType) -> Result<(ChunkType, BufParser<'a>), ParseError>
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

    pub fn expect_chunk(&mut self, expect_chunk_type: ChunkType) -> Result<BufParser<'a>, ParseError> {
        self.expect_chunk_pred(|c| c == expect_chunk_type, expect_chunk_type)
            .map(|(_c, r)| r)
    }
}
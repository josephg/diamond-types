use std::mem::size_of;
use crate::encoding::parseerror::ParseError;
use crate::encoding::varint::*;
use crate::list::encoding::leb::{decode_leb_u32, decode_leb_u64, decode_leb_usize};

#[derive(Debug, Clone)]
pub struct BufParser<'a>(pub(crate) &'a [u8]);

impl<'a> BufParser<'a> {
    #[inline]
    pub(crate) fn check_not_empty(&self) -> Result<(), ParseError> {
        self.check_has_bytes(1)
    }

    #[inline]
    pub(crate) fn check_has_bytes(&self, num: usize) -> Result<(), ParseError> {
        if self.0.len() < num { Err(ParseError::UnexpectedEOF) } else { Ok(()) }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn expect_empty(&self) -> Result<(), ParseError> {
        if self.is_empty() { Ok(()) } else { Err(ParseError::InvalidLength) }
    }

    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn consume(&mut self, num: usize) {
        self.0 = unsafe { self.0.get_unchecked(num..) };
    }

    // pub(crate) fn read_magic(&mut self) -> Result<(), ParseError> {
    //     self.check_has_bytes(8)?;
    //     if self.0[..MAGIC_BYTES.len()] != MAGIC_BYTES {
    //         return Err(ParseError::InvalidMagic);
    //     }
    //     self.consume(8);
    //     Ok(())
    // }

    pub(crate) fn peek_u32(&self) -> Result<Option<u32>, ParseError> {
        if self.is_empty() { return Ok(None); }
        // Some(decode_u32(self.0))
        Ok(Some(decode_leb_u32(self.0)?.0))
    }

    pub(crate) fn next_u32(&mut self) -> Result<u32, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_leb_u32(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(crate) fn next_u32_le(&mut self) -> Result<u32, ParseError> {
        // self.check_has_bytes(size_of::<u32>())?;
        let val = u32::from_le_bytes(self.0[0..4].try_into().map_err(|_| ParseError::UnexpectedEOF)?);
        self.consume(size_of::<u32>());
        Ok(val)
    }

    #[allow(unused)]
    pub(crate) fn next_u64(&mut self) -> Result<u64, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_leb_u64(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(crate) fn next_usize(&mut self) -> Result<usize, ParseError> {
        self.check_not_empty()?;
        let (val, count) = decode_leb_usize(self.0)?;
        self.consume(count);
        Ok(val)
    }

    pub(crate) fn next_zigzag_isize(&mut self) -> Result<isize, ParseError> {
        let n = self.next_usize()?;
        Ok(num_decode_zigzag_isize(n))
    }

    pub(crate) fn next_n_bytes(&mut self, num_bytes: usize) -> Result<&'a [u8], ParseError> {
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

    // #[inline]
    // pub(crate) fn chunks(self) -> ChunkReader<'a> {
    //     ChunkReader(self)
    // }

    // Note the result is attached to the lifetime 'a, not the lifetime of self.
    pub(crate) fn next_str(&mut self) -> Result<&'a str, ParseError> {
        if self.0.is_empty() { return Err(ParseError::UnexpectedEOF); }

        let len = self.next_usize()?;
        if len > self.0.len() { return Err(ParseError::InvalidLength); }

        let bytes = self.next_n_bytes(len)?;
        // std::str::from_utf8(bytes).map_err(InvalidUTF8)
        std::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUTF8)
    }

    // /// Read the next string thats encoded in this content chunk
    // pub(crate) fn into_content_str(mut self) -> Result<&'a str, ParseError> {
    //     // dbg!(&self.0);
    //     let data_type = self.next_u32()?;
    //     if data_type != (DataType::PlainText as u32) {
    //         return Err(ParseError::UnknownChunk);
    //     }
    //     // let len = self.next_usize()?;
    //     // if len > self.0.len() {
    //     //     return Err(InvalidLength);
    //     // }
    //     std::str::from_utf8(self.0).map_err(|_| ParseError::InvalidUTF8)
    // }
}
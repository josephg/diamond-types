use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::list::remote_ids::ConversionError;


// #[derive(Debug)]
// pub enum ParseError {
//     GenericInvalidBytes,
//     InvalidLength,
//     UnexpectedEOF,
//     InvalidVarInt,
// }

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize), serde(crate="serde_crate"))]
pub enum ParseError {
    InvalidMagic,
    UnsupportedProtocolVersion,
    DocIdMismatch,
    BaseVersionUnknown,
    UnknownChunk,
    LZ4DecoderNeeded,
    LZ4DecompressionError, // I'd wrap it but lz4_flex errors don't implement any traits
    // LZ4DecompressionError(lz4_flex::block::DecompressError),
    CompressedDataMissing,
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
    // InvalidUTF8(Utf8Error),
    InvalidUTF8,
    InvalidRemoteID(ConversionError),
    InvalidVarInt,
    InvalidContent,

    ChecksumFailed,

    /// This error is interesting. We're loading a chunk but missing some of the data. In the future
    /// I'd like to explicitly support this case, and allow the oplog to contain a somewhat- sparse
    /// set of data, and load more as needed.
    DataMissing,
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseError {:?}", self)
    }
}

impl Error for ParseError {}

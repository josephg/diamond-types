//! Storage engine. See [`README.md`] for more details.
//!
//!

use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;

use std::path::Path;
use page_writer::PageWriter;
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::{calc_checksum, ExtendFromSlice};
use crate::storage::page_writer::page_checksum_offset;

mod page_writer;

const SE_MAGIC_BYTES: [u8; 8] = *b"DT_STOR1";
const SE_VERSION: u32 = 1; // 2 bytes would probably be fine for this but eh.
// const SE_VERSION_BYTES: [u8; 2] = SE_VERSION.to_le_bytes();

// 4k block size. Any other size is currently not supported, though we'll still store the block size
// in the file.
const DEFAULT_PAGE_SIZE: usize = 4096;

// const MIN_PAGE_SIZE: usize = 512;

#[derive(Debug)]
#[non_exhaustive]
pub enum HeaderError {
    InvalidMagicBytes,
    VersionTooNew(u32),
    InvalidPageSize(usize),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum SEError {
    // InvalidHeader,
    // UnexpectedEOF,
    // ChecksumMismatch,

    // InvalidBlit,
    // InvalidData,

    PageTooLarge,
    InvalidChecksum,

    GenericInvalidData,

    HeaderError(HeaderError),
    ParseError(ParseError),
    IO(io::Error),
}


impl From<io::Error> for SEError {
    fn from(io_err: io::Error) -> Self {
        // If we get an EOF while reading, we should deal with that immediately.

        SEError::IO(io_err)
        // if io_err.kind() == ErrorKind::UnexpectedEof { SEError::UnexpectedEOF }
        // else { SEError::IO(io_err) }
    }
}
impl From<ParseError> for SEError {
    fn from(pe: ParseError) -> Self {
        SEError::ParseError(pe)
    }
}
impl From<HeaderError> for SEError {
    fn from(inner: HeaderError) -> Self {
        SEError::HeaderError(inner)
    }
}

#[derive(Debug)]
struct StorageEngine {
    file: File,

    header_fields: StorageHeaderFields,
}

#[derive(Debug, Clone)]
#[repr(u16)]
enum StoragePageType {
    AgentNames = 1,
    CGInfo = 2,
    // etc.

    ChunkMax,
}
const NUM_STORAGE_CHUNK_TYPES: usize = 3;

type PageNum = u32;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ChunkInfo {
    blit_page: PageNum,
    first_page: PageNum,

    // cur_block: BlockNum,
}

#[derive(Debug)]
struct StorageHeaderFields {
    file_format_version: u32,
    page_size: usize,
    chunks: [Option<ChunkInfo>; NUM_STORAGE_CHUNK_TYPES] // The slot is the chunk type.
}

impl Default for StorageHeaderFields {
    fn default() -> Self {
        Self {
            file_format_version: SE_VERSION,
            page_size: DEFAULT_PAGE_SIZE,
            chunks: [None; NUM_STORAGE_CHUNK_TYPES],
        }
    }
}

struct OwnedPage {
    data: [u8; DEFAULT_PAGE_SIZE],
    start: usize,
    end: usize
}

fn read_page(file: &mut File, page_no: PageNum, expect_header: bool) -> Result<OwnedPage, SEError> {
    let mut buffer = [0u8; DEFAULT_PAGE_SIZE];

    #[cfg(target_os = "linux")]
    file.read_exact_at(&mut buffer, page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;
    #[cfg(not(target_os = "linux"))] {
        file.seek(SeekFrom::Start(page_no as u64 * DEFAULT_PAGE_SIZE as u64))?;
        file.read_exact(&mut buffer)?;
    }

    let checksum_start = if expect_header {
        if buffer[0..SE_MAGIC_BYTES.len()] != SE_MAGIC_BYTES {
            return Err(HeaderError::InvalidMagicBytes.into());
        }

        SE_MAGIC_BYTES.len()
    } else { 0 };

    debug_assert_eq!(checksum_start, page_checksum_offset(expect_header));

    let mut checksum_bytes = [0u8; 4];
    checksum_bytes.copy_from_slice(&buffer[checksum_start..checksum_start + 4]);
    let expected_checksum = u32::from_le_bytes(checksum_bytes);

    let len_start = checksum_start + 4;

    let mut len_bytes = [0u8; 2];
    len_bytes.copy_from_slice(&buffer[len_start..len_start + 2]);
    let len = u16::from_le_bytes(len_bytes) as usize;

    let data_start = len_start + 2;

    if data_start + len > DEFAULT_PAGE_SIZE {
        return Err(SEError::PageTooLarge);
    }

    let actual_checksum = calc_checksum(&buffer[len_start..data_start+len]);
    if expected_checksum != actual_checksum {
        return Err(SEError::InvalidChecksum);
    }

    Ok(OwnedPage {
        data: buffer,
        start: data_start,
        end: data_start + len,
    })
}

fn read_header_page(file: &mut File, page_no: PageNum) -> Result<StorageHeaderFields, SEError> {
    let page = read_page(file, page_no, true)?;

    let mut parser = BufParser(&page.data[page.start..page.end]);
    let file_format_version = parser.next_u32()?;
    if file_format_version != SE_VERSION {
        return Err(HeaderError::VersionTooNew(file_format_version).into());
    }

    let req_page_size = parser.next_usize()?;
    if req_page_size != DEFAULT_PAGE_SIZE {
        return Err(HeaderError::InvalidPageSize(req_page_size).into());
    }

    let mut header_fields = StorageHeaderFields {
        file_format_version,
        page_size: req_page_size,
        chunks: [None; NUM_STORAGE_CHUNK_TYPES],
    };

    loop {
        let chunk_type_or_end = parser.next_usize()?;
        if chunk_type_or_end == 0 { break; }
        let chunk_type = chunk_type_or_end - 1;
        let first_page = parser.next_u32()?;
        let blit_page = parser.next_u32()?;

        // TODO: Is it worth checking that the pages are valid?
        if first_page == blit_page { return Err(SEError::GenericInvalidData); }

        if chunk_type < header_fields.chunks.len() {
            header_fields.chunks[chunk_type] = Some(ChunkInfo {
                blit_page,
                first_page,
            });
        } else {
            eprintln!("Warning: Ignoring unknown chunk type in file: {chunk_type}");
        }
    }

    Ok(header_fields)
}

impl StorageEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SEError> {
        let mut file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(false)
            .open(path.as_ref())?;

        let total_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let header_fields = Self::read_or_initialize_header(&mut file, total_len)?;

        Ok(Self {
            file,
            header_fields,
        })
    }

    fn encode_header(header_fields: &StorageHeaderFields) -> Result<PageWriter, SEError> {
        assert_eq!(header_fields.page_size, DEFAULT_PAGE_SIZE, "Other block sizes are not yet implemented");
        let mut page = PageWriter::new_header();

        page.write_u32(header_fields.file_format_version)?;
        page.write_usize(header_fields.page_size)?;

        for (i, c) in header_fields.chunks
            .iter()
            .enumerate()
            .filter_map(|(i, data)|
                data.map(|c| (i, c))
            )
        {
            page.write_usize(i + 1)?;
            page.write_u32(c.first_page)?;
            page.write_u32(c.blit_page)?;
        }
        page.write_usize(0)?;

        Ok(page)
    }

    // fn write_header(mut file: &mut File, current_len: u64) -> Result<(), SEError> {
    //
    // }

    /// Read the header block - which is the start of the file. Returns the block size.
    fn read_or_initialize_header(file: &mut File, total_len: u64) -> Result<StorageHeaderFields, SEError> {
        if total_len == 0 {
            println!("Initializing headers");
            // Presumably a new file. Initialize it using the default options.
            let header_fields = StorageHeaderFields::default();
            Self::encode_header(&header_fields)?
                .finish_and_write(file, 0)?;
            Ok(header_fields)
        } else {
            println!("Parsing fields");
            // Parse the header page.
            let header_fields = read_header_page(file, 0)?;
            // TODO: If the header page has an invalid checksum, we should now search the file for
            // the backup header page and load that instead.
            Ok(header_fields)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::storage::StorageEngine;

    #[test]
    fn foo() {
        let se = StorageEngine::open("foo.dts").unwrap();
        dbg!(se);
    }
}

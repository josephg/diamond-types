//! Storage engine. See [`README.md`] for more details.
//!
//!

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};

#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;

use std::path::Path;
use smallvec::{smallvec, SmallVec};
use page_writer::PageWriter;
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::{calc_checksum, ExtendFromSlice};
use crate::encoding::varint::{decode_prefix_varint_u32, decode_prefix_varint_usize};
use crate::storage::page_writer::{page_checksum_offset, write_page};

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

    UnexpectedPageType,

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
    next_free_page: PageNum,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u16)]
enum PageType {
    Header = 0,
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

    // The slot (array index) is the chunk type. We can't actually read any chunk types beyond
    // the ones this code knows about, but when we write a new copy of the file header, we'll
    // preserve any chunk info blocks that are here that we don't recognise.
    chunks: SmallVec<[Option<ChunkInfo>; NUM_STORAGE_CHUNK_TYPES]> // The slot is the chunk type.
}

impl Default for StorageHeaderFields {
    fn default() -> Self {
        Self {
            file_format_version: SE_VERSION,
            page_size: DEFAULT_PAGE_SIZE,
            chunks: smallvec![],
        }
    }
}

impl StorageHeaderFields {
    fn each_chunk_info(&self) -> impl Iterator<Item = (u32, ChunkInfo)> + '_ {
        self.chunks
            .iter()
            .enumerate()
            .filter_map(|(i, data)|
                data.map(|c| (i as u32, c))
            )
    }

    // fn assign_chunk_info(&mut self, kind: u32, info: ChunkInfo) {
    //     if chunks.len() < chunk_type {
    //         chunks.resize(chunk_type, None);
    //     }
    //
    //     chunks[chunk_type] = Some(ChunkInfo {
    //         blit_page,
    //         first_page,
    //     });
    // }
}

struct OwnedPage {
    data: [u8; DEFAULT_PAGE_SIZE],
    start: usize,
    end: usize
}

impl OwnedPage {
    fn get_next_page_no(&self) -> PageNum {
        // This is a pretty round about way to write this code, but the optimizer makes it trivial.
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.data[NEXT_PAGE_BYTE_OFFSET..NEXT_PAGE_BYTE_OFFSET+4]);
        u32::from_le_bytes(buf)
    }
}

// struct DataPage {
//     page: OwnedPage, // starting at the first byte of data.
//     next_page_no: PageNum,
//     prev_page_no: PageNum,
// }

const NEXT_PAGE_BYTE_OFFSET: usize = 4 + 2; // checksum then length.

// fn read_page(file: &mut File, page_no: PageNum, expect_type: u32) -> Result<OwnedPage, SEError> {
fn read_page_raw(file: &mut File, page_no: PageNum, is_header: bool) -> Result<OwnedPage, SEError> {
    let mut buffer = [0u8; DEFAULT_PAGE_SIZE];

    #[cfg(target_os = "linux")]
    file.read_exact_at(&mut buffer, page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;
    #[cfg(not(target_os = "linux"))] {
        file.seek(SeekFrom::Start(page_no as u64 * DEFAULT_PAGE_SIZE as u64))?;
        file.read_exact(&mut buffer)?;
    }

    let checksum_start = if is_header {
        if buffer[0..SE_MAGIC_BYTES.len()] != SE_MAGIC_BYTES {
            return Err(HeaderError::InvalidMagicBytes.into());
        }

        SE_MAGIC_BYTES.len()
    } else { 0 };

    debug_assert_eq!(checksum_start, page_checksum_offset(is_header));

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

fn read_data_page(file: &mut File, page_no: PageNum, expect_type: u32) -> Result<OwnedPage, SEError> {
    debug_assert_ne!(expect_type, PageType::Header as u32);

    let mut page = read_page_raw(file, page_no, false)?;

    // Data pages start with the next page number (u32_le). This is filled in with 0, and filled in
    // when the data page is allocated.

    page.start += 4;

    // Then the page type. Check that it matches and consume it.
    let (actual_type, bytes_consumed) = decode_prefix_varint_u32(&page.data[page.start..])?;
    page.start += bytes_consumed;
    if actual_type != expect_type {
        return Err(SEError::UnexpectedPageType);
    }

    Ok(page)
}

fn read_header_page(file: &mut File, page_no: PageNum) -> Result<StorageHeaderFields, SEError> {
    let page = read_page_raw(file, page_no, true)?;

    let mut parser = BufParser(&page.data[page.start..page.end]);
    let file_format_version = parser.next_u32()?;
    if file_format_version != SE_VERSION {
        return Err(HeaderError::VersionTooNew(file_format_version).into());
    }

    let req_page_size = parser.next_usize()?;
    if req_page_size != DEFAULT_PAGE_SIZE {
        return Err(HeaderError::InvalidPageSize(req_page_size).into());
    }

    let mut chunks = smallvec![None; NUM_STORAGE_CHUNK_TYPES];
    loop {
        let chunk_type_or_end = parser.next_usize()?;
        if chunk_type_or_end == 0 { break; }
        let chunk_type = chunk_type_or_end - 1;
        let first_page = parser.next_u32()?;
        let blit_page = parser.next_u32()?;

        // TODO: Is it worth checking that the pages are valid?
        if first_page == blit_page { return Err(SEError::GenericInvalidData); }

        if chunks.len() < chunk_type {
            chunks.resize(chunk_type, None);
        }

        chunks[chunk_type] = Some(ChunkInfo {
            blit_page,
            first_page,
        });
    }

    Ok(StorageHeaderFields {
        file_format_version,
        page_size: req_page_size,
        chunks,
    })
}

fn scan_for_next_free_block(file: &mut File, header_fields: &StorageHeaderFields) -> Result<PageNum, SEError> {
    // Ok, now we need to find the next free page.
    // For now, the file should always be "packed" - that is, there can't be any holes in
    // the file. I might be able to get away with scanning from the back, but this is
    // cleaner.
    //
    // For now I'll just scan the whole file, making sure it doesn't have any holes.
    // Any allocated / assigned (but unused) blocks are skipped. Blits are skipped. And
    // items which are valid and have a page number assigned are considered to have
    // allocated that block, even if nothing is written at that block number.

    // I'm not super happy about making this queue have multiple fields.
    #[derive(PartialEq, Eq)]
    struct Item(PageNum, u32, bool); // Page number, page type, blit flag.

    impl Ord for Item {
        fn cmp(&self, other: &Self) -> Ordering {
            // Just compare based on page number. .reverse() to make it a min-heap.
            self.0.cmp(&other.0).reverse()
        }
    }

    impl PartialOrd for Item {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut queue = BinaryHeap::<Item>::new();
    for (kind, info) in header_fields.each_chunk_info() {
        queue.push(Item(info.first_page, kind, false));
        queue.push(Item(info.blit_page, kind, true));
    }

    dbg!(header_fields);
    let mut next_page = 1;
    while let Some(Item(page_no, kind, is_blit)) = queue.pop() {
        dbg!((page_no, kind, is_blit, next_page));
        if page_no != next_page {
            panic!("Ermagherd bad");
            // return Err(SEError::GenericInvalidData);
        }

        next_page = page_no + 1;

        if is_blit { continue; } // We don't care about blits.

        let page = match read_data_page(file, page_no, kind) {
            Ok(page) => page,
            Err(SEError::InvalidChecksum) => { continue; } // Ignore this.
            Err(SEError::IO(io_err)) => {
                // We'll get an UnexpectedEof error if we hit the end of the file. Its
                // possible the next block is assigned by the previous block, but not
                // actually allocated on disk yet.
                if io_err.kind() == ErrorKind::UnexpectedEof { continue; }
                else { return Err(SEError::IO(io_err)); }
            },
            Err(e) => { return Err(e); }
        };

        let next_page = page.get_next_page_no();
        if next_page != 0 {
            queue.push(Item(next_page, kind, false));
        }
    }

    Ok(next_page)
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

        let (header_fields, next_free_page) = Self::read_or_initialize_header(&mut file, total_len)?;

        Ok(Self {
            file,
            header_fields,
            next_free_page,
        })
    }

    fn encode_header(header_fields: &StorageHeaderFields) -> Result<PageWriter, SEError> {
        assert_eq!(header_fields.page_size, DEFAULT_PAGE_SIZE, "Other block sizes are not yet implemented");
        let mut page = PageWriter::new_header();

        page.write_u32(header_fields.file_format_version)?;
        page.write_usize(header_fields.page_size)?;

        for (kind, c) in header_fields.each_chunk_info() {
            page.write_u32(kind + 1)?;
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
    fn read_or_initialize_header(file: &mut File, total_len: u64) -> Result<(StorageHeaderFields, PageNum), SEError> {
        if total_len == 0 {
            println!("Initializing headers");
            // Presumably a new file. Initialize it using the default options.
            let header_fields = StorageHeaderFields::default();
            Self::encode_header(&header_fields)?
                .finish_and_write(file, 0)?;

            // Could probably get away with this flush here, but its basically free and it makes me
            // feel better.
            file.sync_all()?;
            Ok((header_fields, 1))
        } else {
            println!("Parsing fields");
            // Parse the header page.
            let header_fields = read_header_page(file, 0)?;
            // TODO: If the header page has an invalid checksum, we should now search the file for
            // the backup header page and load that instead.

            let free_page = scan_for_next_free_block(file, &header_fields)?;

            Ok((header_fields, free_page))
        }
    }

    fn assign_next_page(&mut self) -> PageNum {
        let page = self.next_free_page;
        self.next_free_page += 1;
        page
    }

    fn make_data(&mut self, kind: PageType) -> Result<(), SEError> {
        let kind_usize = kind as usize;
        if let Some(Some(t)) = self.header_fields.chunks.get(kind_usize) {
            dbg!(t);
        } else {
            // Assign new pages for it.
            let blit_page = self.assign_next_page();
            let first_page = self.assign_next_page();
            dbg!((blit_page, first_page));

            let chunks = &mut self.header_fields.chunks;
            if chunks.len() < kind_usize {
                chunks.resize(kind_usize, None);
            }

            chunks[kind_usize] = Some(ChunkInfo {
                blit_page,
                first_page,
            });

            // And rewrite the header.
            let (new_head, _len) = Self::encode_header(&self.header_fields).unwrap()
                .finish();

            write_page(&mut self.file, blit_page, &new_head)?;
            self.file.sync_all()?;
            write_page(&mut self.file, 0, &new_head)?;

            // I don't think we actually need to sync again here.
            //
            // If pages are used but not assigned, the contents are ignored.
            // If pages are assigned but not used, it doesn't matter.
            // So it only matters when the content is written to the new blocks.
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::os::unix::fs::FileExt;
    use crate::storage::{PageType, StorageEngine};

    #[test]
    fn foo() {
        let mut se = StorageEngine::open("foo.dts").unwrap();

        se.make_data(PageType::AgentNames).unwrap();
        dbg!(&se);
    }

    // #[test]
    // fn bar() {
    //     let file = std::fs::File::options()
    //         .read(true)
    //         .create(true)
    //         .write(true)
    //         .append(false)
    //         .open("blah")
    //         .unwrap();
    //
    //     let mut buf = [0u8; 1000];
    //     file.read_exact_at(&mut buf, 0).unwrap();
    // }
}

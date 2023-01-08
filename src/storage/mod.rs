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
use num_enum::TryFromPrimitive;
use smallvec::{smallvec, SmallVec};
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::ExtendFromSlice;
use crate::storage::page::{DataPage, HeaderPage, Page};

mod page;

const SE_MAGIC_BYTES: [u8; 8] = *b"DT_STOR1";
const SE_VERSION: u32 = 1; // 2 bytes would probably be fine for this but eh.
// const SE_VERSION_BYTES: [u8; 2] = SE_VERSION.to_le_bytes();

// 4k block size. Any other size is currently not supported, though we'll still store the block size
// in the file.
const DEFAULT_PAGE_SIZE: usize = 4096;

// const MIN_PAGE_SIZE: usize = 512;

#[derive(Debug)]
#[non_exhaustive]
pub enum PageDataError {
    InvalidHeaderMagicBytes,
    InvalidChecksum,
    VersionTooNew(u16),
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

    UnexpectedPageType,

    GenericInvalidData,

    PageDataError(PageDataError),
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
impl From<PageDataError> for SEError {
    fn from(inner: PageDataError) -> Self {
        SEError::PageDataError(inner)
    }
}

#[derive(Debug)]
struct StorageEngine {
    file: File,

    header_fields: StorageHeaderFields,
    next_free_page: PageNum,

    data_chunks: [Option<Box<DataPage>>; NUM_DATA_CHUNK_TYPES] // The slot is the chunk type.
}

#[derive(Debug)]
struct WritePageData {
    next_page_no: PageNum,
    write_to_blit_next: bool,
    data: Box<OwnedPage>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[derive(TryFromPrimitive)]
#[repr(u16)]
enum PageType {
    Header = 0,
    Data = 1,
    Overflow = 2,
    Free = 3,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u16)]
enum DataPageType {
    AgentNames = 0,
    CGInfo = 1,
    // etc.
}

const NUM_DATA_CHUNK_TYPES: usize = 3;

type PageNum = u32;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct DataChunkHeaderInfo {
    blit_page: PageNum,
    first_page: PageNum,

    // cur_block: BlockNum,
}

#[derive(Debug)]
pub(super) struct StorageHeaderFields {
    page_size: usize,

    // The slot (array index) is the chunk type. We can't actually read any chunk types beyond
    // the ones this code knows about, but when we write a new copy of the file header, we'll
    // preserve any chunk info blocks that are here that we don't recognise.
    data_page_info: SmallVec<[Option<DataChunkHeaderInfo>; NUM_DATA_CHUNK_TYPES]> // The slot is the chunk type.
}

impl Default for StorageHeaderFields {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            data_page_info: smallvec![],
        }
    }
}

impl StorageHeaderFields {
    fn data_chunk_info_iter(&self) -> impl Iterator<Item = (u32, DataChunkHeaderInfo)> + '_ {
        self.data_page_info
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

#[derive(Debug)]
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

fn scan_blocks<F: FnMut(PageNum, u32, bool, Option<&DataPage>)>(file: &mut File, header_fields: &StorageHeaderFields, mut visit: F) -> Result<PageNum, SEError> {
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
    struct Item(PageNum, u32, bool); // Page number, data page type, blit flag.

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
    for (kind, info) in header_fields.data_chunk_info_iter() {
        queue.push(Item(info.first_page, kind, false));
        queue.push(Item(info.blit_page, kind, true));
    }

    dbg!(header_fields);
    let mut next_page = 1;
    while let Some(Item(page_no, kind, is_blit)) = queue.pop() {
        // dbg!((page_no, kind, is_blit, next_page));
        if page_no != next_page {
            panic!("Ermagherd bad");
            // return Err(SEError::GenericInvalidData);
        }

        next_page = page_no + 1;

        if is_blit {
            // We don't really care about blits, or need to read them.
            visit(page_no, kind, true, None);
        } else {
            // Read the page, looking for info on the next page information.

            let page = match DataPage::read_raw(file, page_no) {
                Ok(page) => Some(page),
                Err(SEError::PageDataError(PageDataError::InvalidChecksum)) => None, // Ignore this.
                Err(SEError::IO(io_err)) => {
                    // We'll get an UnexpectedEof error if we hit the end of the file. Its
                    // possible the next block is assigned by the previous block, but not
                    // actually allocated on disk yet.
                    if io_err.kind() == ErrorKind::UnexpectedEof { None } else { return Err(SEError::IO(io_err)); }
                },
                Err(e) => { return Err(e); }
            };

            // TODO: Maybe check the page type is correct?

            visit(page_no, kind, false, page.as_ref());

            if let Some(page) = page {
                let next_page = page.get_next_page();
                if next_page != 0 {
                    queue.push(Item(next_page, kind, false));
                }
            }
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

        // Gross!
        const HACK_NONE: Option<Box<DataPage>> = None;
        Ok(Self {
            file,
            header_fields,
            next_free_page,
            data_chunks: [HACK_NONE; NUM_DATA_CHUNK_TYPES],
        })
    }


    /// Read the header block - which is the start of the file. Returns the block size.
    fn read_or_initialize_header(file: &mut File, total_len: u64) -> Result<(StorageHeaderFields, PageNum), SEError> {
        if total_len == 0 {
            println!("Initializing headers");
            // Presumably a new file. Initialize it using the default options.
            let header_fields = StorageHeaderFields::default();
            HeaderPage::encode_and_bake(&header_fields)
                .write(file, 0)?;

            // Could probably get away with this flush here, but its basically free and it makes me
            // feel better.
            file.sync_all()?;
            Ok((header_fields, 1))
        } else {
            println!("Parsing fields");
            // Parse the header page.
            let header_fields = HeaderPage::read(file, 0)?;
            // TODO: If the header page has an invalid checksum, we should now search the file for
            // the backup header page and load that instead.

            // TODO: It would be better if I didn't have to do this, but eh.
            let free_page = scan_blocks(file, &header_fields, |page_no, kind, is_blit, page_data| {
                dbg!((page_no, kind, is_blit, page_data));
            })?;
// [Option<Box<PageData>>; NUM_STORAGE_CHUNK_TYPES]
            Ok((header_fields, free_page))
        }
    }

    fn assign_next_page(&mut self) -> PageNum {
        let page = self.next_free_page;
        self.next_free_page += 1;
        page
    }

    fn make_data(&mut self, kind: DataPageType) -> Result<(), SEError> {
        let kind_usize = kind as usize;
        if let Some(Some(t)) = self.header_fields.data_page_info.get(kind_usize) {
            dbg!(t);
        } else {
            // Assign new pages for it.
            let blit_page = self.assign_next_page();
            let first_page = self.assign_next_page();
            dbg!((blit_page, first_page));

            let chunks = &mut self.header_fields.data_page_info;
            if chunks.len() < kind_usize {
                chunks.resize(kind_usize, None);
            }

            chunks[kind_usize] = Some(DataChunkHeaderInfo {
                blit_page,
                first_page,
            });

            // And rewrite the header.
            // let (new_head, _len) = Self::encode_header(&self.header_fields).unwrap()
            //     .finish();

            let new_head = HeaderPage::encode_and_bake(&self.header_fields);

            new_head.write(&mut self.file, blit_page)?;
            self.file.sync_all()?;
            new_head.write(&mut self.file, 0)?;

            assert!(kind_usize < self.data_chunks.len());
            // Since we're adding data, the old data must have been None.
            // self.chunks[kind_usize].replace(Box::new(WritePageData {
            //     next_page_no: first_page,
            //     write_to_blit_next: false,
            //     data: Box::new(OwnedPage {
            //         data: [],
            //         start: 0,
            //         end: 0,
            //     }),
            // });

            // I don't think we actually need to sync again here.
            //
            // If pages are used but not assigned, the contents are ignored.
            // If pages are assigned but not used, it doesn't matter.
            // So it only matters when the content is written to the new blocks.
        }

        Ok(())
    }

    fn append_bytes_to(&mut self, _kind: PageType) -> Result<(), SEError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::storage::{DataPageType, StorageEngine};

    #[test]
    fn foo() {
        let mut se = StorageEngine::open("foo.dts").unwrap();

        se.make_data(DataPageType::AgentNames).unwrap();
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

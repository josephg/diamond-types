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
use crate::storage::page::{BlitStatus, DataPage, DataPageImmutableFields, HeaderPage, Page};

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
    InvalidHeaderPageSize(usize),
    PageTooLarge(u16),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum SEError {
    // InvalidHeader,
    // UnexpectedEOF,
    // ChecksumMismatch,

    // InvalidBlit,
    // InvalidData,

    PageFull,

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

    data_chunks: [Option<Box<DataPageState>>; NUM_DATA_CHUNK_TYPES] // The slot is the chunk type.
}

#[derive(Debug)]
struct DataPageState {
    next_page_no: PageNum,
    write_to_blit_next: bool,
    page: DataPage,
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
#[derive(TryFromPrimitive)]
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

const NEXT_PAGE_BYTE_OFFSET: usize = 4 + 2; // checksum then length.

// This function does a lot. I could refactor it to pass a visitor function or something, but I'm
// only using it in this one context so I think its ok.
fn scan_blocks(file: &mut File, header_fields: &StorageHeaderFields) -> Result<(PageNum, [Option<Box<DataPageState>>; NUM_DATA_CHUNK_TYPES]), SEError> {
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
    struct Item(PageNum, PageNum, u32, bool); // Page number, prev page num, data page type, blit flag.

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
        queue.push(Item(info.first_page, 0, kind, false));
        // Only adding the blit pages so we can move past them while checking that the file is
        // packed.
        queue.push(Item(info.blit_page, 0, kind, true));
    }

    dbg!(header_fields);
    const HACK_NONE: Option<Box<DataPageState>> = None;
    let mut data_chunks = [HACK_NONE; NUM_DATA_CHUNK_TYPES];
    // let mut blit_flags = [BlitStatus(u8::MAX); NUM_DATA_CHUNK_TYPES];
    // let mut blit_associated_page = [PageNum; NUM_DATA_CHUNK_TYPES];

    let mut next_page = 1;
    while let Some(Item(page_no, prev_page, kind, is_blit)) = queue.pop() {
        dbg!((page_no, kind, is_blit, next_page));
        if page_no != next_page {
            panic!("Ermagherd bad");
            // return Err(SEError::GenericInvalidData);
        }

        next_page = page_no + 1;

        // We don't need to read blits just yet. First we'll scan to the last allocated page for
        // all the types of data.
        if !is_blit {
            // Read the page, looking for info on the next page information.
            //
            // There's 3 things that can happen here:
            // 1. The page has a valid checksum but its still corrupted somehow, or we get a read
            //   error. Bail and return the error.
            // 2. The read is past the end of the file, or the checksum doesn't match. This means
            //   the page we're looking at is assigned but not valid, or the last write to this page
            //   failed. page = None.
            // 3. (Most common) The page is valid. If we can, we'll keep walking through the pages.

            fn try_read(file: &mut File, page_no: PageNum) -> Result<Option<DataPage>, SEError> {
                match DataPage::read_raw(file, page_no) {
                    Ok(page) => Ok(Some(page)),
                    Err(SEError::PageDataError(PageDataError::InvalidChecksum)) => Ok(None), // Ignore this.
                    Err(SEError::IO(io_err)) => {
                        // We'll get an UnexpectedEof error if we hit the end of the file. Its
                        // possible the next block is assigned by the previous block, but not
                        // actually allocated on disk yet.
                        if io_err.kind() == ErrorKind::UnexpectedEof { Ok(None) } else { return Err(SEError::IO(io_err)); }
                    },
                    Err(e) => { return Err(e); }
                }
            }

            let page = try_read(file, page_no)?;

            // TODO: Check the page type and page prev fields are correct

            if let Some(page) = page.as_ref() {
                let next_page = page.get_next_or_associated_page();
                if next_page != 0 {
                    // The page is valid and it has an assigned next page. Onwards!
                    queue.push(Item(next_page, page_no, kind, false));
                    continue;
                }
            }

            // We get here if its the last page in the history for this data type. The page might
            // be None if it hasn't been written to yet, or the last write failed.

            // Check the blit data at this point.
            let blit_page_no = header_fields.data_page_info[kind as usize].unwrap().blit_page;
            let mut blit_page = try_read(file, blit_page_no)?;

            // This is a bit of a hack. If the blit page is old (it is associated with an earlier
            // page) then discard it.
            if let Some(p) = blit_page.as_ref() {
                if p.get_next_or_associated_page() != page_no {
                    blit_page = None;
                }
            }

            dbg!((page.is_some(), blit_page.is_some()));
            let (blit, page_used) = match (page, blit_page) {
                (Some(page), Some(blit_page)) => {
                    // Keep the page which is "furthest along".
                    dbg!(page.get_blit_status());
                    dbg!(blit_page.get_blit_status());
                    match page.get_blit_status().partial_cmp(&blit_page.get_blit_status()) {
                        // Use the page version.
                        None => { return Err(SEError::GenericInvalidData); }
                        Some(Ordering::Greater) | Some(Ordering::Equal) => {
                            println!("page");
                            // Use the page version. If the blits are equal it doesn't matter.
                            (true, page)
                        }
                        Some(Ordering::Less) => {
                            println!("blit");
                            // Use the blit version.
                            (false, blit_page)
                        }
                    }
                }
                (None, Some(blit_page)) => {
                    (false, blit_page)
                }
                (Some(page), None) => {
                    (true, page)
                }
                (None, None) => {
                    (true, DataPage::new(DataPageImmutableFields {
                        kind: (kind as u16).try_into().unwrap(),
                        prev_page,
                    }))
                }
            };

            data_chunks[kind as usize] = Some(Box::new(DataPageState {
                next_page_no: page_no,
                write_to_blit_next: blit,
                page: page_used
            }));
        }
    }

    Ok((next_page, data_chunks))
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

        // let (header_fields, next_free_page, data_chunks) = Self::read_or_initialize_header(&mut file, total_len)?;

        // Gross!
        const HACK_NONE: Option<Box<DataPageState>> = None;

        if total_len == 0 {
            println!("Initializing headers");
            // Presumably a new file. Initialize it using the default options.
            let header_fields = StorageHeaderFields::default();
            HeaderPage::encode_and_bake(&header_fields)
                .write(&mut file, 0)?;

            // Could probably get away with this flush here, but its basically free and it makes me
            // feel better.
            file.sync_all()?;
            Ok(Self {
                file,
                header_fields,
                next_free_page: 1,
                data_chunks: [HACK_NONE; NUM_DATA_CHUNK_TYPES],
            })
        } else {
            println!("Parsing fields");
            // Parse the header page.
            let header_fields = HeaderPage::read(&mut file, 0)?;
            // TODO: If the header page has an invalid checksum, we should now search the file for
            // the backup header page and load that instead.

            // TODO: It would be better if I didn't have to do this, but eh.
            // let last_page_for_type
            // let data_chunks = [HACK_NONE; NUM_DATA_CHUNK_TYPES];

            let (next_free_page, data_chunks) = scan_blocks(&mut file, &header_fields)?;

            Ok(Self {
                file,
                header_fields,
                next_free_page,
                data_chunks,
            })
        }
    }

    fn assign_next_page(&mut self) -> PageNum {
        let page = self.next_free_page;
        self.next_free_page += 1;
        page
    }

    fn make_data(&mut self, kind: DataPageType) -> Result<(), SEError> {
        let kind_usize = kind as usize;
        if self.data_chunks[kind_usize].is_none() {
            // Assign new pages for it.
            let blit_page = self.assign_next_page();
            let first_page = self.assign_next_page();
            dbg!((blit_page, first_page));

            let chunks = &mut self.header_fields.data_page_info;
            if chunks.len() <= kind_usize {
                chunks.resize(kind_usize + 1, None);
            }

            chunks[kind_usize] = Some(DataChunkHeaderInfo {
                blit_page,
                first_page,
            });

            // And rewrite the header.
            // let (new_head, _len) = Self::encode_header(&self.header_fields).unwrap()
            //     .finish();

            let new_head = HeaderPage::encode_and_bake(&self.header_fields);

            println!("Writing new header {:?}", &self.header_fields);
            new_head.write(&mut self.file, blit_page)?;
            self.file.sync_all()?;
            new_head.write(&mut self.file, 0)?;

            assert!(kind_usize < self.data_chunks.len());

            self.data_chunks[kind_usize] = Some(Box::new(DataPageState {
                next_page_no: first_page,
                write_to_blit_next: false,
                page: DataPage::new(DataPageImmutableFields {
                    kind,
                    prev_page: 0,
                }),
            }));

            // I don't think we actually need to sync again here.
            //
            // If pages are used but not assigned, the contents are ignored.
            // If pages are assigned but not used, it doesn't matter.
            // So it only matters when the content is written to the new blocks.
        }

        Ok(())
    }

    fn append_bytes_to(&mut self, kind: DataPageType) -> Result<(), SEError> {
        let kind_usize = kind as usize;
        if self.data_chunks[kind_usize].is_none() {
            self.make_data(kind)?;
        }

        let state = self.data_chunks[kind_usize].as_mut().unwrap();
        state.page.push_usize(100 * 128 + 55)?;

        // TODO: There is a bug here: If this write fails, the in-memory state is corrupt.
        state.page.roll_blit_status();
        if state.write_to_blit_next {
            state.page.set_next_page(state.next_page_no);

            let blit_page = self.header_fields.data_page_info[kind_usize]
                .as_ref().unwrap().blit_page;
            state.page.bake_and_write(&mut self.file, blit_page)?;
            state.write_to_blit_next = false;
            println!("Wrote blit page {blit_page}");
        } else {
            state.page.set_next_page(0); // Unassigned.
            state.page.bake_and_write(&mut self.file, state.next_page_no)?;
            state.write_to_blit_next = true;
            println!("Wrote normal page {}", state.next_page_no);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::storage::{DataPageType, StorageEngine};

    #[test]
    fn foo() {
        let mut se = StorageEngine::open("foo.dts").unwrap();

        // se.make_data(DataPageType::AgentNames).unwrap();
        se.append_bytes_to(DataPageType::AgentNames).unwrap();
        se.append_bytes_to(DataPageType::AgentNames).unwrap();
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

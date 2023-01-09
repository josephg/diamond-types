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
pub enum CorruptPageError {
    InvalidHeaderMagicBytes,
    InvalidChecksum,
    VersionTooNew(u16),
    InvalidHeaderPageSize(usize),
    PageLengthInvalid(u16),
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

    PageIsCorrupt(CorruptPageError),
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
impl From<CorruptPageError> for SEError {
    fn from(inner: CorruptPageError) -> Self {
        SEError::PageIsCorrupt(inner)
    }
}

const NUM_DATA_CHUNK_TYPES: usize = 3;
type PageNum = u32;

#[derive(Debug)]
struct StorageEngine {
    file: File,

    header_dirty: bool,
    header_fields: StorageHeaderFields,
    next_free_page: PageNum,

    // Using a Box<> here because the inlined data pages are 4kb each. Could just box the entire
    // array or something instead? Eh.
    data_chunks: [Option<Box<DataPageState>>; NUM_DATA_CHUNK_TYPES] // The slot is the chunk type.
}

#[derive(Debug)]
pub(super) struct StorageHeaderFields {
    page_size: usize,

    // The slot (array index) is the chunk type. We can't actually read any chunk types beyond
    // the ones this code knows about, but when we write a new copy of the file header, we'll
    // preserve any chunk info blocks that are here that we don't recognise.
    data_page_info: SmallVec<[Option<DataChunkHeaderInfo>; NUM_DATA_CHUNK_TYPES]> // The slot is the chunk type.
}

// For each chunk type we store two structs - one in the header fields and one in the storage engine
// data. This is so if there's new data types we don't know about, we leave their header fields
// alone.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct DataChunkHeaderInfo {
    blit_page: PageNum,
    first_page: PageNum,

    // cur_block: BlockNum,
}

#[derive(Debug)]
struct DataPageState {
    current_page_no: PageNum,
    write_to_blit_next: bool,
    blit_page: PageNum, // Copied from header info.
    page: DataPage,
    dirty: bool,
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
                    Err(SEError::PageIsCorrupt(e)) => {
                        eprintln!("Page is corrupt. This is probably fine? {:?}", e);
                        Ok(None)
                    }, // Ignore this.
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
                current_page_no: page_no,
                write_to_blit_next: blit,
                blit_page: blit_page_no,
                page: page_used,
                dirty: false,
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

            // TODO: Consider just leaving header_dirty=true here and not writing the inital header.
            HeaderPage::encode_and_bake(&header_fields)
                .write(&mut file, 0)?;

            // Could probably get away with this flush here, but its basically free and it makes me
            // feel better.
            file.sync_all()?;
            Ok(Self {
                file,
                header_dirty: false,
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
                header_dirty: false,
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

    // This method could return a &mut DataPageState but I can't really use it because of the borrow
    // check rules. (The field needs to be a partial borrow of &self)
    fn prepare_data_page_type(&mut self, kind: DataPageType) -> (&File, &mut PageNum, &mut DataPageState) {
        let kind_usize = kind as usize;

        assert!(kind_usize < self.data_chunks.len());
        let state = self.data_chunks[kind_usize].get_or_insert_with(|| {
            // Assign new pages for it.
            println!("Assigning new pages {}", self.next_free_page);
            // not using assign_next_page because of borrowck.
            let blit_page = self.next_free_page;
            let first_page = self.next_free_page + 1;
            self.next_free_page += 2;
            dbg!((blit_page, first_page));

            let chunks = &mut self.header_fields.data_page_info;
            if chunks.len() <= kind_usize {
                chunks.resize(kind_usize + 1, None);
            }

            chunks[kind_usize] = Some(DataChunkHeaderInfo {
                blit_page,
                first_page,
            });

            self.header_dirty = true;

            // I don't think we actually need to sync again here.
            //
            // If pages are used but not assigned, the contents are ignored.
            // If pages are assigned but not used, it doesn't matter.
            // So it only matters when the content is written to the new blocks.

            Box::new(DataPageState {
                current_page_no: first_page,
                write_to_blit_next: false,
                blit_page,
                page: DataPage::new(DataPageImmutableFields {
                    kind,
                    prev_page: 0,
                }),
                dirty: false,
            })
        });

        (&self.file, &mut self.next_free_page, state)
    }

    fn append_bytes_to(&mut self, kind: DataPageType) -> Result<(), SEError> {
        let (file, next_free_page, state) = self.prepare_data_page_type(kind);

        state.dirty = true;
        match state.page.push_usize(100 * 128 + 55) {
            Ok(()) => {},
            Err(SEError::PageFull) => {
                // If the page is full, finish out the page and assign a new one. We need to write
                // the new page to register the new page ID.
                let new_page = *next_free_page;
                *next_free_page += 1;

                println!("Assigning new page {}", new_page);

                let is_blit = Self::write_page(file, state, new_page)?;

                if is_blit {
                    // fsync here to make sure we don't partially overwrite the current state
                    // before the blit page has been written.
                    file.sync_data()?;
                    Self::write_page(file, state, new_page)?;
                }

                // Might be an easier way to wipe this.
                state.page = DataPage::new(DataPageImmutableFields {
                    kind,
                    prev_page: state.current_page_no,
                });
                state.current_page_no = new_page;

                state.page.push_usize(100 * 128 + 55)?;
            }
            Err(e) => { return Err(e); }
        }

        Ok(())
    }

    /// returns true if the page written was a blit page.
    fn write_page(file: &File, state: &mut DataPageState, next_page: PageNum) -> Result::<bool, SEError> {
        // TODO: This code assumes that if this write fails, then no further writes will happen.
        state.dirty = false;
        state.page.roll_blit_status();
        if state.write_to_blit_next {
            state.page.set_next_page(state.current_page_no);
            state.page.bake_and_write(&file, state.blit_page)?;
            state.write_to_blit_next = false;
            println!("Wrote blit page {}", state.blit_page);
            Ok(true)
        } else {
            state.page.set_next_page(next_page); // Unassigned.
            state.page.bake_and_write(&file, state.current_page_no)?;
            state.write_to_blit_next = true;
            println!("Wrote normal page {}", state.current_page_no);
            Ok(false)
        }
    }

    pub fn fsync(&mut self) -> Result<(), SEError> {
        let mut sync_needed = false;

        if self.header_dirty {
            let new_head = HeaderPage::encode_and_bake(&self.header_fields);

            println!("Writing new header {:?} to page {}", &self.header_fields, self.next_free_page);
            new_head.write(&mut self.file, self.next_free_page)?;
            // We need a sync here in case the writes are reordered, and the write to page 0 is
            // only partially completed and the write to next_free_page doesn't happen at all.
            self.file.sync_data()?;
            new_head.write(&mut self.file, 0)?;

            sync_needed = true;
        }

        // for (kind_usize, state) in self.data_chunks.iter_mut()
        //     .enumerate()
        //     .filter_map(|(kind, chunk)| {
        //         chunk.as_mut().map(|c| (kind, c))
        //     })
        //     .filter(|(_, chunk)| chunk.dirty)
        for state in self.data_chunks.iter_mut()
            .flatten()
            .filter(|chunk| chunk.dirty)
            .map(|s| s.as_mut()) // Not strictly needed, but kinda cleaner.
        {
            Self::write_page(&self.file, state, 0)?;
            sync_needed = true;
        }

        if sync_needed {
            self.file.sync_data()?;
        }

        Ok(())
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.fsync().unwrap();
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
        se.fsync().unwrap();
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

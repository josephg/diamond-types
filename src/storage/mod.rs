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
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use smallvec::{smallvec, SmallVec};
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::ExtendFromSlice;
use crate::storage::file::{DTFile, DTFilesystem, OsFilesystem};
use crate::storage::page::{BlitStatus, DataPage, DataPageImmutableFields, HeaderPage, Page};

mod page;
mod file;

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

impl<E: TryFromPrimitive> From<TryFromPrimitiveError<E>> for SEError {
    fn from(_value: TryFromPrimitiveError<E>) -> Self {
        // TODO: Something better here.
        SEError::GenericInvalidData
    }
}

const NUM_DATA_CHUNK_TYPES: usize = 3;
type PageNum = u32;

#[derive(Debug)]
struct StorageEngine<F: DTFile> {
    file: F,

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
fn scan_blocks<F: DTFile>(file: &mut F, header_fields: &StorageHeaderFields) -> Result<(PageNum, [Option<Box<DataPageState>>; NUM_DATA_CHUNK_TYPES]), SEError> {
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
        dbg!((page_no, kind, is_blit));
        if page_no != next_page {
            panic!("Ermagherd bad {page_no} {next_page}");
            // return Err(SEError::GenericInvalidData);
        }

        next_page = page_no + 1;

        // We don't need to read blits just yet. First we'll scan to the last allocated page for
        // all the types of data.
        if !is_blit {
            let page = DataPage::try_read_raw(file, page_no)?;

            // TODO: Check the page type and page prev fields are correct

            if let Some(page) = page.as_ref() {
                let next_page = page.get_next_or_associated_page();
                println!("Next page {next_page}");
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
            let mut blit_page = DataPage::try_read_raw(file, blit_page_no)?;

            // This is a bit of a hack. If the blit page is old (it is associated with an earlier
            // page) then discard it.
            if let Some(p) = blit_page.as_ref() {
                if p.get_next_or_associated_page() != page_no {
                    blit_page = None;
                }
            }

            dbg!((page.is_some(), blit_page.is_some()));
            let (write_to_blit_next, page_used) = match (page, blit_page) {
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
                write_to_blit_next,
                blit_page: blit_page_no,
                page: page_used,
                dirty: false,
            }));
        }
    }

    Ok((next_page, data_chunks))
}

impl<F: DTFile> StorageEngine<F> {
    pub fn open<FS: DTFilesystem<File=F>, P: AsRef<Path>>(path: P, filesystem: &mut FS) -> Result<Self, SEError> {
        let mut file = filesystem.open(path.as_ref())?;

        let total_len = file.stream_len()?;

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
    fn prepare_data_page_type(&mut self, kind: DataPageType) -> (&mut F, &mut PageNum, &mut DataPageState) {
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

        (&mut self.file, &mut self.next_free_page, state)
    }

    fn append_bytes_to(&mut self, kind: DataPageType, num: usize) -> Result<(), SEError> {
        let (file, next_free_page, state) = self.prepare_data_page_type(kind);

        state.dirty = true;
        match state.page.push_usize(num) {
            Ok(()) => {},
            Err(SEError::PageFull) => {
                // If the page is full, finish out the page and assign a new one. We need to write
                // the new page to register the new page ID.
                let new_page = *next_free_page;
                *next_free_page += 1;

                println!("Page full! Assigning new page {}", new_page);

                let is_blit = Self::write_page(file, state, new_page)?;

                if is_blit {
                    println!("Writing back to the page");
                    // fsync here to make sure we don't partially overwrite the current state
                    // before the blit page has been written.
                    file.write_barrier()?;
                    Self::write_page(file, state, new_page)?;
                }

                // Might be an easier way to wipe this.
                state.current_page_no = new_page;
                state.write_to_blit_next = false;
                state.page = DataPage::new(DataPageImmutableFields {
                    kind,
                    prev_page: state.current_page_no,
                });

                state.page.push_usize(num)?;
            }
            Err(e) => { return Err(e); }
        }

        Ok(())
    }

    /// returns true if the page written was a blit page.
    fn write_page(file: &mut F, state: &mut DataPageState, next_page: PageNum) -> Result::<bool, SEError> {
        // TODO: This code assumes that if this write fails, then no further writes will happen.
        state.dirty = false;
        state.page.roll_blit_status();
        if state.write_to_blit_next {
            state.page.set_next_page(state.current_page_no);
            state.page.bake_and_write(file, state.blit_page)?;
            state.write_to_blit_next = false;
            // println!("Wrote blit page {} (next {})", state.blit_page, state.current_page_no);
            Ok(true)
        } else {
            state.page.set_next_page(next_page); // Unassigned.
            state.page.bake_and_write(file, state.current_page_no)?;
            state.write_to_blit_next = true;
            // println!("Wrote normal page {} (next {})", state.current_page_no, next_page);
            Ok(false)
        }
    }

    pub fn fsync(&mut self) -> Result<(), SEError> {
        let mut sync_needed = false;

        if self.header_dirty {
            let new_head = HeaderPage::encode_and_bake(&self.header_fields);

            println!("Writing new header {:?} to page {}", &self.header_fields, self.next_free_page);
            new_head.write(&mut self.file, self.next_free_page)?;
            // We need a barrier here in case the writes are reordered, and the write to page 0 is
            // only partially completed and the write to next_free_page doesn't happen at all.
            self.file.write_barrier()?;
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
            Self::write_page(&mut self.file, state, 0)?;
            sync_needed = true;
        }

        if sync_needed {
            self.file.sync_data()?;
        }

        Ok(())
    }

    fn get_data_header_info(&self, kind: DataPageType) -> Option<DataChunkHeaderInfo> {
        let kind_usize = kind as usize;
        if kind_usize >= self.header_fields.data_page_info.len() { None }
        else {
            self.header_fields.data_page_info[kind_usize]
        }
    }

    // TODO: I wish this didn't need to be &mut.
    fn iter_data_pages(&mut self, kind: DataPageType) -> DataChunkIterator<F> {
        // assert!(!self.header_dirty);
        // assert!(!self.data_chunks.iter().flatten().any(|d| d.dirty));

        if let Some(info) = self.get_data_header_info(kind) {
            DataChunkIterator {
                file: &mut self.file,
                next_page: info.first_page,
                blit_page: info.blit_page,
                current_page: self.data_chunks[kind as usize].as_deref(),
            }
        } else {
            // We don't have any chunks of this type. The easiest answer is to just return a "dud"
            // iterator which will immediately return None.
            DataChunkIterator {
                file: &mut self.file,
                next_page: 0,
                blit_page: 0,
                current_page: None,
            }
        }
    }
}

impl<F: DTFile> Drop for StorageEngine<F> {
    fn drop(&mut self) {
        self.fsync().unwrap();
    }
}

struct DataChunkIterator<'a, F> {
    // kind: DataPageType,
    file: &'a mut F,
    next_page: PageNum,
    blit_page: PageNum,
    // The current page may not have been flushed to disk yet. We'll take a reference to it here and
    // just return this data directly.
    current_page: Option<&'a DataPageState>,
}

impl<'a, F: DTFile> Iterator for DataChunkIterator<'a, F> {
    type Item = Result<DataPage, SEError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_page == 0 { return None; }

        if let Some(current_page) = self.current_page {
            if current_page.current_page_no == self.next_page {
                self.next_page = 0;
                // We'll yield the current page to the consumer.
                //
                // I'd really like to avoid this memcpy, but I don't think thats practical here
                // because of ownership. (I guess I could return a Cow-style wrapper struct).
                //
                // Also note when the returned page is read, we'll update the start cursor position.
                // ... so this makes it quite practical to read the page like this.
                println!("Returning current");
                let mut page = current_page.page.clone();
                // The page should already have its read position set to the correct place...
                page.reset_read_pos();
                return Some(Ok(page));
            }
        }

        // If we get a real read error, pass it up.
        let page = match DataPage::try_read_raw(self.file, self.next_page) {
            Ok(p) => { p }
            Err(e) => {
                self.next_page = 0;
                return Some(Err(e));
            }
        };

        let this_page_no = self.next_page;
        if let Some(page) = page {
            // page.type
            let next_page = page.get_next_or_associated_page();

            self.next_page = next_page;

            // TODO: Consider removing blit page logic.
            if next_page == 0 {
                // This is the last page. We need to read the blit page to check if its newer.
                match DataPage::try_read_raw(self.file, self.blit_page) {
                    Ok(Some(b)) => {
                        if b.get_next_or_associated_page() == this_page_no && b.get_blit_status() > page.get_blit_status() {
                            // Use the blit instead.
                            return Some(Ok(b));
                        }
                    }
                    Ok(None) => {} // Fall through below.
                    Err(e) => {
                        self.next_page = 0;
                        return Some(Err(e));
                    }
                }
            }

            Some(Ok(page))
        } else {
            // We get here if the current page is corrupted or has not yet been written. We'll check
            // if the blit is valid (and current). If so we can return it, but probably we're done.

            self.next_page = 0; // This is the last page read regardless.

            match DataPage::try_read_raw(self.file, self.blit_page) {
                Ok(Some(b)) if b.get_next_or_associated_page() == this_page_no => {
                    // Use the blit.
                    Some(Ok(b))
                }
                Ok(_) => None, // No valid page here at all. We're done.
                Err(e) => Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::storage::{DataPageType, StorageEngine};
    use crate::storage::file::OsFilesystem;
    use crate::storage::file::test::TestFilesystem;

    #[test]
    fn one() {
        let mut se = StorageEngine::open("foo.dts", &mut TestFilesystem).unwrap();

        for i in 0..4000 {
            se.append_bytes_to(DataPageType::AgentNames, i).unwrap();
            // se.fsync().unwrap();
        }

        se.fsync().unwrap();

        for page in se.iter_data_pages(DataPageType::AgentNames) {
            let mut page = page.unwrap();
            dbg!(page.read_fields().unwrap());
            dbg!(page.get_content().len());
        }

        // se.make_data(DataPageType::AgentNames).unwrap();
        // se.append_bytes_to(DataPageType::AgentNames).unwrap();
        // se.fsync().unwrap();
        // se.append_bytes_to(DataPageType::AgentNames).unwrap();
        dbg!(&se);
    }

    #[test]
    fn two() {
        let mut se = StorageEngine::open("foo.dts", &mut TestFilesystem).unwrap();

        for page in se.iter_data_pages(DataPageType::AgentNames) {
            let mut page = page.unwrap();
            dbg!(page.read_fields().unwrap());
            dbg!(page.get_content().len());
        }

        dbg!(&se.data_chunks, &se.header_fields, &se.next_free_page);
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

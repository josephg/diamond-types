//! All data in the storage engine is stored in pages.
//!
//! The code to read, write and edit pages is a bit complex because it involves a lot of explicit
//! offsets and things. This module encapsulates that code.

use std::fmt;
use std::ops::Range;
use std::fs::File;
use smallvec::smallvec;
use std::os::unix::fs::FileExt;
use crate::encoding::bufparser::BufParser;
use crate::encoding::tools::{calc_checksum, ExtendFromSlice};
use crate::encoding::varint::{decode_prefix_varint_u32, push_u32, push_u64, push_usize};
use crate::storage::*;


/// Pages have 3 kinds of data:
///
/// 1. Fixed position fields. These are fields stored at fixed offsets in the page. This allows the
///    data contained to be mutable. They must be set before the page is written to disk. They all
///    use fixed byte sizes.
/// 2. Immutable fields. These fields are set when the page is created / read and never modified.
/// 3. Content bytes. This is the "meat and potatoes" of a data page, with the actual content.
///
/// I could store (buffer) mutable pages in separate fields in this struct, but I'd need to keep
/// those fields in sync with the data stored in the byte array. That sounds sketchy to me. So the
/// mutable fields are read / written via accessor methods directly into the data chunk.
///
/// # Fixed position fields
///
/// For header pages:
/// - Magic bytes
/// - File format
/// - Checksum
/// - Page length
///
///
/// For data pages:
///
/// - Checksum
/// - Page length: This stores the index of the next free byte in the page.
/// - Next page: u32 pointer to the next allocated page. 0 if not yet allocated.
///   Blit info: A pointer to the associated page.
/// - Blit status
///
/// The checksum is a LE CRC32 checksum of all bytes after the checksum field in the page.
///
///
/// # Immutable fields
///
/// Immutable fields are stored using varint encoding, and packed after the mutable fields.
///
/// For data pages:
///
/// - Page type
/// - Pointer to the previous page (or 0 if none)
/// - Cursor data
#[derive(Clone)]
pub(super) struct Page<const T: usize> {
    // *** Mutable fields ***

    data: [u8; DEFAULT_PAGE_SIZE],
    // cursor_start_pos: usize,
    content_start_pos: usize,
    content_end_pos: usize,
}

pub(super) const PAGE_TYPE_HEADER: usize = PageType::Header as usize;
pub(super) const PAGE_TYPE_DATA: usize = PageType::Data as usize;
pub(super) const PAGE_TYPE_OVERFLOW: usize = PageType::Overflow as usize;

pub(super) type HeaderPage = Page<PAGE_TYPE_HEADER>;
pub(super) type DataPage = Page<PAGE_TYPE_DATA>;
pub(super) type OverflowPage = Page<PAGE_TYPE_OVERFLOW>;

impl<const T: usize> fmt::Debug for Page<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DataPage")
            .field("(page type)", &PageType::try_from(T as u16).unwrap())
            // .field("cursor_start_pos", &self.cursor_start_pos)
            .field("content_start_pos", &self.content_start_pos)
            .field("content_end_pos", &self.content_end_pos)
            // .field("data", &&self.data[0..self.content_end_pos])
            .finish()
    }
}

// Mutable page fields are at fixed offsets.
const PO_DATA_CHECKSUM: Range<usize> = 0..4; // 4 bytes (u32)
const PO_DATA_LEN: Range<usize> = 4..6;
const PO_DATA_NEXT_PAGE: Range<usize> = 6..10; // 4 bytes (u32)
const PO_DATA_BLIT_STATUS: Range<usize> = 10..11; // 1 byte (u8)
// 1 byte reserved for flags and future use.

const PO_DATA_IMMUTABLE_FIELD_START: usize = 12;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct DataPageImmutableFields {
    pub(super) kind: DataPageType,
    pub(super) prev_page: PageNum,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) struct BlitStatus(pub u8);

impl PartialOrd for BlitStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO: There's almost certainly better ways to write this code.
        if self.0 == other.0 { Some(Ordering::Equal) }
        else if self.0 == (other.0 + 1) % 3 { Some(Ordering::Greater) }
        else if other.0 == (self.0 + 1) % 3 { Some(Ordering::Less) }
        else { None }
    }
}

// *** Header pages ***

const MAGIC_BYTES: [u8; 8] = *b"DT_STOR1";
const FORMAT_VERSION: u16 = 0; // 2 bytes would probably be fine for this but eh.

// Mutable page fields are at fixed offsets.
const PO_HEADER_MAGIC: Range<usize> = 0..8;
const PO_HEADER_CHECKSUM: Range<usize> = 8..12; // 4 bytes (u32)
const PO_HEADER_FORMAT_VERSION: Range<usize> = 12..14; // 2 bytes (u16)
const PO_HEADER_LEN: Range<usize> = 14..16;
// 4 bytes reserved for future use.

const PO_HEADER_START: usize = 20;



impl<const T: usize> ExtendFromSlice for Page<T> {
    type Result = Result<(), SEError>;

    fn extend_from_slice(&mut self, slice: &[u8]) -> Result<(), SEError> {
        if self.content_end_pos + slice.len() > self.data.len() {
            return Err(SEError::PageFull);
        }
        self.data[self.content_end_pos..self.content_end_pos + slice.len()].copy_from_slice(slice);
        self.content_end_pos += slice.len();
        Ok(())
    }
}

struct InfallibleWritePage<'a, const T: usize>(&'a mut Page<T>);
impl<'a, const T: usize> ExtendFromSlice for InfallibleWritePage<'a, T> {
    type Result = ();

    fn extend_from_slice(&mut self, slice: &[u8]) {
        assert!(self.0.content_end_pos + slice.len() <= self.0.data.len());
        self.0.data[self.0.content_end_pos..self.0.content_end_pos + slice.len()].copy_from_slice(slice);
        self.0.content_end_pos += slice.len();
    }
}


impl<const T: usize> Page<T> {
    fn checksum_offset() -> Range<usize> {
        match T {
            PAGE_TYPE_HEADER => PO_HEADER_CHECKSUM.clone(),
            PAGE_TYPE_DATA => PO_DATA_CHECKSUM.clone(),
            _ => unimplemented!(),
        }
    }

    fn len_offset() -> Range<usize> {
        match T {
            PAGE_TYPE_HEADER => PO_HEADER_LEN.clone(),
            PAGE_TYPE_DATA => PO_DATA_LEN.clone(),
            _ => unimplemented!(),
        }
    }

    fn immutable_data_start_offset() -> usize {
        match T {
            PAGE_TYPE_HEADER => PO_HEADER_START,
            PAGE_TYPE_DATA => PO_DATA_IMMUTABLE_FIELD_START,
            _ => unimplemented!(),
        }
    }

    fn read_checksum(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.data[Self::checksum_offset()]);
        u32::from_le_bytes(buf)
    }

    fn calc_checksum(&self) -> u32 {
        let len = self.get_len();
        assert!(len <= self.data.len());

        calc_checksum(&self.data[Self::checksum_offset().end..len])
    }

    fn set_checksum(&mut self, checksum: u32) {
        self.data[Self::checksum_offset()].copy_from_slice(&checksum.to_le_bytes());
    }

    fn get_len(&self) -> usize {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.data[Self::len_offset()]);
        u16::from_le_bytes(buf) as usize
    }

    fn set_len(&mut self, len: usize) {
        let len_u16 = len as u16;
        self.data[Self::len_offset()].copy_from_slice(&len_u16.to_le_bytes());
    }

    fn bake_len_and_checksum(&mut self) {
        assert!(self.content_end_pos <= DEFAULT_PAGE_SIZE);

        // Fill in the page length and checksum.
        self.set_len(self.content_end_pos);

        // Calculate and fill in the checksum. The checksum includes the length to the end of the page.
        let checksum = calc_checksum(&self.data[Self::checksum_offset().end..self.content_end_pos]);
        println!("Shake and bake {} checksum {:x}", self.content_end_pos, checksum);
        self.set_checksum(checksum);
    }

    // pub(super) fn finish(mut self) -> ([u8; DEFAULT_PAGE_SIZE], usize) {
    pub(super) fn finish(mut self) -> ([u8; DEFAULT_PAGE_SIZE], usize) {
        self.bake_len_and_checksum();

        // file.seek(SeekFrom::Start(page_no as u64 * DEFAULT_PAGE_SIZE as u64))?;
        // file.write_all(&self.data[0..self.pos])?;
        (self.data, self.content_end_pos)
        // self.data
    }

    pub(crate) fn push_u32(&mut self, num: u32) -> Result<(), SEError> {
        push_u32(self, num)
    }
    pub(crate) fn push_u64(&mut self, num: u64) -> Result<(), SEError> {
        push_u64(self, num)
    }
    pub(crate) fn push_usize(&mut self, num: usize) -> Result<(), SEError> {
        push_usize(self, num)
    }

    fn extend_from_slice_infallible(&mut self, slice: &[u8]) {
        InfallibleWritePage(self).extend_from_slice(slice)
    }
    fn push_u32_infallable(&mut self, num: u32) {
        push_u32(&mut InfallibleWritePage(self), num);
    }
    fn push_u64_infallible(&mut self, num: u64) {
        push_u64(&mut InfallibleWritePage(self), num);
    }
    fn push_usize_infallible(&mut self, num: usize) {
        push_usize(&mut InfallibleWritePage(self), num);
    }

    pub(super) fn write<F: DTFile>(&self, file: &mut F, page_no: PageNum) -> Result<(), SEError> {
        file.dt_write_all_at(&self.data, page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;
        Ok(())
    }

    pub(super) fn bake_and_write<F: DTFile>(&mut self, file: &mut F, page_no: PageNum) -> Result<(), SEError> {
        self.bake_len_and_checksum();
        self.write(file, page_no)
    }

    /// This reads a page in a primitive way. It just checks the checksum, but doesn't actually
    /// parse any of the content beyond the length. Further explicit parsing is needed to use the
    /// result.
    pub(super) fn read_raw<F: DTFile>(file: &mut F, page_no: PageNum) -> Result<Self, SEError> {
        let mut page = Self {
            data: [0; DEFAULT_PAGE_SIZE],
            // cursor_start_pos: Self::immutable_data_start_offset(),
            content_start_pos: Self::immutable_data_start_offset(),
            content_end_pos: usize::MAX,
        };

        file.dt_read_all_at(&mut page.data, page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;

        // I hate doing this here, but its the right place - since checking magic is cheaper than
        // reading the checksum.
        if T == PAGE_TYPE_HEADER {
            if page.data[PO_HEADER_MAGIC] != MAGIC_BYTES {
                return Err(CorruptPageError::InvalidHeaderMagicBytes.into());
            }
        }

        let len = page.get_len();
        if len <= Self::len_offset().end || len > page.data.len() {
            return Err(CorruptPageError::PageLengthInvalid(len as u16).into());
        }

        page.content_end_pos = len;

        if page.read_checksum() != page.calc_checksum() {
            return Err(CorruptPageError::InvalidChecksum.into());
        }

        Ok(page)
    }

    fn make_parser(&self) -> BufParser {
        BufParser(&self.data[self.content_start_pos..self.content_end_pos])
    }
}


impl HeaderPage {
    // We'll write and encode header pages in a "1-shot" way, because they get rewritten so
    // infrequently.
    pub(super) fn encode_and_bake(header_fields: &StorageHeaderFields) -> Self {
        assert_eq!(header_fields.page_size, DEFAULT_PAGE_SIZE, "Other block sizes are not yet implemented");

        let mut page = Self {
            data: [0; DEFAULT_PAGE_SIZE],
            // cursor_start_pos: usize::MAX,
            content_start_pos: PO_HEADER_START,
            content_end_pos: PO_HEADER_START,
        };

        page.data[PO_HEADER_MAGIC].copy_from_slice(&MAGIC_BYTES);
        page.data[PO_HEADER_FORMAT_VERSION].copy_from_slice(&FORMAT_VERSION.to_le_bytes());

        // TODO: Check how all these unwrap() calls affect binary size.
        page.push_usize_infallible(header_fields.page_size);

        for (kind, c) in header_fields.data_chunk_info_iter() {
            page.push_u32_infallable(kind + 1);
            page.push_u32_infallable(c.first_page);
            page.push_u32_infallable(c.blit_page);
        }
        page.push_u32_infallable(0);
        page.bake_len_and_checksum();

        page
    }

    fn get_version(&self) -> u16 {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.data[PO_HEADER_FORMAT_VERSION]);
        u16::from_le_bytes(buf)
    }

    pub(super) fn read<F: DTFile>(file: &mut F, page_no: PageNum) -> Result<StorageHeaderFields, SEError> {
        let page = Self::read_raw(file, page_no)?;
        // At this point the magic bytes have already been checked by read_raw.

        let file_version = page.get_version();
        if file_version != FORMAT_VERSION {
            return Err(CorruptPageError::VersionTooNew(file_version).into());
        }

        let mut parser = page.make_parser();
        let page_size = parser.next_usize()?;
        if page_size != DEFAULT_PAGE_SIZE {
            return Err(CorruptPageError::InvalidHeaderPageSize(page_size).into());
        }

        let mut data_page_info = smallvec![None; NUM_DATA_CHUNK_TYPES];
        loop {
            let chunk_type_or_end = parser.next_usize()?;
            if chunk_type_or_end == 0 { break; }
            let chunk_type = chunk_type_or_end - 1;
            let first_page = parser.next_u32()?;
            let blit_page = parser.next_u32()?;

            // TODO: Is it worth checking that the pages are valid?
            if first_page == blit_page { return Err(SEError::GenericInvalidData); }

            if data_page_info.len() < chunk_type {
                data_page_info.resize(chunk_type, None);
            }

            data_page_info[chunk_type] = Some(DataChunkHeaderInfo {
                blit_page,
                first_page,
            });
        }

        Ok(StorageHeaderFields {
            // file_format_version,
            page_size,
            data_page_info,
        })
    }
}

impl DataPage {
    pub(super) fn new(fields: DataPageImmutableFields) -> Self {
        let mut page = Self {
            data: [0; DEFAULT_PAGE_SIZE],
            // cursor_start_pos: usize::MAX,
            content_start_pos: usize::MAX,
            content_end_pos: PO_DATA_IMMUTABLE_FIELD_START,
        };

        // Write the immutable bytes. This will write at self.content_start_pos.
        page.push_u32_infallable(fields.kind as u32);
        page.push_u32_infallable(fields.prev_page);
        // page.cursor_start_pos = page.content_end_pos;
        // page.extend_from_slice_infallible(cursor_data);

        page.content_start_pos = page.content_end_pos;

        page
    }

    // pub fn get_cursor_data(&self) -> &[u8] {
    //     &self.data[self.cursor_start_pos..self.content_start_pos]
    // }

    pub fn get_next_or_associated_page(&self) -> PageNum {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.data[PO_DATA_NEXT_PAGE]);
        u32::from_le_bytes(buf)
    }

    pub fn set_next_page(&mut self, page: PageNum) {
        self.data[PO_DATA_NEXT_PAGE].copy_from_slice(&page.to_le_bytes());
    }

    pub fn get_blit_status(&self) -> BlitStatus { // TODO: Make a struct for this.
        BlitStatus(self.data[PO_DATA_BLIT_STATUS.start])
    }

    fn set_blit_status(&mut self, status: BlitStatus) {
        self.data[PO_DATA_BLIT_STATUS.start] = status.0;
    }

    pub fn roll_blit_status(&mut self) {
        let last = self.get_blit_status();
        // self.set_blit_status(BlitStatus(last.0.wrapping_add(1)));
        self.set_blit_status(BlitStatus((last.0 + 1) % 3));
    }
}

#[inline]
pub(in crate::storage) fn page_checksum_offset(is_header: bool) -> usize {
    if is_header {
        MAGIC_BYTES.len()
    } else { 0 }
}

#[inline]
pub fn page_len_offset(is_header: bool) -> usize {
    page_checksum_offset(is_header) + 4
}


#[inline]
pub fn page_first_byte_offset(is_header: bool) -> usize {
    page_len_offset(is_header) + 2
}

#[cfg(test)]
mod test {
    use crate::encoding::tools::ExtendFromSlice;
    use crate::storage::page::{BlitStatus, Page, DataPageImmutableFields, DataPage};
    use crate::storage::{DataPageType, PageType};

    #[test]
    fn blah() {
        let mut page = DataPage::new(DataPageImmutableFields {
            kind: DataPageType::AgentNames,
            prev_page: 0,
        });

        page.extend_from_slice("hello".as_bytes()).unwrap();

        page.set_blit_status(BlitStatus(6));
        page.set_next_page(123);

        dbg!(&page);

        let (bytes, len) = page.finish();
        dbg!(&bytes[0..len]);
    }
}

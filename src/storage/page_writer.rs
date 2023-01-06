use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use crate::encoding::tools::{calc_checksum, ExtendFromSlice};
use crate::encoding::varint::{push_u32, push_u64, push_usize};
use crate::storage;
use crate::storage::{DEFAULT_PAGE_SIZE, PageNum, SE_MAGIC_BYTES, SEError};

#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;

#[inline]
pub(super) fn page_checksum_offset(is_header: bool) -> usize {
    if is_header {
        SE_MAGIC_BYTES.len()
    } else { 0 }
}

#[inline]
fn page_len_offset(is_header: bool) -> usize {
    page_checksum_offset(is_header) + 4
}

#[inline]
fn page_first_byte_offset(is_header: bool) -> usize {
    page_len_offset(is_header) + 2
}

/// The page contains:
///
/// - (only for header pages) 8 bytes of magic
/// - Checksum (4 bytes CRC32 little endian)
/// - Page length (2 bytes little endian)
/// - Data (variable length)
#[derive(Debug, Clone)]
pub(super) struct PageWriter {
    data: [u8; DEFAULT_PAGE_SIZE],
    pos: usize, // We only need a u16 here, but usize is more ergonomic.

    /// This is a mess, but the file header needs its magic bytes at the start of the block, so
    /// the in-block offsets for length & checksum get bumped in the case of a header block.
    is_header: bool,
}

impl ExtendFromSlice for PageWriter {
    type Result = Result<(), SEError>;

    fn extend_from_slice(&mut self, slice: &[u8]) -> Result<(), SEError> {
        self.write_slice(slice)
    }
}

impl PageWriter {
    pub(super) fn new_header() -> Self {
        let mut writer = Self {
            data: [0; DEFAULT_PAGE_SIZE],
            pos: page_first_byte_offset(true),
            is_header: true,
        };

        writer.data[0..8].copy_from_slice(&SE_MAGIC_BYTES);

        writer
    }

    pub(super) fn new() -> Self {
        Self {
            data: [0; DEFAULT_PAGE_SIZE],
            pos: page_first_byte_offset(false),
            is_header: false,
        }
    }

    // I could write it like this but the optimizer inlines it the same without forcing it to
    // monomorphize.
    // pub fn write_arr<const SIZE: usize>(&mut self, arr: &[u8; SIZE]) -> Result<(), SEError> {
    pub(super) fn write_slice(&mut self, arr: &[u8]) -> Result<(), SEError> {
        if self.pos + arr.len() > self.data.len() {
            return Err(SEError::PageTooLarge);
        }
        self.data[self.pos..self.pos + arr.len()].copy_from_slice(arr);
        self.pos += arr.len();
        Ok(())
    }

    /// Fill in the page length and checksum.
    pub(super) fn finish(mut self) -> Result<([u8; DEFAULT_PAGE_SIZE], usize), SEError> {
        assert!(self.pos <= DEFAULT_PAGE_SIZE);

        // Fill in the page length
        let page_data_len = self.pos - page_first_byte_offset(self.is_header);

        let len_offset = page_len_offset(self.is_header);
        self.data[len_offset..len_offset + 2].copy_from_slice(&(page_data_len as u16).to_le_bytes());

        // Calculate and fill in the checksum. The checksum includes the length to the end of the page.
        let checksum = calc_checksum(&self.data[len_offset..self.pos]);

        let checksum_offset = page_checksum_offset(self.is_header);
        self.data[checksum_offset..checksum_offset+4].copy_from_slice(&checksum.to_le_bytes());

        // file.seek(SeekFrom::Start(page_no as u64 * DEFAULT_PAGE_SIZE as u64))?;
        // file.write_all(&self.data[0..self.pos])?;
        Ok((self.data, self.pos))
    }

    pub(super) fn finish_and_write(self, file: &mut File, page_no: PageNum) -> Result<(), SEError> {
        let (buffer, _len) = self.finish()?;

        // file.write_all(&buffer[0..len], page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;

        // We have a choice here about whether we want to write the whole page, or just write the
        // edited data.
        //
        // Given modern block storage devices usually work on 4k pages, I suspect writing an entire
        // 4kb will be faster, but this is worth benchmarking.
        #[cfg(target_os = "linux")]
        file.write_all_at(&buffer, page_no as u64 * DEFAULT_PAGE_SIZE as u64)?;
        #[cfg(not(target_os = "linux"))] {
            file.seek(SeekFrom::Start(page_no as u64 * DEFAULT_PAGE_SIZE as u64))?;
            file.write_all(buffer)?;
        }

        Ok(())
    }

    pub(super) fn write_u64(&mut self, val: u64) -> Result<(), SEError> {
        push_u64(self, val)
    }
    pub(super) fn write_u32(&mut self, val: u32) -> Result<(), SEError> {
        push_u32(self, val)
    }
    pub(super) fn write_usize(&mut self, val: usize) -> Result<(), SEError> {
        push_usize(self, val)
    }
}

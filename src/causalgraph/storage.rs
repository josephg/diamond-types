//! This is an experiment in storing the causal graph (time DAG) in a file.
//!
//! The file starts with magic bytes ("DMNDT_CG") and a version.
//!
//! Then we have the 2 blitting buffers. The buffers store outstanding entries for both agent
//! assignment and parent information.
//!
//! Then all the chunks. Each chunk has a type.
//!
//!
//! Blitting buffers contain:
//! - Checksum
//! - Length
//! - Entry index (goes up every time we flush to the end of the file)
//! - Counter (goes up every time we blit back and forth)
//! - Actual data


// TODO: Open question: Currently this tracks 2 kinds of data (agent assignment and parents). I
// suspect the code would be simpler & cleaner if both kinds of data were associated together, with
// a flag if the parents need to be stored. Might be a slight efficiency reduction but the
// difference is probably trivial. Do a POC of this and test it.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use bumpalo::Bump;
use rle::{HasLength, MergableSpan};
use crate::encoding::bufparser::BufParser;
use crate::encoding::parseerror::ParseError;
use crate::encoding::tools::{calc_checksum, push_u64, push_usize};
use crate::encoding::varint::{decode_usize, encode_usize};
use crate::{CausalGraph, DTRange, Time};
use bumpalo::collections::vec::Vec as BumpVec;
use crate::causalgraph::entry::CGEntry;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, read_cg_entry_into_cg_nonoverlapping, write_cg_entry};
use crate::encoding::map::{WriteMap, ReadMap};


const CG_MAGIC_BYTES: [u8; 8] = *b"DMNDT_CG";
const CG_VERSION: [u8; 4] = 1u32.to_le_bytes();

const CG_DEFAULT_BLIT_SIZE: u64 = 64;
// const CG_DEFAULT_BLIT_SIZE: u64 = 10;

// Magic bytes, version then blit size.
const CG_HEADER_LENGTH: usize = CG_MAGIC_BYTES.len() + CG_VERSION.len() + 4;
const CG_HEADER_LENGTH_U64: u64 = CG_HEADER_LENGTH as u64;

const MAX_BLIT_SIZE: usize = 1024;

#[derive(Debug)]
#[non_exhaustive]
pub enum CGError {
    InvalidHeader,
    UnexpectedEOF,
    ChecksumMismatch,

    InvalidBlit,
    // InvalidData,

    BlitTooLarge,

    ParseError(ParseError),
    IO(io::Error),
}

impl Display for CGError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseError {:?}", self)
    }
}

impl Error for CGError {}

impl From<io::Error> for CGError {
    fn from(io_err: io::Error) -> Self {
        if io_err.kind() == ErrorKind::UnexpectedEof { CGError::UnexpectedEOF }
        else { CGError::IO(io_err) }
    }
}
impl From<ParseError> for CGError {
    fn from(pe: ParseError) -> Self {
        CGError::ParseError(pe)
    }
}

#[derive(Debug, Clone)]
struct Blit<'a> {
    filesize: u64,
    counter: usize,
    data: &'a [u8],
}

impl<'a> PartialEq for Blit<'a> {
    // I don't think this is ever executed anyway.
    fn eq(&self, other: &Self) -> bool {
        self.filesize == other.filesize && self.counter == other.counter
    }
}

impl<'a> Eq for Blit<'a> {}

impl<'a> PartialOrd<Self> for Blit<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Blit<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.filesize.cmp(&other.filesize)
            .then(self.counter.cmp(&other.counter))
    }
}

#[derive(Debug)]
pub(crate) struct CGStorage {
    file: File,

    blit_size: u64,

    /// The write location is the position in the file where the next written chunk will go.
    /// This is an offset from the start of the data chunk (after header & blits).
    next_write_location: u64,

    /// The counter increments by 1 every time we update a blit without flushing a new chunk. Resets
    /// to 0 every time we write a chunk (and thus the write location increases).
    next_counter: usize,

    /// Set when we've appended data to the file but haven't synced it, or marked it as written with
    /// a new blit.
    dirty_blit: bool,
    /// False when we're ready to write blit 0, true when we're about to write blit 1.
    next_blit: bool,

    entry: CGEntry,

    write_map: WriteMap,

    next_flush_time: Time,
}

impl CGStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<(CausalGraph, CGStorage), CGError> {
        let mut cg = CausalGraph::new();

        let mut file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(false)
            .open(path.as_ref())?;

        let mut total_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        let blit_size = Self::read_header(&mut file, total_len)?;
        debug_assert_eq!(file.stream_position()?, CG_HEADER_LENGTH_U64);
        total_len = total_len.max(CG_HEADER_LENGTH_U64);

        let mut cgs = Self {
            file,
            blit_size,
            next_counter: 0,
            next_write_location: 0,
            dirty_blit: false,
            next_blit: false,
            entry: Default::default(),
            write_map: WriteMap::with_capacity_from(&cg.client_data),
            next_flush_time: 0,
        };

        // If the file doesn't have room for the blit data, its probably new. Just set_len().
        let ds = cgs.data_start();
        if total_len < ds {
            cgs.file.set_len(ds)?;
            total_len = ds;
            cgs.file.sync_all()?; // Force update metadata to include the new size.
        }

        // Next we need to read the blit data to find out the flushed file size. Any bytes after
        // the file size specified in the last blit come from stale writes, and they're discarded.

        // The blits will be read into the provided (stack) buffer.
        let mut raw_buf = [0u8; MAX_BLIT_SIZE * 2];
        let active_blit = cgs.read_initial_blits(&mut raw_buf, blit_size)?;

        let committed_filesize = active_blit.filesize;

        // dbg!(&active_blit);

        assert!(committed_filesize <= total_len - cgs.data_start());

        debug_assert_eq!(cgs.file.stream_position()?, cgs.data_start());


        // Now scan all the entries in the data chunk.

        // TODO: This is suuuper duper dirty!
        let mut buf = vec![0u8; active_blit.filesize as usize];
        cgs.file.read_exact(&mut buf)?;
        // dbg!(&buf);

        let mut r = BufParser(&buf);
        let mut read_map = ReadMap::new();
        // let mut next_pos
        while !r.is_empty() {
            // We can use non_overlapping because the cg is guaranteed to be empty.
            read_cg_entry_into_cg_nonoverlapping(&mut r, true, &mut cg, &mut read_map)?;
        }
        cgs.write_map.populate_from_dec(&read_map);

        if !active_blit.data.is_empty() {
            let mut reader = BufParser(active_blit.data);
            let id_p = read_cg_entry_into_cg_nonoverlapping(&mut reader, false, &mut cg, &mut read_map)?;
            cgs.entry = id_p;

            // dbg!(&cgs.last_parents, &cgs.assigned_to);

            assert!(reader.is_empty());
        }
        cgs.next_flush_time = cg.len();

        debug_assert_eq!(cgs.file.stream_position()?, cgs.data_start() + committed_filesize);

        Ok((cg, cgs))
    }

    fn read_initial_blits<'a>(&mut self, raw_buf: &'a mut [u8; MAX_BLIT_SIZE * 2], blit_size: u64) -> Result<Blit<'a>, io::Error> {
        let bs_u = blit_size as usize;
        let buf = &mut raw_buf[..bs_u * 2];
        self.file.read_exact(buf)?;

        let b1 = Self::read_blit(&buf[0..bs_u]);
        let b2 = Self::read_blit(&buf[bs_u..bs_u * 2]);
        let (active_blit, next_blit) = match (b1, b2) {
            (Ok(b1), Ok(b2)) => {
                // dbg!(&b1, &b2);
                match b1.cmp(&b2) {
                    Ordering::Less | Ordering::Equal => (b2, false),
                    Ordering::Greater => (b1, true),
                }
            },
            (Ok(b1), _) => (b1, true),
            (_, Ok(b2)) => (b2, false),
            _ => {
                (Blit {
                    filesize: 0,
                    counter: 0,
                    data: &[]
                }, false)
            }
        };

        self.next_blit = next_blit;
        self.next_counter = active_blit.counter + 1;
        self.next_write_location = active_blit.filesize;

        Ok(active_blit)
    }

    fn read_blit(buf: &[u8]) -> Result<Blit, CGError> {
        // Blits always start with a checksum,
        // dbg!(buf);
        let mut pos = 0;
        let expected_checksum = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        pos += 4;

        // Length
        let (len, len_size) = decode_usize(&buf[pos..]).map_err(|e| {
            assert_eq!(e, ParseError::InvalidVarInt);
            CGError::InvalidBlit
        })?;
        pos += len_size;

        // We need to explicitly check for len == 0 because the checksum of nothing is 0 :/
        if len == 0 || buf.len() - pos < len {
            return Err(CGError::InvalidBlit);
        }

        let mut r = BufParser(&buf[pos..pos+len]);

        let actual_checksum = calc_checksum(r.0);
        if expected_checksum != actual_checksum {
            return Err(CGError::ChecksumMismatch);
        }

        let filesize = r.next_u64().map_err(|_| CGError::InvalidBlit)?;
        let counter = r.next_usize().map_err(|_| CGError::InvalidBlit)?;

        Ok(Blit {
            filesize,
            counter,
            data: r.0
        })
    }

    fn next_blit_location(&self) -> u64 {
        CG_HEADER_LENGTH_U64 + (self.blit_size * self.next_blit as u64)
    }

    fn write_blit_with_data(&mut self, data: &[u8]) -> Result<(), CGError> {
        self.write_blit(Blit {
            filesize: self.next_write_location,
            counter: self.next_counter,
            data
        })?;
        self.next_counter += 1;
        self.dirty_blit = false;
        Ok(())
    }

    fn write_blit(&mut self, blit: Blit) -> Result<(), CGError> {
        debug_assert_eq!(self.file.seek(SeekFrom::Current(0)).unwrap(), self.next_write_location + self.data_start());
        self.file.seek(SeekFrom::Start(self.next_blit_location()))?;

        Self::write_blit_to(BufWriter::new(&mut self.file), self.blit_size, blit)?;
        self.file.flush()?;
        self.file.sync_data()?;

        self.next_blit = !self.next_blit;
        self.file.seek(SeekFrom::Start(self.next_write_location + self.data_start()))?;

        Ok(())
    }

    fn write_blit_to<W: Write>(mut w: BufWriter<W>, max_size: u64, blit: Blit) -> Result<(), CGError> {
        let mut body = Vec::new(); // Bleh. TODO: Better to allocate on the stack here.
        push_u64(&mut body, blit.filesize);
        push_usize(&mut body, blit.counter);
        body.extend_from_slice(blit.data); // TODO: Less copying!

        let checksum = calc_checksum(&body);
        w.write(&checksum.to_le_bytes())?;

        let mut buf = [0u8; 10];
        let len_len = encode_usize(body.len(), &mut buf);
        w.write(&buf[..len_len])?;

        // TODO: DO THIS BETTER!!
        if 4 + len_len + body.len() > max_size as usize {
            return Err(CGError::BlitTooLarge)
        }

        w.write(&body)?;

        Ok(())
    }

    fn write_data(&mut self, data: &[u8]) -> Result<(), io::Error> {
        // First we write the data to the end of the file.
        debug_assert_eq!(self.file.seek(SeekFrom::Current(0)).unwrap(), self.next_write_location + self.data_start());

        self.file.write_all(data)?;
        self.next_write_location += data.len() as u64;
        self.next_counter = 0;

        self.dirty_blit = true;

        Ok(())
    }

    fn data_start(&self) -> u64 {
        CG_HEADER_LENGTH_U64 + self.blit_size * 2
    }

    /// Returns blit size.
    fn read_header(mut file: &mut File, total_len: u64) -> Result<u64, CGError> {
        let blitsize = if total_len < CG_HEADER_LENGTH_U64 {
            // Presumably we're creating a new file.
            let mut bw = BufWriter::new(file);
            bw.write_all(&CG_MAGIC_BYTES)?;
            bw.write_all(&CG_VERSION)?;
            bw.write_all(&(CG_DEFAULT_BLIT_SIZE as u32).to_le_bytes())?;

            file = bw.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;

            CG_DEFAULT_BLIT_SIZE
        } else {
            // Check the WAL header.
            let mut header = [0u8; CG_HEADER_LENGTH];
            file.read_exact(&mut header)?;
            let mut pos = 0;
            if header[0..CG_MAGIC_BYTES.len()] != CG_MAGIC_BYTES {
                eprintln!("Causality graph has invalid magic bytes");
                return Err(CGError::InvalidHeader);
            }
            pos += CG_MAGIC_BYTES.len();

            if header[pos..pos + CG_VERSION.len()] != CG_VERSION {
                eprintln!("Causality graph has unknown version");
                return Err(CGError::InvalidHeader);
            }
            pos += CG_VERSION.len();

            // Read the blit size.
            // This try_into stuff will get optimized out: https://godbolt.org/z/f886W5hvW
            let blit_size = u32::from_le_bytes(header[pos..pos+4].try_into().unwrap()) as u64;
            if blit_size > MAX_BLIT_SIZE as u64 {
                eprintln!("Causality graph has invalid blit size ({blit_size} > {MAX_BLIT_SIZE})");
                return Err(CGError::InvalidHeader);
            }
            // pos += 4;

            blit_size
        };

        debug_assert_eq!(file.stream_position()?, CG_HEADER_LENGTH_U64);
        Ok(blitsize)
    }

    fn encode_last_entry(&mut self, buf: &mut BumpVec<u8>, persist: bool, cg: &CausalGraph) {
        write_cg_entry(buf, &self.entry, &mut self.write_map, persist, &cg);
    }

    pub(crate) fn push_entry_no_sync(&mut self, bump: &Bump, entry: CGEntry, cg: &CausalGraph) -> Result<bool, CGError> {
        if entry.is_empty() { return Ok(false); }

        let mut buf = BumpVec::new_in(bump);

        self.dirty_blit = true;
        Ok(if self.entry.is_empty() {
            self.entry = entry;
            false
        } else if self.entry.can_append(&entry) {
            self.entry.append(entry);
            false
        } else {
            // Flush the last entry out.
            // eprintln!("Writing entry to data {:?}", self.entry);
            self.encode_last_entry(&mut buf, true, cg);
            self.write_data(&buf)?;

            // Then save the new value in a fresh blit.
            self.entry = entry;
            true
        })
    }

    fn flush(&mut self, bump: &Bump, cg: &CausalGraph) -> Result<(), CGError> {
        if !self.dirty_blit { return Ok(()); }

        // Not needed in a lot of situations.
        // self.file.sync_all();

        // Regardless of what happened above, write a new blit entry.
        // eprintln!("Writing blip {:?} / {:?}", self.last_parents, self.assigned_to);
        let mut buf = BumpVec::new_in(bump);
        self.encode_last_entry(&mut buf, false, cg);
        let result = self.write_blit_with_data(&buf);

        match result {
            Err(CGError::BlitTooLarge) => {
                eprintln!("Warning: Blit too small for data");

                // The buffered data doesn't fit in the blit region. This should basically never happen
                // in regular use - but if the user merges lots of changes for some reason, or if they
                // have super long UIDs this will happen.
                //
                // Luckily there's a reasonable fallback here - we can flush out the blit to the end of
                // the data segment anyway. We lose some compaction, but this is rare enough it doesn't
                // matter.

                // We could only write out the larger of these two, but eh.
                buf.clear();
                self.encode_last_entry(&mut buf, true, cg);

                self.file.seek(SeekFrom::Start(self.next_write_location + self.data_start()))?;
                self.write_data(&buf)?;
                self.file.sync_all()?;

                self.entry.clear();

                self.write_blit_with_data(&[])?;
            },
            Err(e) => { return Err(e); }
            _ => {}
        }

        Ok(())
    }

    pub(crate) fn save_missing(&mut self, cg: &CausalGraph) -> Result<(), CGError>{
        let bump = Bump::new();

        let mut needs_sync = false;

        let range: DTRange = (self.next_flush_time..cg.len()).into();
        for entry in cg.iter_range(range) {
            needs_sync |= self.push_entry_no_sync(&bump, entry, cg)?;
        }

        if needs_sync {
            self.file.sync_all()?;
        }

        self.flush(&bump, cg)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::{File, remove_file};
    use std::io::Read;
    use std::path::Path;
    use crate::causalgraph::storage::CGStorage;

    #[test]
    fn foo() {
        drop(std::fs::remove_file("test.cg"));

        let (mut cg, mut cgs) = CGStorage::open("test.cg").unwrap();
        // dbg!(&cgs, &cg);

        let seph = cg.get_or_create_agent_id("seph");
        cg.assign_op(&[], seph, 10);
        cg.assign_op(&[5], seph, 15);
        cg.assign_op(&[15], seph, 20);
        // dbg!(&cg);

        cgs.save_missing(&cg).unwrap();

        // dbg!(&cgs);

        drop(cgs);
        let (cg2, _) = CGStorage::open("test.cg").unwrap();
        // dbg!((cg, cg2));
        assert_eq!(cg, cg2);
        cg2.dbg_check(true);
    }

    #[test]
    fn write_node_nodecc() {
        if !std::path::Path::exists(Path::new("node_nodecc.dt")) {
            eprintln!("Test ignored - node_nodecc.dt does not exist.");
            return;
        }

        use crate::list::ListOpLog;

        let mut bytes = vec![];
        File::open("node_nodecc.dt").unwrap().read_to_end(&mut bytes).unwrap();
        let o = ListOpLog::load_from(&bytes).unwrap();

        let cg = o.cg;

        drop(remove_file("node_nodecc.cg"));
        let (_, mut cgs) = CGStorage::open("node_nodecc.cg").unwrap();
        cgs.save_missing(&cg).unwrap();
        drop(cgs);

        // Open it back up again and check the contents match.
        let (cg2, _) = CGStorage::open("node_nodecc.cg").unwrap();
        // dbg!(cg2);

        assert_eq!(cg, cg2);
        cg2.dbg_check(true);
    }
}
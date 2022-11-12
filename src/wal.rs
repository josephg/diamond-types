/// The write-ahead log encodes new operations directly to disk in chunks. Each chunk has a
/// checksum, so inopportune crashes don't corrupt any data.
///
/// Design question:
///
/// This is a bit controversial, but there's two options here for how I encode WAL entries:
///
/// 1. Each entry has a fresh agent & txn map. This will make the WAL entries bigger, because
/// they'll all explicitly name all the IDs used and referenced.
///
/// But the benefit is that we can blindly append to the WAL, without reading any of the data first.
/// Mind you, if the WAL has a corrupt tail (the last entries are broken), then this will have no
/// effect. So to blindly append you'd still need to scan the chunks in the WAL anyway.
///
/// Or 2. Entries reuse an agent/txn map. This would result in smaller file sizes, but we can't
/// blindly sendfile() at the WAL.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use crate::encoding::parseerror::ParseError;
use crate::{DTRange, RleVec, LV};
use std::ffi::OsString;
use std::{fs, io};
use std::io::{BufReader, ErrorKind, Read, Result as IOResult, Seek, SeekFrom, Write};
use std::path::Path;
use bumpalo::Bump;
use crate::encoding::{ChunkType, varint};
use crate::encoding::tools::{calc_checksum, push_chunk};
use bumpalo::collections::vec::Vec as BumpVec;
use rle::HasLength;
use crate::{CausalGraph, KVPair, Ops};
use crate::encoding::bufparser::BufParser;
use crate::encoding::cg_entry::{read_cg_entry_into_cg, write_cg_entry_iter};
use crate::encoding::chunk_reader::ChunkReader;
use crate::encoding::op::write_ops;
use crate::encoding::map::{WriteMap, ReadMap};

// pub(crate) mod wal_encoding;

#[derive(Debug)]
#[non_exhaustive]
pub enum WALError {
    InvalidHeader,
    UnexpectedEOF,
    ChecksumMismatch,
    ParseError(ParseError),
    IO(io::Error),
}

#[derive(Debug)]
pub(crate) struct WriteAheadLog {
    file: File,

    write_map: WriteMap,
    
    // The WAL just stores changes in order. We don't need to worry about complex time DAG
    // traversal.
    next_version: LV,
}

impl Display for WALError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseError {:?}", self)
    }
}

impl Error for WALError {}

impl From<io::Error> for WALError {
    fn from(io_err: io::Error) -> Self {
        if io_err.kind() == ErrorKind::UnexpectedEof { WALError::UnexpectedEOF }
        else { WALError::IO(io_err) }
    }
}

impl From<ParseError> for WALError {
    fn from(pe: ParseError) -> Self {
        WALError::ParseError(pe)
    }
}

// The file starts with "DMNDTWAL" followed by a 4 byte LE file version.
const WAL_MAGIC_BYTES: [u8; 8] = *b"DMNDTWAL";
const WAL_VERSION: [u8; 4] = 1u32.to_le_bytes();
const WAL_HEADER_LENGTH: usize = WAL_MAGIC_BYTES.len() + WAL_VERSION.len();
const WAL_HEADER_LENGTH_U64: u64 = WAL_HEADER_LENGTH as u64;


impl WriteAheadLog {
    pub fn open<P: AsRef<Path>>(path: P, cg: &mut CausalGraph) -> Result<(Self, Ops), WALError> {
        let mut file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(false)
            .open(path.as_ref())?;

        // Before anything else, we scan the file to find out the current value.
        debug_assert_eq!(file.stream_position()?, 0); // Should be 0 since we're not in append mode.

        Self::prep_file(file, path.as_ref(), cg)

        // Ok((Self {
        //     file,
        //     txn_map: Default::default(),
        //     agent_map: AgentMappingEnc::with_capacity_from(&cg.client_data),
        //     next_version: 0
        // }, ops))
    }

    fn check_header(file: &mut File, total_len: u64) -> Result<(), WALError> {
        if total_len < WAL_HEADER_LENGTH_U64 {
            // Presumably we're creating a new file.
            file.write_all(&WAL_MAGIC_BYTES)?;
            file.write_all(&WAL_VERSION)?;
            file.sync_all()?;
        } else {
            // Check the WAL header.
            let mut header = [0u8; WAL_HEADER_LENGTH];
            file.read_exact(&mut header)?;
            if header[0..WAL_MAGIC_BYTES.len()] != WAL_MAGIC_BYTES {
                eprintln!("WAL has invalid magic bytes");
                return Err(WALError::InvalidHeader);
            }

            if header[WAL_MAGIC_BYTES.len()..] != WAL_VERSION {
                eprintln!("WAL has unknown version");
                return Err(WALError::InvalidHeader);
            }
        }

        debug_assert_eq!(file.stream_position()?, WAL_HEADER_LENGTH_U64);
        Ok(())
    }

    fn prep_file<P: AsRef<Path>>(mut file: File, path: P, cg: &mut CausalGraph) -> Result<(Self, Ops), WALError> {
        // First we need to know how large the file is.
        let total_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        Self::check_header(&mut file, total_len)?;
        // check_header will make the file at a minimum HEADER_LEN.
        let total_len = total_len.max(WAL_HEADER_LENGTH_U64);

        debug_assert_eq!(file.stream_position()?, WAL_HEADER_LENGTH_U64);
        let mut pos = WAL_HEADER_LENGTH_U64;

        let mut r = BufReader::new(file);

        let mut read_map = ReadMap::new();

        let mut ops = Ops::default();

        let file = loop {
            if pos >= total_len {
                break r.into_inner();
            }
            debug_assert_eq!(r.stream_position()?, pos);

            match Self::consume_chunk(&mut r, total_len - pos) {
                Ok((chunk_total_len, chunk_bytes)) => {
                    // dbg!(chunk_bytes);
                    // let (value, _) = varint::decode_u32(&chunk_bytes).unwrap();
                    // dbg!(value);
                    // parse_chunk(&chunk_bytes)?;
                    Self::read_chunk(&chunk_bytes, &mut ops, &mut read_map, cg)?;

                    pos += chunk_total_len;
                }
                Err(err @ WALError::ChecksumMismatch | err @ WALError::UnexpectedEOF) => {
                    // If a chunk is invalid, it probably signifies that a partial write happened.
                    // We'll truncate the file here and recover. Hopefully other peers have the
                    // change that we failed to save.
                    eprintln!("ERROR: Last chunk is invalid: {}", err);
                    eprintln!("Subsequent chunks skipped. Operation log will be truncated.");

                    let mut backup_path = OsString::from(path.as_ref());
                    backup_path.push(".backup");
                    let backup_path = Path::new(&backup_path);
                    eprintln!("Backing up corrupted data to '{}'", backup_path.display());
                    fs::copy(path, backup_path)?;

                    let mut f = r.into_inner();
                    // Set the seek position such that the next chunk written will overwrite the
                    // invalid data.
                    f.seek(SeekFrom::Start(pos))?;

                    // Truncating the file is not strictly necessary for correctness, but its
                    // cleaner, and it means the database will not error when we reload.
                    f.set_len(pos)?;
                    break f;
                }
                Err(err) => {
                    // Other errors are non-recoverable.
                    return Err(err)
                }
            }
        };

        debug_assert_eq!(pos, total_len);
        Ok((Self {
            file,
            write_map: WriteMap::from_dec(&cg.client_data, read_map),
            next_version: 0 // TODO!
        }, ops))
    }

    fn consume_chunk(r: &mut BufReader<File>, remaining_len: u64) -> Result<(u64, Vec<u8>), WALError> {
        let header_len: u64 = 4 + 4; // CRC32 + Length (LE).

        if remaining_len < header_len {
            return Err(WALError::UnexpectedEOF);
        }

        // Checksum
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        let expected_checksum = u32::from_le_bytes(buf);

        // Length
        r.read_exact(&mut buf)?;
        let len = u32::from_le_bytes(buf) as usize;

        if remaining_len < header_len + len as u64 {
            return Err(WALError::UnexpectedEOF);
        }

        let mut chunk_bytes = vec![0; len];
        r.read_exact(&mut chunk_bytes)?;

        // Now check that the checksum matches.
        let actual_checksum = calc_checksum(&chunk_bytes);
        if expected_checksum != actual_checksum {
            return Err(WALError::ChecksumMismatch);
        }

        Ok((header_len + len as u64, chunk_bytes))
    }

    pub fn write_chunk<F>(&mut self, chunk_writer: F) -> IOResult<()>
        where F: FnOnce(&Bump, &mut BumpVec<u8>) -> IOResult<()>
    {
        // The chunk header contains a checksum + length. In order to minimize the number of bytes
        // in the WAL, I could use a varint to store the length. But that makes encoding and
        // decoding significantly more complex, since the header (which specifies the length) also
        // has a variable length.
        //
        // Instead I'm just going to use a u32 for the checksum and a u32 for the length. Its a few
        // wasted bytes per file chunk. Not a big deal since we'll reclaim that space during
        // compaction anyway.

        // Also note a u32 per chunk means chunks can't be bigger than 4gb. I'm ok with that
        // constraint for now.

        let bump = Bump::new();
        let mut chunk_bytes = BumpVec::with_capacity_in(1024, &bump);

        let header_len = 4 + 4;

        chunk_bytes.resize(header_len, 0);

        chunk_writer(&bump, &mut chunk_bytes)?;

        let len = chunk_bytes.len() - header_len;
        assert!(len < u32::MAX as usize, "Chunk cannot be >4gb bytes in size");

        let checksum = calc_checksum(&chunk_bytes[header_len..]);
        chunk_bytes[0..4].copy_from_slice(&checksum.to_le_bytes());
        chunk_bytes[4..8].copy_from_slice(&(len as u32).to_le_bytes());

        self.file.write_all(&chunk_bytes)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub fn flush(&mut self, cg: &CausalGraph, ops: &Ops) -> Result<(), WALError> {
        let next = cg.len();

        if next == self.next_version {
            // Nothing to do!
            return Ok(());
        }

        // Data to store:
        //
        // - Agent assignment
        // - Parents
        // - Ops within the specified range

        let range = (self.next_version..next).into();
        self.write_chunk(|bump, buf| {
            // let start = buf.len();

            let mut write_map = WriteMap::with_capacity_from(&cg.client_data);

            let iter = cg.iter_range(range);
            let cg_data = write_cg_entry_iter(bump, iter, &mut write_map, cg);

            let ops_iter = ops.ops.iter_range_ctx(range, &ops.list_ctx);
            let ops = write_ops(bump, ops_iter, range.start, &write_map, &ops.list_ctx, cg);
            // dbg!(&ops);

            push_chunk(buf, ChunkType::CausalGraph, &cg_data);
            push_chunk(buf, ChunkType::Operations, &ops);

            // dbg!(&buf[start..]);
            // dbg!(buf.len() - start);

            Ok(())
        })?;

        self.next_version = next;
        Ok(())
    }

    fn read_chunk(bytes: &[u8], _ops: &mut Ops, read_map: &mut ReadMap, cg: &mut CausalGraph) -> Result<(), WALError> {
        dbg!(bytes.len());

        let mut reader = ChunkReader(BufParser(bytes));
        let mut cg_chunk = reader.expect_chunk(ChunkType::CausalGraph)?;

        while !cg_chunk.is_empty() {
            read_cg_entry_into_cg(&mut cg_chunk, true, cg, read_map)?;

        }

        let _ops_chunk = reader.expect_chunk(ChunkType::Operations)?;
        // TODO: Read ops chunk!

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use crate::{CausalGraph, CRDTKind, KVPair, Op, OpContents, OpLog, Ops, CreateValue, Primitive, CollectionOp, WriteAheadLog};
    use crate::ROOT_CRDT_ID;

    #[test]
    fn simple_encode_test() {
        let mut cg = CausalGraph::new();
        // let mut oplog = OpLog::new();

        let path = "test.wal";
        drop(std::fs::remove_file(path)); // Ignoring errors.
        let (mut wal, mut ops) = WriteAheadLog::open("test.wal", &mut cg).unwrap();

        wal.flush(&cg, &ops).unwrap(); // Should do nothing!

        let seph = cg.get_or_create_agent_id("seph");
        // let mike = cg.get_or_create_agent_id("mike");

        let mut span = cg.assign_local_op(seph, 1);
        ops.ops.push(KVPair(span.start, Op {
            target_id: ROOT_CRDT_ID,
            contents: OpContents::MapSet("hi".into(), CreateValue::Primitive(Primitive::I64(123)))
        }));
        span = cg.assign_local_op(seph, 1);
        ops.ops.push(KVPair(span.start, Op {
            target_id: ROOT_CRDT_ID,
            contents: OpContents::MapSet("cool set".into(), CreateValue::NewCRDT(CRDTKind::Collection))
        }));

        span = cg.assign_local_op(seph, 1);
        ops.ops.push(KVPair(span.start, Op {
            target_id: ROOT_CRDT_ID,
            contents: OpContents::Collection(CollectionOp::Insert(CreateValue::NewCRDT(CRDTKind::Register)))
        }));

        wal.flush(&cg, &ops).unwrap();
        dbg!(&wal);


        drop(wal);
        let mut cg = CausalGraph::new();
        let (mut _wal, mut _ops) = WriteAheadLog::open("test.wal", &mut cg).unwrap();


        // let mut v = 0;
        //
        // oplog.set_at_path(seph, &[Key("name")], I64(1));
        // let t = oplog.set_at_path(seph, &[Key("name")], I64(2));
        // // wal.flush(&oplog).unwrap();
        // oplog.set_at_path(seph, &[Key("name")], I64(3));
        // oplog.set_at_path(mike, &[Key("name")], I64(4));
        // wal.flush(&oplog).unwrap();
        //
        // let item = oplog.get_or_create_map_child(ROOT_CRDT_ID, "child".into());
        // oplog.append_set(mike, &[t], item, Primitive::I64(321));
        // // dbg!(oplog.checkout(&oplog.version));
        //
        // // dbg!(&oplog);
        // oplog.dbg_check(true);
        //
        // wal.flush(&oplog).unwrap();
    }
}
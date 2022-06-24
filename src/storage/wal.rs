use std::error::Error;
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::{fs, io};
use std::io::{BufReader, ErrorKind, Read, Result as IOResult, Seek, SeekFrom, Write};
use std::path::Path;
use crate::encoding::varint;
use crate::list::encoding::calc_checksum;

// The file starts with "DMNDTWAL" followed by a 4 byte LE file version.
const WAL_MAGIC_BYTES: [u8; 8] = *b"DMNDTWAL";
const WAL_VERSION: [u8; 4] = 1u32.to_le_bytes();
const WAL_HEADER_LENGTH: usize = WAL_MAGIC_BYTES.len() + WAL_VERSION.len();
const WAL_HEADER_LENGTH_U64: u64 = WAL_HEADER_LENGTH as u64;

pub struct WriteAheadLog(File);

#[derive(Debug)]
pub enum WALError {
    InvalidHeader,
    UnexpectedEOF,
    ChecksumMismatch,
    IO(io::Error),
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

impl WriteAheadLog {
    pub fn open<P: AsRef<Path>, F>(path: P, parse_chunk: F) -> Result<Self, WALError>
        where F: FnMut(&[u8]) -> Result<(), WALError>
    {
        let mut file = File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(false)
            .open(path.as_ref())?;

        // Before anything else, we scan the file to find out the current value.
        debug_assert_eq!(file.stream_position()?, 0); // Should be 0 since we're not in append mode.

        let file = Self::prep_file(file, path.as_ref(), parse_chunk)?;

        Ok(Self(file))
    }

    fn check_header(file: &mut File, total_len: u64) -> Result<(), WALError> {
        if total_len < WAL_HEADER_LENGTH_U64 {
            // Presumably we're creating a new file.
            file.write_all(&WAL_MAGIC_BYTES)?;
            file.write_all(&WAL_VERSION)?;
            file.sync_all();
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

    fn prep_file<P: AsRef<Path>, F>(mut file: File, path: P, mut parse_chunk: F) -> Result<File, WALError>
        where F: FnMut(&[u8]) -> Result<(), WALError>
    {
        // First we need to know how large the file is.
        let total_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        Self::check_header(&mut file, total_len)?;

        debug_assert_eq!(file.stream_position()?, WAL_HEADER_LENGTH_U64);
        let mut pos = WAL_HEADER_LENGTH_U64;

        let mut r = BufReader::new(file);

        while pos < total_len {
            debug_assert_eq!(r.stream_position()?, pos);

            match Self::consume_chunk(&mut r, total_len - pos) {
                Ok((chunk_total_len, chunk_bytes)) => {
                    // dbg!(chunk_bytes);
                    // let (value, _) = varint::decode_u32(&chunk_bytes).unwrap();
                    // dbg!(value);
                    parse_chunk(&chunk_bytes)?;

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

                    // Truncating the file is not strictly necessary for correctness, but it means
                    // the database will not error when we reload.
                    f.set_len(pos)?;
                    return Ok(f);
                }
                Err(err) => {
                    // Other errors are non-recoverable.
                    return Err(err)
                }
            }
        }

        debug_assert_eq!(pos, total_len);
        Ok(r.into_inner())
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

        let mut chunk_bytes = Vec::with_capacity(len);
        chunk_bytes.resize(len, 0);
        r.read_exact(&mut chunk_bytes)?;

        // Now check that the checksum matches.
        let actual_checksum = calc_checksum(&chunk_bytes);
        if expected_checksum != actual_checksum {
            return Err(WALError::ChecksumMismatch);
        }

        Ok((header_len + len as u64, chunk_bytes))
    }

    pub fn write_chunk<F>(&mut self, chunk_writer: F) -> IOResult<()>
        where F: FnOnce(&mut Vec<u8>) -> IOResult<()>
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

        let mut chunk_bytes = Vec::with_capacity(1024);

        let header_len = 4 + 4;

        chunk_bytes.resize(header_len, 0);

        chunk_writer(&mut chunk_bytes)?;

        let len = chunk_bytes.len() - header_len;
        assert!(len < u32::MAX as usize, "Chunk cannot be >4gb bytes in size");

        let checksum = calc_checksum(&chunk_bytes[header_len..]);
        chunk_bytes[0..4].copy_from_slice(&checksum.to_le_bytes());
        chunk_bytes[4..8].copy_from_slice(&(len as u32).to_le_bytes());

        self.0.write_all(&chunk_bytes)?;
        self.0.sync_all()?;

        Ok(())
    }
}
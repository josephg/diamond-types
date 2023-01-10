//! Ordinarily I'd just make direct calls to the std::File API, but I'm wrapping the file API here
//! so I can swap out the implementation with something we can test.

use std::fs::File;
use std::path::Path;
use std::io;
#[cfg(not(unix))]
use std::io::{Read, Write};
use std::io::{Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

pub trait DTFilesystem {
    type File: DTFile;

    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File>;
}

pub trait DTFile {
    fn stream_len(&mut self) -> io::Result<u64>;

    fn dt_write_all_at(&mut self, data: &[u8], offset: u64) -> io::Result<()>;
    fn dt_read_all_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<()>;

    fn sync_all(&self) -> io::Result<()>;
    fn sync_data(&self) -> io::Result<()>;
}

// *** Os filesystem. This is the real filesystem - and the actual filesystem used will monomorphize to this.

pub struct OsFilesystem;

impl DTFilesystem for OsFilesystem {
    type File = File;

    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        File::options()
            .read(true)
            .create(true)
            .write(true)
            .append(false)
            .open(path.as_ref())
    }
}

impl DTFile for File {
    fn stream_len(&mut self) -> io::Result<u64> {
        self.seek(SeekFrom::End(0))
    }

    fn dt_write_all_at(&mut self, data: &[u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        self.write_all_at(data, offset)?;
        #[cfg(not(unix))] {
            self.seek(std::io::SeekFrom::Start(offset))?;
            self.write_all(data)?;
        }

        Ok(())
    }

    fn dt_read_all_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        self.read_exact_at(buffer, offset)?;
        #[cfg(not(unix))] {
            self.seek(SeekFrom::Start(offset))?;
            self.read_exact(buffer)?;
        }

        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }
    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}
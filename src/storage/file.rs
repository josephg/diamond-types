//! Ordinarily I'd just make direct calls to the std::File API, but I'm wrapping the file API here
//! so I can swap out the implementation with something we can test.

use std::fs::File;
use std::path::Path;
use std::io;
#[cfg(not(unix))]
use std::io::{Read, Write};
use std::io::{ErrorKind, Seek, SeekFrom};
use std::os::fd::{AsFd, AsRawFd};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::ffi::c_int;

pub trait DTFile {
    fn stream_len(&mut self) -> io::Result<u64>;

    fn write_all_at(&mut self, data: &[u8], offset: u64) -> io::Result<()>;
    fn read_all_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<()>;

    // fn sync_all(&self) -> io::Result<()>;

    // Might be cleaner to make both of these methods take a &self and use RefCell when necessary.
    fn write_barrier(&mut self) -> io::Result<()>;
    fn sync_data(&mut self) -> io::Result<()>;
}

impl DTFile for File {
    fn stream_len(&mut self) -> io::Result<u64> {
        self.seek(SeekFrom::End(0))
    }

    fn write_all_at(&mut self, data: &[u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        <Self as FileExt>::write_all_at(self, data, offset)?;
        #[cfg(not(unix))] {
            self.seek(std::io::SeekFrom::Start(offset))?;
            self.write_all(data)?;
        }

        Ok(())
    }

    fn read_all_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        <Self as FileExt>::read_exact_at(self, buffer, offset)?;
        #[cfg(not(unix))] {
            self.seek(SeekFrom::Start(offset))?;
            self.read_exact(buffer)?;
        }

        Ok(())
    }

    fn write_barrier(&mut self) -> io::Result<()> {
        // I have this as a separate function because fsync is very slow on apple hardware (probably
        // because its not cheating). When we finalize a block with blitted data or write a new
        // file header, we need to enforce specific write ordering to make sure the block is written
        // correctly. But thankfully, apple platforms expose F_BARRIERFSYNC which enforces write
        // ordering without needing to incur the cost of a full fsync.
        //
        // Unfortunately, std doesn't expose a wrapper around F_BARRIERFSYNC. So we need to access
        // it directly through libc.
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos", target_os = "watchos"))]
        {
            let ret = unsafe {
                libc::fcntl(self.as_raw_fd(), libc::F_BARRIERFSYNC)
            };

            if ret == -1 {
                Err(io::Error::last_os_error())
            } else { Ok(()) }
        }

        // Everywhere else can just do a normal fsync.
        #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "tvos", target_os = "watchos")))]
        File::sync_data(self)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }
}

// *** Testing filesystem. This is used to make writing tests easier, and enable filesystem error
// injection.

#[cfg(test)]
pub mod test {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::mem::replace;
    use std::path::PathBuf;
    use std::rc::Rc;
    use rand::prelude::*;
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    enum UncommittedEntry {
        Barrier,
        Write(usize, Vec<u8>),
    }

    /// Testing files here have 2 uses:
    ///
    /// 1. Its used to test saving and loading without needing to actually create and destroy files
    ///    on the real filesystem. This makes the tests guaranteed to be repeatable without needing
    ///    to remember to delete the created files.
    /// 2. The testing filesystem can simulate power failures or hardware failure during writing.
    ///    The FS code should just deal with that, and not lose any uncommitted data.
    #[derive(Debug, Clone, Default)]
    pub struct TestFile {
        /// Writes that have been committed to disk.
        committed: Vec<u8>,

        /// Uncommitted (unflushed) writes. (offset, written block).
        ///
        /// usize is fine here because we won't have more than usize bytes in our
        /// fake in-memory file.
        uncommitted: Vec<UncommittedEntry>,

        // rng, per_write_crash_chance.
        failure_rng: Option<(SmallRng, f64)>,
    }

    impl TestFile {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn new_faulty(seed: u64, failure_rate: f64) -> Self {
            TestFile {
                committed: vec![],
                uncommitted: vec![],
                failure_rng: Some((SmallRng::seed_from_u64(seed), failure_rate)),
            }
        }

        fn contents(&mut self) -> &[u8] {
            self.sync_safe();
            &self.committed
        }

        fn sync_safe(&mut self) {
            // I'm pulling the uncommitted writes out like this because it works around a borrow
            // checker problem. Its also fine because we'll need to clear the uncommitted writes
            // anyway.
            let writes = replace(&mut self.uncommitted, vec![]);
            for e in writes {
                let UncommittedEntry::Write(offset, write_data) = e else { continue };

                let end = offset + write_data.len();
                if self.committed.len() < end {
                    self.committed.resize(end, 0);
                }
                self.committed[offset..end].copy_from_slice(&write_data);
            }
        }

        fn sync_and_maybe_crash(&mut self) -> io::Result<()> {
            let Some((rng, crash)) = self.failure_rng.as_mut() else {
                self.sync_safe();
                return Ok(());
            };
            let per_write_crash_chance = *crash;

            // I'm pulling the uncommitted writes out like this because it works around a borrow
            // checker problem. Its also fine because we'll need to clear the uncommitted writes
            // anyway.
            let writes = replace(&mut self.uncommitted, vec![]);

            for block in writes.split(|e| *e == UncommittedEntry::Barrier) {
                if block.is_empty() { continue; }

                // For each block of writes, decide if we're going to crash.
                let crash_here = if per_write_crash_chance > 0.0 {
                    !rng.gen_bool((1.0 - per_write_crash_chance).powi(block.len() as i32))
                } else { false };

                for e in block {
                    let UncommittedEntry::Write(offset, write_data) = e else { panic!("Unreachable") };
                    if write_data.is_empty() { continue; }

                    let mut offset = *offset;
                    let mut data = &write_data[..];

                    if crash_here && rng.gen_bool(0.2) {
                        if rng.gen_bool(0.8) {
                            // Skip this write entirely.
                            continue;
                        } else {
                            // Just write some random chunk of the data.
                            let skip_start = rng.gen_range(0..data.len());
                            let skip_end = if skip_start < data.len() {
                                rng.gen_range(0..data.len() - skip_start)
                            } else { 0 };

                            offset += skip_start;
                            data = &data[skip_start .. data.len() - skip_end];
                        }
                    }

                    let end = offset + data.len();
                    if self.committed.len() < end {
                        self.committed.resize(end, 0);
                    }
                    self.committed[offset..end].copy_from_slice(data);
                }

                if crash_here {
                    return Err(io::Error::from(ErrorKind::Other));
                }
            }

            Ok(())
        }
    }

    impl DTFile for TestFile {
        fn stream_len(&mut self) -> io::Result<u64> {
            Ok(self.committed.len() as u64)
        }

        fn write_all_at(&mut self, write_data: &[u8], offset: u64) -> io::Result<()> {
            // Just add the uncommitted data to the queue.
            self.uncommitted
                .push(UncommittedEntry::Write(offset as usize, write_data.into()));

            Ok(())
        }

        fn read_all_at(&mut self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
            // Linux guarantees that if you write then immediately read, you'll see your written
            // data. So read_all_at() here will return data from uncommitted blocks too.
            buffer.fill(0);
            let mut last_read_pos = 0;

            let start_req = offset as usize;
            let end_req = start_req + buffer.len();

            // First read from committed data and overwrite with anything we find in uncommitted
            // data.
            if start_req < self.committed.len() {
                let end_committed = usize::min(self.committed.len(), end_req);
                buffer[..end_committed - start_req].copy_from_slice(&self.committed[start_req..end_committed]);
                last_read_pos = end_committed;
            }

            for e in self.uncommitted.iter() {
                let UncommittedEntry::Write(offset, data) = e else { continue };
                // We don't care about barriers.

                // If there's any overlap, copy it in.
                let slice_start = *offset;
                let slice_end = slice_start + data.len();
                if slice_start < end_req && slice_end > start_req {
                    // There's overlap. s and e are "absolute" file offsets.
                    let s = slice_start.max(start_req);
                    let e = slice_end.min(end_req);
                    buffer[s - start_req..e - start_req].copy_from_slice(&data[s - slice_start..e - slice_start]);
                    last_read_pos = last_read_pos.max(e);
                }
            }

            if last_read_pos < end_req {
                Err(io::Error::from(ErrorKind::UnexpectedEof))
            } else {
                Ok(())
            }
        }

        fn write_barrier(&mut self) -> io::Result<()> {
            self.uncommitted.push(UncommittedEntry::Barrier);
            Ok(())
        }

        fn sync_data(&mut self) -> io::Result<()> {
            self.sync_and_maybe_crash()
        }
    }

    #[test]
    fn smoke_test_testing_filesystem() {
        let mut file = TestFile::new();

        // You should read your own writes even if the contents haven't been flushed.
        file.write_all_at(&[1,2,3], 0).unwrap();

        let mut buf = [0u8; 3];
        file.read_all_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, &[1,2,3]);
        file.sync_data().unwrap();
        dbg!(&file);

        // And open it again - we should see the new contents.
        // let mut file = fs.open("yo").unwrap();
        let mut buf = [0u8; 3];
        file.read_all_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, &[1,2,3]);
    }

    #[test]
    fn write_until_crash() {
        for seed in 0..100 {
            let mut file = TestFile::new_faulty(seed, 0.003);

            for i in 0..255 {
                // Write 2 bytes at a time to exercise it a bit more.
                file.write_all_at(&[i, i], i as u64 * 2).unwrap();
            }

            let succeeded = file.sync_and_maybe_crash().is_ok();
            // dbg!(succeeded);

            let resulting_data = file.contents();
            for (pos, i) in resulting_data.into_iter().enumerate() {
                if succeeded {
                    assert_eq!(*i, (pos / 2) as u8);
                } else {
                    assert!(*i == 0 || *i == (pos / 2) as u8);
                }
            }
        }
    }
}

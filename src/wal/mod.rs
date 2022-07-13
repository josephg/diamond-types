use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use crate::Time;

pub(crate) mod wal;
// pub(crate) mod wal_encoding;

#[derive(Debug)]
#[non_exhaustive]
pub enum WALError {
    InvalidHeader,
    UnexpectedEOF,
    ChecksumMismatch,
    IO(io::Error),
}

#[derive(Debug)]
pub(crate) struct WriteAheadLog {
    file: File,

    // The WAL just stores changes in order. We don't need to worry about complex time DAG
    // traversal.
    next_version: Time,
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

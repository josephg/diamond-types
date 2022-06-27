use std::path::Path;
use crate::{CRDTSpan, KVPair, LocalVersion, NewOpLog, Time};
use crate::encoding::agent_assignment::{AgentMapping, encode_agent_assignment};
use crate::encoding::parents::encode_parents;
use crate::storage::wal::{WALError, WriteAheadLog};

struct WALChunks {
    wal: WriteAheadLog,

    // The WAL just stores changes in order. We don't need to worry about complex time DAG
    // traversal.
    next_version: Time,
}

impl WALChunks {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        Ok(Self {
            wal: WriteAheadLog::open(path, |chunk| {
                dbg!(chunk);
                Ok(())
            })?,
            next_version: 0
        })
    }

    // fn parse_chunk(chunk: &[u8]) -> Result<(), WALError> {
    //     dbg!(chunk);
    //     Ok(())
    // }

    pub fn flush(&mut self, oplog: &NewOpLog) -> Result<(), WALError> {
        let next = oplog.len();

        if next == self.next_version {
            // Nothing to do!
            return Ok(());
        }

        // Data to store:
        //
        // - Agent assignment
        // - Parents

        let range = (self.next_version..next).into();
        self.wal.write_chunk(|buf| {
            let start = buf.len();

            let iter = oplog.client_with_localtime
                .iter_range_packed(range)
                .map(|KVPair(_, span)| span);
            let mut map = AgentMapping::new(&oplog.client_data);
            encode_agent_assignment(iter, buf, oplog, &mut map);

            let hist_iter = oplog.history.entries.iter_range_map_packed(range, |h| h.into());

            encode_parents(hist_iter, buf, &mut map, oplog);

            buf.extend_from_slice(&map.into_output());

            dbg!(&buf[start..]);
            Ok(())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::new_oplog::Primitive::I64;
    use crate::new_oplog::ROOT_MAP;
    use crate::NewOpLog;
    use crate::path::PathComponent;
    use crate::path::PathComponent::Key;
    use crate::storage::wal::WALError;
    use crate::storage::wal_encoding::WALChunks;

    #[test]
    fn simple_encode_test() {
        let mut oplog = NewOpLog::new();
        // dbg!(&oplog);

        let seph = oplog.get_or_create_agent_id("seph");
        let mike = oplog.get_or_create_agent_id("mike");
        let mut v = 0;

        oplog.set_at_path(seph, &[Key("name")], I64(1));
        oplog.set_at_path(seph, &[Key("name")], I64(2));
        oplog.set_at_path(seph, &[Key("name")], I64(3));
        oplog.set_at_path(mike, &[Key("name")], I64(3));
        // dbg!(oplog.checkout(&oplog.version));

        // dbg!(&oplog);
        oplog.dbg_check(true);

        let mut wal = WALChunks::open("test.wal").unwrap();
        wal.flush(&oplog).unwrap();
    }
}
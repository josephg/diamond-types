use std::path::Path;
use bumpalo::Bump;
use crate::{CRDTKind, CRDTSpan, KVPair, LocalVersion, NewOpLog, Primitive, Time, Value};
use crate::encoding::agent_assignment::{AgentMappingEnc, encode_agent_assignment};
use crate::encoding::ChunkType;
use crate::encoding::op_contents::encode_op_contents;
use crate::encoding::parents::encode_parents;
use crate::encoding::tools::push_chunk;
use crate::storage::wal::{WALError, WriteAheadLogRaw};
use smartstring::alias::String as SmartString;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum WALValue {
    Primitive(Primitive),
    NewCRDT(CRDTKind),
    Deleted,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SetOp {
    pub time: Time,
    pub crdt_id: Time,
    pub key: Option<SmartString>,
    pub new_value: WALValue,
}


#[derive(Debug)]
pub(crate) struct WriteAheadLog {
    wal: WriteAheadLogRaw,

    // The WAL just stores changes in order. We don't need to worry about complex time DAG
    // traversal.
    next_version: Time,

    pub(crate) unwritten_values: Vec<SetOp>,
}

impl WriteAheadLog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WALError> {
        Ok(Self {
            wal: WriteAheadLogRaw::open(path, |chunk| {
                // dbg!(chunk);
                Ok(())
            })?,
            next_version: 0,
            unwritten_values: vec![]
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
        self.wal.write_chunk(|bump, buf| {
            let start = buf.len();

            let mut map = AgentMappingEnc::new(&oplog.cg.client_data);

            let iter = oplog.cg.client_with_localtime
                .iter_range_packed(range)
                .map(|KVPair(_, span)| span);
            let aa = encode_agent_assignment(bump, iter, &oplog.cg.client_data, &mut map);

            let hist_iter = oplog.cg.history.iter_range(range);
            let parents = encode_parents(bump, hist_iter, &mut map, &oplog.cg);

            // buf.extend_from_slice(&map.into_output());

            // let first_set_idx = oplog.register_set_operations
            //     .binary_search_by_key(&self.next_version, |e| e.0)
            //     .unwrap_or_else(|idx| idx);
            //
            // let op_contents = if first_set_idx < oplog.register_set_operations.len() {
            //     let iter = oplog.register_set_operations[first_set_idx..]
            //         .iter()
            //         .map(|(_, value)| value);
            //     Some(encode_op_contents(bump, iter, oplog))
            // } else { None };

            // dbg!(map.into_output());
            // push_chunk(buf, ChunkType::AgentNames, &map.into_output());
            push_chunk(buf, ChunkType::OpVersions, &aa);
            push_chunk(buf, ChunkType::OpParents, &parents);
            // if let Some(op_contents) = op_contents {
            //     push_chunk(buf, ChunkType::SetContent, &op_contents);
            // }
            dbg!(&buf[start..]);
            dbg!(buf.len() - start);

            Ok(())
        })?;

        self.next_version = next;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use crate::new_oplog::Primitive::I64;
//     use crate::new_oplog::{Primitive, ROOT_MAP};
//     use crate::NewOpLog;
//     use crate::path::PathComponent;
//     use crate::path::PathComponent::Key;
//     use crate::storage::wal::WALError;
//     use crate::storage::wal_encoding::WriteAheadLog;
//
//     #[test]
//     fn simple_encode_test() {
//         let mut oplog = NewOpLog::new();
//         let mut wal = WriteAheadLog::open("test.wal").unwrap();
//         // wal.flush(&oplog).unwrap(); // Should do nothing!
//
//         // dbg!(&oplog);
//
//         let seph = oplog.get_or_create_agent_id("seph");
//         let mike = oplog.get_or_create_agent_id("mike");
//         let mut v = 0;
//
//         oplog.set_at_path(seph, &[Key("name")], I64(1));
//         let t = oplog.set_at_path(seph, &[Key("name")], I64(2));
//         // wal.flush(&oplog).unwrap();
//         oplog.set_at_path(seph, &[Key("name")], I64(3));
//         oplog.set_at_path(mike, &[Key("name")], I64(4));
//         wal.flush(&oplog).unwrap();
//
//         let item = oplog.get_or_create_map_child(ROOT_MAP, "child".into());
//         oplog.append_set(mike, &[t], item, Primitive::I64(321));
//         // dbg!(oplog.checkout(&oplog.version));
//
//         // dbg!(&oplog);
//         oplog.dbg_check(true);
//
//         wal.flush(&oplog).unwrap();
//     }
// }
//! This file contains some wrappers to interact with operations publicly.

use crate::causalgraph::agent_assignment::remote_ids::{RemoteFrontier, RemoteVersion, RemoteVersionOwned};
use crate::{CollectionOp, CreateValue, LV, Op, OpContents, OpLog, ROOT_CRDT_ID, ROOT_CRDT_ID_AV};
use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use rle::HasLength;
use crate::causalgraph::agent_span::{AgentSpan, AgentVersion};
use crate::list::operation::TextOperation;
use crate::rle::KVPair;
use crate::simpledb::SimpleDatabase;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ExtOpContents<'a> {
    RegisterSet(CreateValue),
    MapSet(SmartString, CreateValue),
    MapDelete(SmartString),
    CollectionInsert(CreateValue),
    #[cfg_attr(feature = "serde", serde(borrow))]
    CollectionRemove(RemoteVersion<'a>),

    // Gross that we don't separate insert / remove here.
    Text(TextOperation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ExtOp<'a> {
    #[cfg_attr(feature = "serde", serde(borrow))]
    pub target: RemoteVersion<'a>,
    #[cfg_attr(feature = "serde", serde(borrow))]
    pub parents: RemoteFrontier<'a>,
    #[cfg_attr(feature = "serde", serde(borrow))]
    pub version: RemoteVersion<'a>, // Start version big ops
    #[cfg_attr(feature = "serde", serde(borrow))]
    pub contents: ExtOpContents<'a>
}

impl OpLog {
    fn target_to_rv(&self, target: LV) -> RemoteVersion<'_> {
        if target == ROOT_CRDT_ID {
            RemoteVersion("ROOT", 0)
        } else {
            self.cg.agent_assignment.local_to_remote_version(target)
        }
    }

    fn op_contents_to_ext_op(&self, op: OpContents) -> ExtOpContents<'_> {
        match op {
            OpContents::RegisterSet(val) => ExtOpContents::RegisterSet(val),
            OpContents::MapSet(key, val) => ExtOpContents::MapSet(key, val),
            OpContents::MapDelete(key) => ExtOpContents::MapDelete(key),
            OpContents::Collection(CollectionOp::Insert(val)) => ExtOpContents::CollectionInsert(val),
            OpContents::Collection(CollectionOp::Remove(lv)) => ExtOpContents::CollectionRemove(
                self.cg.agent_assignment.local_to_remote_version(lv)
            ),
            OpContents::Text(metrics) => ExtOpContents::Text(self.metrics_to_op(&metrics))
        }
    }

    pub fn ext_ops_since(&self, v: &[LV]) -> Vec<ExtOp> {
        let mut result = vec![];

        for walk in self.cg.parents.optimized_txns_between(v, self.cg.version.as_ref()) {
            for KVPair(lv, op) in self.uncommitted_ops.ops.iter_range_ctx(walk.consume, &self.uncommitted_ops.list_ctx) {
                result.push(ExtOp {
                    target: self.target_to_rv(op.target_id),
                    parents: self.cg.agent_assignment.local_to_remote_frontier(self.cg.parents.parents_at_time(lv).as_ref()),
                    version: self.cg.agent_assignment.local_to_remote_version(lv),
                    contents: self.op_contents_to_ext_op(op.contents)
                });

                // result.push(ops.1);
            }
        }

        result
    }

    fn ext_contents_to_local(&mut self, ext: ExtOpContents) -> OpContents {
        match ext {
            ExtOpContents::RegisterSet(val) => OpContents::RegisterSet(val),
            ExtOpContents::MapSet(key, val) => OpContents::MapSet(key, val),
            ExtOpContents::MapDelete(key) => OpContents::MapDelete(key),
            ExtOpContents::CollectionInsert(val) => OpContents::Collection(CollectionOp::Insert(val)),
            ExtOpContents::CollectionRemove(rv) => OpContents::Collection(CollectionOp::Remove(
                self.cg.agent_assignment.remote_to_local_version(rv)
            )),
            ExtOpContents::Text(op) => OpContents::Text(self.text_op_to_metrics(&op))
        }
    }

    fn target_rv_to_av(&self, target: RemoteVersion) -> AgentVersion {
        if target.0 == "ROOT" {
            ROOT_CRDT_ID_AV
        } else {
            self.cg.agent_assignment.remote_to_agent_version_known(target)
        }
    }

    pub fn merge_ext_ops(&mut self, ops: Vec<ExtOp>) {
        for op in ops {
            let ExtOp {
                target, parents, version, contents
            } = op;

            let parents_local = self.cg.agent_assignment.remote_to_local_frontier(parents.into_iter());
            // let target_local = self.cg.remote_to_local_version(target);
            let version_local = self.cg.agent_assignment.remote_to_agent_version_unknown(version);
            let target_local = self.target_rv_to_av(target);
            let contents = self.ext_contents_to_local(contents);

            self.push_remote_op(parents_local.as_ref(), version_local.into(), target_local, contents);
        }
    }
}

impl SimpleDatabase {
    pub fn merge_ext_ops(&mut self, ops: Vec<ExtOp>) {
        for op in ops {
            let ExtOp {
                target, parents, version, contents
            } = op;

            let parents_local = self.oplog.cg.agent_assignment.remote_to_local_frontier(parents.into_iter());
            // let target_local = self.cg.remote_to_local_version(target);
            let version_local = self.oplog.cg.agent_assignment.remote_to_agent_version_unknown(version);
            let target_local = self.oplog.target_rv_to_av(target);
            let contents = self.oplog.ext_contents_to_local(contents);

            let version_span = AgentSpan {
                agent: version_local.0,
                seq_range: (version_local.1 .. version_local.1 + contents.len()).into()
            };
            self.apply_remote_op(parents_local.as_ref(), version_span, target_local, contents);
            // self.push_remote_op(parents_local.as_ref(), version_local.into(), target_local, contents);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CRDTKind, OpLog};
    use crate::ROOT_CRDT_ID;
    use crate::simpledb::SimpleDatabase;
    use crate::Primitive::*;
    use crate::CreateValue::*;

    #[test]
    fn external_ops_merge() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");
        db.map_set(seph, ROOT_CRDT_ID, "name", Primitive(Str("seph".into())));

        let inner = db.map_set(seph, ROOT_CRDT_ID, "facts", NewCRDT(CRDTKind::Map));
        db.map_set(seph, inner, "cool", Primitive(I64(1)));

        let inner_set = db.map_set(seph, ROOT_CRDT_ID, "set stuff", NewCRDT(CRDTKind::Collection));
        let inner_map = db.collection_insert(seph, inner_set, NewCRDT(CRDTKind::Map));
        db.map_set(seph, inner_map, "whoa", Primitive(I64(3214)));

        let ops_ext = db.oplog.ext_ops_since(&[]);

        // for op in &ops_ext {
        //     println!("{}", serde_json::to_string(op).unwrap());
        // }
        // println!("{}", serde_json::to_string(&ops_ext).unwrap());
        // println!("LEN {}", serde_json::to_string(&ops_ext).unwrap().len());


        // let ops_ext2 = db.oplog.ext_ops_since(&[4]);
        // dbg!(&ops_ext2);

        // let mut oplog2 = OpLog::new_mem();
        // oplog2.merge_ext_ops(ops_ext);
        //
        // dbg!(&db.oplog);
        // dbg!(&oplog2);

        let mut db2 = SimpleDatabase::new_mem();
        db2.merge_ext_ops(ops_ext);
        assert_eq!(db.get_recursive(), db2.get_recursive());
        // dbg!(db2.get_recursive());

        // println!("{}", serde_json::to_string(&db2.get_recursive()).unwrap());
        // println!("LEN {}", serde_json::to_string(&db2.get_recursive()).unwrap().len())
    }

    #[test]
    #[ignore] // Text is not implemented in branch.
    fn edit_text() {
        let mut db = SimpleDatabase::new_mem();
        let seph = db.get_or_create_agent_id("seph");
        let text = db.map_set(seph, ROOT_CRDT_ID, "name", NewCRDT(CRDTKind::Text));

        db.text_insert(seph, text, 0, "Oh hai".into());
        // db.text_remove(seph, text, (0..3).into());

        dbg!(db.get_recursive());

        let ops_ext = db.oplog.ext_ops_since(&[]);
        // dbg!(&ops_ext);

        // println!("{}", serde_json::to_string(&ops_ext).unwrap());


        let mut db2 = SimpleDatabase::new_mem();
        db2.merge_ext_ops(ops_ext);
        assert_eq!(db.get_recursive(), db2.get_recursive());
    }
}
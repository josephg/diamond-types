use std::collections::BTreeMap;
use jumprope::JumpRopeBuf;
use smallvec::{SmallVec, smallvec};
use crate::{AgentId, CausalGraph, CRDTKind, CreateValue, DTRange, Frontier, LV, Primitive, ROOT_CRDT_ID, SnapshotValue};
use smartstring::alias::String as SmartString;
use rle::HasLength;
use crate::branch::DTValue;
use crate::list::op_iter::{OpIterFast, OpMetricsIter};
use crate::list::op_metrics::{ListOperationCtx, ListOpMetrics};
use crate::list::operation::TextOperation;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};

// type Pair<T> = (LV, T);
type ValPair = (LV, CreateValue);
// type RawPair<'a, T> = (RemoteVersion<'a>, T);
type LVKey = LV;


#[derive(Debug, Clone, Default)]
struct RegisterInfo {
    // I bet there's a clever way to use RLE to optimize this.
    ops: Vec<ValPair>,

    // Indexes into ops.
    supremum: SmallVec<[usize; 2]>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TextInfo {
    pub ctx: ListOperationCtx,
    pub ops: RleVec<KVPair<ListOpMetrics>>
}

impl TextInfo {
    pub(crate) fn iter_metrics_range(&self, range: DTRange) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, range)
    }
    pub(crate) fn iter_metrics(&self) -> OpMetricsIter {
        OpMetricsIter::new(&self.ops, &self.ctx, (0..self.ops.end()).into())
    }

    pub(crate) fn iter_fast(&self) -> OpIterFast {
        self.iter_metrics().into()
    }

    pub fn iter(&self) -> impl Iterator<Item = TextOperation> + '_ {
        self.iter_fast().map(|pair| (pair.0.1, pair.1).into())
    }

    pub(crate) fn push_op(&mut self, op: TextOperation, range: DTRange) {
        debug_assert_eq!(range.len(), op.len());

        let content_pos = op.content.as_ref().map(|content| {
            self.ctx.push_str(op.kind, content)
        });

        self.ops.push(KVPair(range.start, ListOpMetrics {
            loc: op.loc,
            kind: op.kind,
            content_pos
        }));
    }
}


#[derive(Debug, Clone, Default)]
struct ExperimentalOpLog {
    cg: CausalGraph,

    // TODO: Vec -> SmallVec.
    registers: BTreeMap<LVKey, RegisterInfo>,

    // Information about whether the map still exists!
    // maps: BTreeMap<LVKey, MapInfo>,

    map_keys: BTreeMap<(LVKey, SmartString), RegisterInfo>,
    texts: BTreeMap<LVKey, TextInfo>,

    // A different index for each data set, or one index with an enum?
    map_index: BTreeMap<LV, (LVKey, SmartString)>,
    text_indexes: BTreeMap<LV, LVKey>,
}

// #[derive(Debug, Clone, Default)]
// struct ExperimentalBranch {
//     v: Frontier,
//
//     registers: BTreeMap<LVKey, SmallVec<[LV; 2]>>,
//     maps: BTreeMap<(LVKey, SmartString), SmallVec<[LV; 2]>>,
//     texts: BTreeMap<LVKey, JumpRopeBuf>,
// }

#[derive(Debug, Clone, PartialEq, Eq)]
enum RegisterValue {
    Primitive(Primitive),
    OwnedCRDT(CRDTKind, LVKey),
}

impl ExperimentalOpLog {
    pub fn new() -> Self {
        Default::default()
    }

    fn create_child_crdt(&mut self, v: LV, kind: CRDTKind) {
        match kind {
            CRDTKind::Map => {}
            CRDTKind::Register => {}
            CRDTKind::Collection => {}
            CRDTKind::Text => {
                self.texts.insert(v, TextInfo::default());
            }
        }
    }


    pub fn push_map_set(&mut self, agent: AgentId, crdt: LVKey, key: &str, value: CreateValue) -> LV {
        let v = self.cg.assign_local_op(agent, 1).start;
        if let CreateValue::NewCRDT(kind) = value {
            self.create_child_crdt(v, kind);
        }

        let mut entry = self.map_keys.entry((crdt, key.into()))
            .or_default();

        let new_idx = entry.ops.len();

        // Remove the old supremum from the index
        for idx in &entry.supremum {
            self.map_index.remove(&entry.ops[*idx].0);
        }

        entry.supremum = smallvec![new_idx];
        entry.ops.push((v, value));

        self.map_index.insert(v, (crdt, key.into()));
        v
    }

    pub fn push_text_op(&mut self, agent: AgentId, crdt: LVKey, op: TextOperation) {
        let v_range = self.cg.assign_local_op(agent, op.len());

        let entry = self.texts.get_mut(&crdt).unwrap();

        // Remove it from the index
        if let Some(last_op) = entry.ops.last() {
            let old_index_item = self.text_indexes.remove(&last_op.last());
            assert!(old_index_item.is_some());
        }

        entry.push_op(op, v_range);

        // And add it back to the index.
        self.text_indexes.insert(v_range.last(), crdt);
    }


    fn create_to_snapshot(v: LV, create: &CreateValue) -> RegisterValue {
        match create {
            CreateValue::Primitive(p) => RegisterValue::Primitive(p.clone()),
            CreateValue::NewCRDT(kind) => RegisterValue::OwnedCRDT(*kind, v)
        }
    }

    fn resolve_mv(&self, reg: &RegisterInfo) -> RegisterValue {
        let s = match reg.supremum.len() {
            0 => panic!("Internal consistency violation"),
            1 => reg.supremum[0],
            _ => {
                reg.supremum.iter()
                    .map(|s| (*s, self.cg.agent_assignment.lv_to_agent_version(reg.ops[*s].0)))
                    .max_by(|(_, a), (_, b)| {
                        self.cg.agent_assignment.tie_break_crdt_versions(*a, *b)
                    })
                    .unwrap().0
            }
        };

        let (v, value) = &reg.ops[s];
        Self::create_to_snapshot(*v, value)
    }

    pub fn checkout_text(&self, crdt: LVKey) -> JumpRopeBuf {
        let info = self.texts.get(&crdt).unwrap();

        let mut result = JumpRopeBuf::new();
        info.merge_into(&mut result, &self.cg, &[], self.cg.version.as_ref());
        result
    }

    pub fn checkout_map(&self, crdt: LVKey) -> BTreeMap<SmartString, Box<DTValue>> {
        let empty_str: SmartString = "".into();
        // dbg!((crdt, empty_str.clone())..(crdt, empty_str));
        let iter = if crdt == ROOT_CRDT_ID {
            // For the root CRDT we can't use the crdt+1 trick because the range wraps around.
            self.map_keys.range((crdt, empty_str)..)
        } else {
            self.map_keys.range((crdt, empty_str.clone())..(crdt + 1, empty_str))
        };

        iter.map(|((_, key), info)| {
            let inner = match self.resolve_mv(info) {
                RegisterValue::Primitive(p) => DTValue::Primitive(p),
                RegisterValue::OwnedCRDT(kind, child_crdt) => {
                    match kind {
                        CRDTKind::Map => DTValue::Map(self.checkout_map(child_crdt)),
                        CRDTKind::Text => DTValue::Text(self.checkout_text(child_crdt).to_string()),
                        _ => unimplemented!(),
                        // CRDTKind::Register => {}
                        // CRDTKind::Collection => {}
                        // CRDTKind::Text => {}
                    }
                }
            };
            (key.clone(), Box::new(inner))
        }).collect()
    }

    pub fn checkout(&self) -> BTreeMap<SmartString, Box<DTValue>> {
        self.checkout_map(ROOT_CRDT_ID)
    }
}

#[cfg(test)]
mod tests {
    use crate::experiments::ExperimentalOpLog;
    use crate::{CRDTKind, CreateValue, Primitive, ROOT_CRDT_ID};
    use crate::list::operation::TextOperation;

    #[test]
    fn smoke() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.push_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        oplog.push_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(321)));

        dbg!(&oplog);
    }

    #[test]
    fn text() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        let text = oplog.push_map_set(seph, ROOT_CRDT_ID, "textitem", CreateValue::NewCRDT(CRDTKind::Text));

        oplog.push_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));
        oplog.push_text_op(seph, text, TextOperation::new_delete(0..3));

        // dbg!(&oplog);

        assert_eq!(oplog.checkout_text(text).to_string(), "hai!");

        // dbg!(oplog.checkout());
    }

    #[test]
    fn checkout() {
        let mut oplog = ExperimentalOpLog::new();

        let seph = oplog.cg.get_or_create_agent_id("seph");
        oplog.push_map_set(seph, ROOT_CRDT_ID, "hi", CreateValue::Primitive(Primitive::I64(123)));
        let map = oplog.push_map_set(seph, ROOT_CRDT_ID, "yo", CreateValue::NewCRDT(CRDTKind::Map));
        oplog.push_map_set(seph, map, "yo", CreateValue::Primitive(Primitive::Str("blah".into())));

        dbg!(oplog.checkout());

    }
}
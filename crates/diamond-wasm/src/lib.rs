mod utils;

use smallvec::SmallVec;
use wasm_bindgen::prelude::*;
// use serde_wasm_bindgen::Serializer;
// use serde::{Serialize};
use diamond_types::{AgentId, ROOT_TIME};
use diamond_types::list::{ListCRDT, Time, Branch as DTBranch, OpLog as DTOpLog};
use diamond_types::list::encoding::EncodeOptions;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub struct Branch(DTBranch);

#[wasm_bindgen]
impl Branch {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        utils::set_panic_hook();

        Self(DTBranch::new())
    }

    #[wasm_bindgen]
    pub fn all(oplog: &OpLog) -> Self {
        let mut result = Self::new();
        result.0.merge(&oplog.inner, &oplog.inner.get_frontier());
        result
    }

    #[wasm_bindgen]
    pub fn get(&self) -> String {
        self.0.content.to_string()
    }

    /// Merge in from some named point in time
    #[wasm_bindgen]
    pub fn merge(&mut self, ops: &OpLog, branch: Time) {
        self.0.merge(&ops.inner, &[branch]);
    }

    #[wasm_bindgen(js_name = getLocalFrontier)]
    pub fn get_local_frontier(&self) -> Box<[Time]> {
        self.0.frontier.iter().copied().collect::<Box<[Time]>>()
    }
}

#[wasm_bindgen]
pub struct OpLog {
    inner: DTOpLog,
    agent_id: AgentId,
}

fn map_parents(parents_in: &[isize]) -> SmallVec<[Time; 4]> {
    parents_in
        .iter()
        .map(|p| if *p < 0 { ROOT_TIME } else { *p as usize })
        .collect()
}

#[wasm_bindgen]
impl OpLog {
    #[wasm_bindgen(constructor)]
    pub fn new(agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = DTOpLog::new();
        let name_str = agent_name.as_ref().map_or("seph", |s| s.as_str());
        let agent_id = inner.get_or_create_agent_id(name_str);

        Self { inner, agent_id }
    }

    #[wasm_bindgen(js_name = ins)]
    pub fn push_insert(&mut self, pos: usize, content: &str, parents_in: Option<Box<[isize]>>) -> usize {
        let parents = parents_in.map_or_else(|| {
            // Its gross here - I'm converting the frontier into a smallvec then immediately
            // converting it to a slice again :p
            self.inner.get_frontier().into()
        }, |p| map_parents(&p));
        self.inner.push_insert_at(self.agent_id, &parents, pos, content)
    }

    #[wasm_bindgen(js_name = del)]
    pub fn push_delete(&mut self, pos: usize, len: usize, parents_in: Option<Box<[isize]>>) -> usize {
        let parents = parents_in.map_or_else(|| {
            // And here :p
            self.inner.get_frontier().into()
        }, |p| map_parents(&p));
        self.inner.push_delete_at(self.agent_id, &parents, pos, len)
    }

    #[wasm_bindgen(js_name = toArray)]
    pub fn to_arr(&self) -> Result<JsValue, JsValue> {
        let ops = self.inner.iter().collect::<Vec<_>>();

        serde_wasm_bindgen::to_value(&ops)
                .map_err(|err| err.into())
    }

    #[wasm_bindgen(js_name = txns)]
    pub fn to_txn_arr(&self) -> Result<JsValue, JsValue> {
        let txns = self.inner.iter_history().collect::<Vec<_>>();

        serde_wasm_bindgen::to_value(&txns)
                .map_err(|err| err.into())
    }

    #[wasm_bindgen(js_name = getLocalFrontier)]
    pub fn get_local_frontier(&self) -> Box<[Time]> {
        self.inner.get_frontier().iter().copied().collect::<Box<[Time]>>()
    }

    #[wasm_bindgen(js_name = getFrontier)]
    pub fn get_frontier(&self) -> Result<JsValue, JsValue> {
        // self.inner.get_frontier().iter().copied().collect::<Box<[Time]>>()
        let frontier = self.inner.frontier_to_remote_ids(self.inner.get_frontier());
        serde_wasm_bindgen::to_value(&frontier)
            .map_err(|err| err.into())
    }

    // This method adds 15kb to the wasm bundle, or 4kb to the brotli size.
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let bytes = self.inner.encode(EncodeOptions::default());
        bytes
    }

    // This method adds 17kb to the wasm bundle, or 5kb after brotli.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8], agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = DTOpLog::load_from(bytes).unwrap();
        let name_str = agent_name.as_ref().map_or("seph", |s| s.as_str());
        let agent_id = inner.get_or_create_agent_id(name_str);

        Self { inner, agent_id }
    }

    #[wasm_bindgen(js_name = mergeBytes)]
    pub fn merge_bytes(&mut self, bytes: &[u8]) {
        let result = self.inner.merge_data(bytes);
        // TODO: Map this error correctly.
        result.unwrap();
        // result.map_err(|err| err.into())
    }

}

#[wasm_bindgen]
pub struct Doc {
    inner: ListCRDT,
    agent_id: AgentId,
}

#[wasm_bindgen]
impl Doc {
    #[wasm_bindgen(constructor)]
    pub fn new(agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = ListCRDT::new();
        let name_str = agent_name.as_ref().map_or("seph", |s| s.as_str());
        let agent_id = inner.get_or_create_agent_id(name_str);

        Doc { inner, agent_id }
    }

    #[wasm_bindgen]
    pub fn ins(&mut self, pos: usize, content: &str) {
        // let id = self.0.get_or_create_agent_id("seph");
        self.inner.local_insert(self.agent_id, pos, content);
    }

    #[wasm_bindgen]
    pub fn del(&mut self, pos: usize, del_span: usize) {
        self.inner.local_delete(self.agent_id, pos, del_span);
    }

    #[wasm_bindgen]
    pub fn len(&self) -> usize {
        self.inner.branch.len()
    }

    #[wasm_bindgen]
    pub fn is_empty(&self) -> bool { // To make clippy happy.
        self.inner.branch.is_empty()
    }

    #[wasm_bindgen]
    pub fn get(&self) -> String {
        self.inner.branch.content.to_string()
    }

    #[wasm_bindgen]
    pub fn merge(&mut self, branch: &[Time]) {
        self.inner.branch.merge(&self.inner.ops, branch);
    }

    // #[wasm_bindgen]
    // pub fn get_vector_clock(&self) -> Result<JsValue, JsValue> {
    //     serde_wasm_bindgen::to_value(&self.inner.get_vector_clock())
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn get_frontier(&self) -> Result<JsValue, JsValue> {
    //     serde_wasm_bindgen::to_value(&self.inner.get_frontier::<Vec<RemoteId>>())
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn get_next_order(&self) -> Result<JsValue, JsValue> {
    //     serde_wasm_bindgen::to_value(&self.inner.get_next_time())
    //         .map_err(|err| err.into())
    // }

    // #[wasm_bindgen]
    // pub fn get_txn_since(&self, version: JsValue) -> Result<JsValue, JsValue> {
    //     let txns = if version.is_null() || version.is_undefined() {
    //         self.inner.get_all_txns::<Vec<_>>()
    //     } else {
    //         let clock: VectorClock = serde_wasm_bindgen::from_value(version)?;
    //         self.inner.get_all_txns_since::<Vec<_>>(&clock)
    //     };
    //
    //     serde_wasm_bindgen::to_value(&txns)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn positional_ops_since(&self, order: u32) -> Result<JsValue, JsValue> {
    //     let changes = self.inner.positional_changes_since(order);
    //
    //     serde_wasm_bindgen::to_value(&changes)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn traversal_ops_since(&self, order: u32) -> Result<JsValue, JsValue> {
    //     let changes = self.inner.traversal_changes_since(order);
    //
    //     serde_wasm_bindgen::to_value(&changes)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn traversal_ops_flat(&self, order: u32) -> Result<JsValue, JsValue> {
    //     let changes = self.inner.flat_traversal_since(order);
    //
    //     serde_wasm_bindgen::to_value(&changes)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn attributed_patches_since(&self, order: u32) -> Result<JsValue, JsValue> {
    //     let (changes, attr) = self.inner.remote_attr_patches_since(order);
    //
    //     // Using serialize_maps_as_objects here to flatten RemoteSpan into {agent, seq, len}.
    //     (changes, attr)
    //         .serialize(&Serializer::new().serialize_maps_as_objects(true))
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn traversal_ops_since_branch(&self, branch: JsValue) -> Result<JsValue, JsValue> {
    //     let branch: Branch = if branch.is_null() || branch.is_undefined() {
    //         smallvec![ROOT_TIME]
    //     } else {
    //         let b: Vec<RemoteId> = serde_wasm_bindgen::from_value(branch)?;
    //         self.inner.remote_ids_to_branch(&b)
    //     };
    //
    //     let changes = self.inner.traversal_changes_since_branch(branch.as_slice());
    //
    //     serde_wasm_bindgen::to_value(&changes)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn merge_remote_txns(&mut self, txns: JsValue) -> Result<(), JsValue> {
    //     let txns: Vec<RemoteTxn> = serde_wasm_bindgen::from_value(txns)?;
    //     for txn in txns.iter() {
    //         self.inner.apply_remote_txn(txn);
    //     }
    //     Ok(())
    // }
    //
    // #[wasm_bindgen]
    // pub fn ins_at_order(&mut self, pos: usize, content: &str, order: u32, is_left: bool) {
    //     // let id = self.0.get_or_create_agent_id("seph");
    //     self.inner.insert_at_ot_order(self.agent_id, pos, content, order, is_left);
    // }
    //
    // #[wasm_bindgen]
    // pub fn del_at_order(&mut self, pos: usize, del_span: usize, order: u32) {
    //     self.inner.delete_at_ot_order(self.agent_id, pos, del_span, order, true);
    // }
    //
    // #[wasm_bindgen]
    // pub fn get_internal_list_entries(&self) -> Result<JsValue, JsValue> {
    //     let entries = self.inner.get_internal_list_entries().collect::<Vec<_>>();
    //     serde_wasm_bindgen::to_value(&entries)
    //         .map_err(|err| err.into())
    // }
    //
    // #[wasm_bindgen]
    // pub fn as_positional_patch(&self) -> Result<JsValue, JsValue> {
    //     let patch = self.inner.as_external_patch();
    //     serde_wasm_bindgen::to_value(&patch)
    //         .map_err(|err| err.into())
    // }
}

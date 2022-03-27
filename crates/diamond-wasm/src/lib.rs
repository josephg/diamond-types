mod utils;

use wasm_bindgen::prelude::*;
// use serde_wasm_bindgen::Serializer;
// use serde::{Serialize};
use diamond_types::{AgentId, ROOT_TIME};
use diamond_types::list::{ListCRDT, Time, Branch as DTBranch, OpLog as DTOpLog};
use diamond_types::list::encoding::{ENCODE_FULL, ENCODE_PATCH};
use diamond_types::list::operation::Operation;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

type WasmResult<T = JsValue> = Result<T, serde_wasm_bindgen::Error>;

// // The versions we consume from javascript cannot really represent ROOT_TIME. (Actually ROOT_TIME
// // is sort of unnecessary internally in DT anyway). We'll map [ROOT_TIME] -> [].
// fn normalize_local_version(v: &[Time]) -> LocalVersion {
//     v.iter().filter(|t| t != ROOT_TIME).collect()
// }

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
        result.0.merge(&oplog.inner, &oplog.inner.local_version());
        result
    }

    #[wasm_bindgen]
    pub fn get(&self) -> String {
        self.0.content().to_string()
    }

    /// Merge in from some named point in time
    #[wasm_bindgen]
    pub fn merge(&mut self, ops: &OpLog, branch: Option<Time>) {
        if let Some(branch) = branch {
            self.0.merge(&ops.inner, &[branch]);
        } else {
            self.0.merge(&ops.inner, &ops.inner.local_version());
        }
    }

    #[wasm_bindgen(js_name = getLocalVersion)]
    pub fn get_local_frontier(&self) -> Box<[Time]> {
        self.0.local_version().iter().copied().collect::<Box<[Time]>>()
    }
}

// pub fn checkout(&self) -> Branch {
//     let mut result = DTBranch::new();
//     result.merge(&self.0, &self.0.get_frontier());
//     Branch(result)
// }

// trait AsOpLog {
//     fn as_oplog(&self) -> &DTOpLog;
//     fn as_mut_oplog(&mut self) -> &mut DTOpLog;
//
//
// }

pub fn get_ops(oplog: &DTOpLog) -> WasmResult {
    let ops = oplog.iter().collect::<Vec<_>>();
    serde_wasm_bindgen::to_value(&ops)
}

pub fn get_ops_since(oplog: &DTOpLog, frontier: &[Time]) -> WasmResult {
    let ops = oplog.iter_range_since(frontier)
        .collect::<Box<[Operation]>>();
    serde_wasm_bindgen::to_value(&ops)
}

// pub fn to_ar

pub fn to_txn_arr(oplog: &DTOpLog) -> WasmResult {
    let txns = oplog.iter_history().collect::<Vec<_>>();

    serde_wasm_bindgen::to_value(&txns)
}

pub fn get_local_frontier(frontier: &[Time]) -> Box<[Time]> {
    frontier.iter().copied().collect::<Box<[Time]>>()
}

// #[wasm_bindgen]
// pub fn local_to_remote_time(oplog: &DTOpLog, time: Time) -> Result<JsValue, serde_wasm_bindgen::Error> {
//     let remote_time = oplog.time_to_remote_id(time);
//     serde_wasm_bindgen::to_value(&remote_time)
// }
pub fn frontier_to_remote_time(oplog: &DTOpLog, time: &[Time]) -> WasmResult {
    let remote_time = oplog.local_to_remote_version(time);
    serde_wasm_bindgen::to_value(&remote_time)
}

pub fn get_frontier(oplog: &DTOpLog) -> WasmResult {
    // oplog.get_frontier().iter().copied().collect::<Box<[Time]>>()
    let frontier = oplog.local_to_remote_version(oplog.local_version());
    serde_wasm_bindgen::to_value(&frontier)
}

// This method adds 15kb to the wasm bundle, or 4kb to the brotli size.
pub fn to_bytes(oplog: &DTOpLog) -> Vec<u8> {
    let bytes = oplog.encode(ENCODE_FULL);
    bytes
}

pub fn get_patch_since(oplog: &DTOpLog, from_version: &[usize]) -> Vec<u8> {
    // let from_version = map_parents(&version);
    let bytes = oplog.encode_from(ENCODE_PATCH, &from_version);
    bytes
}

pub fn merge_bytes(oplog: &mut DTOpLog, bytes: &[u8]) -> WasmResult {
    match oplog.decode_and_add(bytes) {
        Ok(frontier) => serde_wasm_bindgen::to_value(&frontier),
        Err(e) => {
            let s = format!("Error merging {:?}", e);
            let js: JsValue = s.into();
            Err(js.into())
        }
    }
}

pub fn xf_since(oplog: &DTOpLog, from_version: &[usize]) -> WasmResult {
    let xf = oplog.iter_xf_operations_from(from_version, &oplog.local_version())
        .filter_map(|(_v, op)| op)
        .collect::<Vec<_>>();

    serde_wasm_bindgen::to_value(&xf)
}

pub fn merge_versions(oplog: &DTOpLog, a: &[usize], b: &[usize]) -> Box<[usize]> {
    oplog.version_union(a, b).into_iter().collect()
}

fn unwrap_agentid(agent_id: Option<AgentId>) -> AgentId {
    agent_id.expect_throw("Agent missing. Set agent before modifying oplog.")
}

#[wasm_bindgen]
pub struct OpLog {
    inner: DTOpLog,
    agent_id: Option<AgentId>,
}

#[wasm_bindgen]
impl OpLog {
    #[wasm_bindgen(constructor)]
    pub fn new(agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = DTOpLog::new();
        let agent_id = agent_name.map(|name| {
            inner.get_or_create_agent_id(name.as_str())
        });

        Self { inner, agent_id }
    }

    #[wasm_bindgen(js_name = setAgent)]
    pub fn set_agent(&mut self, agent: &str) {
        self.agent_id = Some(self.inner.get_or_create_agent_id(agent));
    }

    #[wasm_bindgen(js_name = clone)]
    pub fn js_clone(&self) -> Self {
        // We can't trust the .clone() process to preserve the agent_id.
        let name = self.agent_id.map(|id| self.inner.get_agent_name(id));
        let mut new_oplog = self.inner.clone();
        let agent_id = name.map(|name| new_oplog.get_or_create_agent_id(name));

        Self {
            inner: new_oplog,
            agent_id
        }
    }

    #[wasm_bindgen(js_name = ins)]
    pub fn add_insert(&mut self, pos: usize, content: &str, parents_in: Option<Box<[usize]>>) -> usize {
        let parents = parents_in.unwrap_or_else(|| {
            // Its gross here - I'm converting the frontier into a smallvec then immediately
            // converting it to a slice again :p
            self.inner.local_version().into()
        });
        self.inner.add_insert_at(unwrap_agentid(self.agent_id), &parents, pos, content)
    }

    #[wasm_bindgen(js_name = del)]
    pub fn add_delete(&mut self, pos: usize, len: usize, parents_in: Option<Box<[usize]>>) -> usize {
        let parents = parents_in.unwrap_or_else(|| {
            // And here :p
            self.inner.local_version().into()
        });
        self.inner.add_delete_at(unwrap_agentid(self.agent_id), &parents, pos..pos + len)
    }

    // This adds like 70kb of size to the WASM binary.
    // #[wasm_bindgen]
    // pub fn apply_op(&mut self, op: JsValue) -> WasmResult<usize> {
    //     let op_inner: Operation = serde_wasm_bindgen::from_value(op)?;
    //     Ok(self.inner.push(self.agent_id, &[op_inner]))
    // }

    // #[wasm_bindgen]
    // pub fn apply_op(&mut self, isInsert: bool, start: usize, end: usize, fwd: bool, content: &str) -> WasmResult<usize> {
    //     let op_inner: Operation = Operation {
    //         span: (start..end).into(),
    //         tag: if isInsert { InsDelTag::Ins } else { InsDelTag::Del },
    //         content: Some(content.into())
    //     };
    //     Ok(self.inner.push(self.agent_id, &[op_inner]))
    // }

    #[wasm_bindgen]
    pub fn checkout(&self) -> Branch {
        Branch::all(self)
    }

    #[wasm_bindgen(js_name = getOps)]
    pub fn get_ops(&self) -> WasmResult {
        get_ops(&self.inner)
    }

    #[wasm_bindgen(js_name = getOpsSince)]
    pub fn get_ops_since(&self, frontier: &[Time]) -> WasmResult {
        get_ops_since(&self.inner, frontier)
    }

    // pub fn to_ar

    #[wasm_bindgen(js_name = txns)]
    pub fn to_txn_arr(&self) -> WasmResult {
        to_txn_arr(&self.inner)
    }

    #[wasm_bindgen(js_name = getLocalVersion)]
    pub fn get_local_frontier(&self) -> Box<[Time]> {
        get_local_frontier(&self.inner.local_version())
    }

    // #[wasm_bindgen]
    // pub fn local_to_remote_time(&self, time: Time) -> Result<JsValue, serde_wasm_bindgen::Error> {
    //     let remote_time = self.inner.time_to_remote_id(time);
    //     serde_wasm_bindgen::to_value(&remote_time)
    // }
    #[wasm_bindgen(js_name = localToRemoteVersion)]
    pub fn local_to_remote_version(&self, time: &[Time]) -> WasmResult {
        frontier_to_remote_time(&self.inner, time)
    }

    #[wasm_bindgen(js_name = getRemoteVersion)]
    pub fn get_remote_version(&self) -> WasmResult {
        get_frontier(&self.inner)
    }

    // This method adds 15kb to the wasm bundle, or 4kb to the brotli size.
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        to_bytes(&self.inner)
    }

    #[wasm_bindgen(js_name = getPatchSince)]
    pub fn get_patch_since(&self, from_version: &[usize]) -> Vec<u8> {
        get_patch_since(&self.inner, from_version)
    }

    // This method adds 17kb to the wasm bundle, or 5kb after brotli.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8], agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = DTOpLog::load_from(bytes).unwrap();
        let agent_id = agent_name.map(|name| {
            inner.get_or_create_agent_id(name.as_str())
        });

        Self { inner, agent_id }
    }

    #[wasm_bindgen(js_name = mergeBytes)]
    pub fn merge_bytes(&mut self, bytes: &[u8]) -> WasmResult {
        merge_bytes(&mut self.inner, bytes)
    }

    // pub fn xf_since(&self, from_version: &[usize]) -> WasmResult {
    #[wasm_bindgen(js_name = getXF)]
    pub fn get_xf(&self) -> WasmResult {
        xf_since(&self.inner, &[ROOT_TIME])
    }

    #[wasm_bindgen(js_name = getXFSince)]
    pub fn get_xf_since(&self, from_version: &[usize]) -> WasmResult {
        xf_since(&self.inner, from_version)
    }

    #[wasm_bindgen(js_name = mergeVersions)]
    pub fn merge_versions(&self, a: &[usize], b: &[usize]) -> Box<[usize]> {
        merge_versions(&self.inner, a, b)
    }

    // pub fn merge_versions(&self, a: &[usize], b: &[usize]) ->
}

#[wasm_bindgen]
pub struct Doc {
    inner: ListCRDT,
    agent_id: Option<AgentId>,
}


// #[wasm_bindgen]
// extern "C" {
//     // Use `js_namespace` here to bind `console.log(..)` instead of just
//     // `log(..)`
//     #[wasm_bindgen(js_namespace = console)]
//     fn log(s: &str);
// }

#[wasm_bindgen]
impl Doc {
    #[wasm_bindgen(constructor)]
    pub fn new(agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        let mut inner = ListCRDT::new();
        let agent_id = agent_name.map(|name| {
            inner.get_or_create_agent_id(name.as_str())
        });

        Doc { inner, agent_id }
    }

    #[wasm_bindgen]
    pub fn ins(&mut self, pos: usize, content: &str) {
        // let id = self.0.get_or_create_agent_id("seph");
        self.inner.insert(unwrap_agentid(self.agent_id), pos, content);
    }

    #[wasm_bindgen]
    pub fn del(&mut self, pos: usize, del_span: usize) {
        self.inner.delete(unwrap_agentid(self.agent_id), pos .. pos + del_span);
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
        self.inner.branch.content().to_string()
    }

    #[wasm_bindgen]
    pub fn merge(&mut self, branch: &[Time]) {
        self.inner.branch.merge(&self.inner.oplog, branch);
    }

    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        to_bytes(&self.inner.oplog)
    }

    #[wasm_bindgen(js_name = getPatchSince)]
    pub fn get_patch_since(&self, from_version: &[usize]) -> Vec<u8> {
        get_patch_since(&self.inner.oplog, from_version)
    }

    // TODO: Do better error handling here.
    // pub fn from_bytes(bytes: &[u8], agent_name: Option<String>) -> WasmResult<Doc> {
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8], agent_name: Option<String>) -> Self {
        utils::set_panic_hook();

        // let mut inner = ListCRDT::load_from(bytes).map_err(|e| e.into())?;
        let mut inner = ListCRDT::load_from(bytes).unwrap();
        let agent_id = agent_name.map(|name| {
            inner.get_or_create_agent_id(name.as_str())
        });

        Self {
            inner,
            agent_id
        }
    }

    #[wasm_bindgen(js_name = mergeBytes)]
    pub fn merge_bytes(&mut self, bytes: &[u8]) -> WasmResult<Box<[usize]>> {
    // pub fn merge_bytes(&mut self, bytes: &[u8]) -> WasmResult {
        match self.inner.merge_data_and_ff(bytes) {
            Err(e) => {
                let s = format!("Error merging {:?}", e);
                let js: JsValue = s.into();
                Err(js.into())
            },
            Ok(frontier) => Ok(frontier.into_iter().collect())
        }
    }
    // #[wasm_bindgen(js_name = mergeBytes)]
    // pub fn merge_bytes(&mut self, bytes: &[u8]) -> WasmResult {
    //     match self.inner.merge_data_and_ff(bytes) {
    //         Err(e) => {
    //             let s = format!("Error merging {:?}", e);
    //             let js: JsValue = s.into();
    //             Err(js.into())
    //         },
    //         Ok(frontier) => serde_wasm_bindgen::to_value(&frontier),
    //     }
    // }

    // TODO: This is identical to get_ops_since in OpLog (above). Remove duplicate code here.
    #[wasm_bindgen(js_name = getOpsSince)]
    pub fn get_ops_since(&self, frontier: &[Time]) -> WasmResult {
        get_ops_since(&self.inner.oplog, frontier)
    }

    #[wasm_bindgen(js_name = getLocalVersion)]
    pub fn get_local_frontier(&self) -> Box<[Time]> {
        get_local_frontier(&self.inner.branch.local_version())
    }

    #[wasm_bindgen(js_name = localToRemoteVersion)]
    pub fn local_to_remote_version(&self, time: &[Time]) -> WasmResult {
        frontier_to_remote_time(&self.inner.oplog, time)
    }

    #[wasm_bindgen(js_name = getRemoteVersion)]
    pub fn get_remote_version(&self) -> WasmResult {
        frontier_to_remote_time(&self.inner.oplog, &self.inner.branch.local_version())
    }

    #[wasm_bindgen(js_name = xfSince)]
    pub fn xf_since(&self, from_version: &[usize]) -> WasmResult {
        xf_since(&self.inner.oplog, from_version)
    }

    #[wasm_bindgen(js_name = mergeVersions)]
    pub fn merge_versions(&self, a: &[usize], b: &[usize]) -> Box<[usize]> {
        merge_versions(&self.inner.oplog, a, b)
    }

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

mod utils;

use wasm_bindgen::prelude::*;
use diamond_types::list::{Branch, ListCRDT, ROOT_ORDER};
use diamond_types::list::external_txn::{RemoteId, RemoteTxn, VectorClock};
use diamond_core::AgentId;
use smallvec::smallvec;
use serde_wasm_bindgen::Serializer;
use serde::{Serialize};

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

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
        self.inner.len()
    }

    #[wasm_bindgen]
    pub fn is_empty(&self) -> bool { // To make clippy happy.
        self.inner.is_empty()
    }

    #[wasm_bindgen]
    pub fn get(&self) -> String {
        self.inner.to_string()
    }

    #[wasm_bindgen]
    pub fn get_vector_clock(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner.get_vector_clock())
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn get_frontier(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner.get_frontier::<Vec<RemoteId>>())
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn get_next_order(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner.get_next_order())
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn get_txn_since(&self, version: JsValue) -> Result<JsValue, JsValue> {
        let txns = if version.is_null() || version.is_undefined() {
            self.inner.get_all_txns::<Vec<_>>()
        } else {
            let clock: VectorClock = serde_wasm_bindgen::from_value(version)?;
            self.inner.get_all_txns_since::<Vec<_>>(&clock)
        };

        serde_wasm_bindgen::to_value(&txns)
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn positional_ops_since(&self, order: u32) -> Result<JsValue, JsValue> {
        let changes = self.inner.positional_changes_since(order);

        serde_wasm_bindgen::to_value(&changes)
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn traversal_ops_since(&self, order: u32) -> Result<JsValue, JsValue> {
        let changes = self.inner.traversal_changes_since(order);

        serde_wasm_bindgen::to_value(&changes)
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn traversal_ops_flat(&self, order: u32) -> Result<JsValue, JsValue> {
        let changes = self.inner.flat_traversal_since(order);

        serde_wasm_bindgen::to_value(&changes)
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn attributed_patches_since(&self, order: u32) -> Result<JsValue, JsValue> {
        let (changes, attr) = self.inner.remote_attr_patches_since(order);

        // Using serialize_maps_as_objects here to flatten RemoteSpan into {agent, seq, len}.
        (changes, attr)
            .serialize(&Serializer::new().serialize_maps_as_objects(true))
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn traversal_ops_since_branch(&self, branch: JsValue) -> Result<JsValue, JsValue> {
        let branch: Branch = if branch.is_null() || branch.is_undefined() {
            smallvec![ROOT_ORDER]
        } else {
            let b: Vec<RemoteId> = serde_wasm_bindgen::from_value(branch)?;
            self.inner.remote_ids_to_branch(&b)
        };

        let changes = self.inner.traversal_changes_since_branch(branch.as_slice());

        serde_wasm_bindgen::to_value(&changes)
            .map_err(|err| err.into())
    }

    #[wasm_bindgen]
    pub fn merge_remote_txns(&mut self, txns: JsValue) -> Result<(), JsValue> {
        let txns: Vec<RemoteTxn> = serde_wasm_bindgen::from_value(txns)?;
        for txn in txns.iter() {
            self.inner.apply_remote_txn(txn);
        }
        Ok(())
    }

    #[wasm_bindgen]
    pub fn ins_at_order(&mut self, pos: usize, content: &str, order: u32, is_left: bool) {
        // let id = self.0.get_or_create_agent_id("seph");
        self.inner.insert_at_ot_order(self.agent_id, pos, content, order, is_left);
    }

    #[wasm_bindgen]
    pub fn del_at_order(&mut self, pos: usize, del_span: usize, order: u32) {
        self.inner.delete_at_ot_order(self.agent_id, pos, del_span, order, true);
    }

}

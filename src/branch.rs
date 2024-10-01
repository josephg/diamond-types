use std::collections::{btree_map, BTreeMap, BTreeSet};
use smallvec::SmallVec;
use crate::{CRDTKind, DTRange, Branch, OpLog, LV, LVKey, RegisterInfo, RegisterState, RegisterValue, ROOT_CRDT_ID, Primitive};
use smartstring::alias::String as SmartString;

pub(crate) fn btree_range_for_crdt<V>(map: &BTreeMap<(LVKey, SmartString), V>, crdt: LVKey) -> btree_map::Range<'_, (LVKey, SmartString), V> {
    let empty_str: SmartString = "".into();
    if crdt == ROOT_CRDT_ID {
        // For the root CRDT we can't use the crdt+1 trick because the range wraps around.
        map.range((crdt, empty_str)..)
    } else {
        map.range((crdt, empty_str.clone())..(crdt + 1, empty_str))
    }
}

pub(crate) fn btree_range_mut_for_crdt<V>(map: &mut BTreeMap<(LVKey, SmartString), V>, crdt: LVKey) -> btree_map::RangeMut<'_, (LVKey, SmartString), V> {
    let empty_str: SmartString = "".into();
    if crdt == ROOT_CRDT_ID {
        // For the root CRDT we can't use the crdt+1 trick because the range wraps around.
        map.range_mut((crdt, empty_str)..)
    } else {
        map.range_mut((crdt, empty_str.clone())..(crdt + 1, empty_str))
    }
}

impl RegisterState {
    fn each_value<F: FnMut(&RegisterValue)>(&self, mut f: F) {
        f(&self.value);
        for rv in self.conflicts_with.iter() {
            f(rv);
        }
    }
}

impl OpLog {
    pub fn checkout_at_version(_frontier: &[LV]) -> Branch {
        todo!()
    }

    /// Get the current value for this register, ignoring any other conflicting values.
    ///
    /// TODO: Is it worth keeping this method? Users could just call get_state below and throw out
    /// the conflicting values...
    fn value_for_register_nc(&self, info: &RegisterInfo) -> RegisterValue {
        // We're calculating but not using the conflicting ops. But eh - conflicts are rare.
        let (active_idx, _) = self.tie_break_mv(info);
        (&info.ops[active_idx]).into()
    }

    /// Get this register's state. This includes the current value and any other conflicting values.
    fn get_state_for_register(&self, info: &RegisterInfo) -> RegisterState {
        let (active_idx, other_idxes) = self.tie_break_mv(info);

        RegisterState {
            value: (&info.ops[active_idx]).into(),
            conflicts_with: other_idxes.map(|iter| {
                iter.map(|idx| (&info.ops[idx]).into()).collect()
            }).unwrap_or_default(),
        }
    }


    fn checkout_map_key_nc(&self, crdt: LVKey, key: &str) -> Option<RegisterValue> {
        // Just checkout this path item.
        let info = self.map_keys.get(&(crdt, key.into()))?;
        Some(self.value_for_register_nc(info))
    }

    pub fn checkout_at_path_nc(&self, path: &[&str]) -> Option<RegisterValue> {
        // let mut map_item = ROOT_CRDT_ID;
        let mut item = RegisterValue::OwnedCRDT(CRDTKind::Map, ROOT_CRDT_ID);
        for p in path {
            if let RegisterValue::OwnedCRDT(CRDTKind::Map, key) = item {
                item = self.checkout_map_key_nc(key, *p)?;
            } else {
                // Mmm return an error result here maybe??
                return None;
            }
        }
        return Some(item)
    }

    pub fn checkout_register_at_path_nc(&self, path: &[&str], key: &str) -> Option<Primitive> {
        let val = self.checkout_at_path_nc(path)?;
        if let RegisterValue::OwnedCRDT(CRDTKind::Map, container) = val {
            return if let RegisterValue::Primitive(primitive) = self.checkout_map_key_nc(container, key)? {
                Some(primitive)
            } else { None }
        } else { None }
    }

    pub fn checkout_tip(&self) -> Branch {
        // There's 2 strategies I could employ here:
        // 1. Walk recursively through the tree and copy items
        // 2. Walk through all the living items (registers, maps, texts) and copy them

        // I'm going with option 2, but that might not be the best option.

        let mut maps_to_copy = vec![ROOT_CRDT_ID];
        let mut result = Branch {
            frontier: self.cg.version.clone(),
            maps: Default::default(),
            texts: Default::default(),
        };

        while let Some(crdt) = maps_to_copy.pop() {
            let mut this_map = BTreeMap::new();
            for ((this_id, key), info) in btree_range_for_crdt(&self.map_keys, crdt) {
                debug_assert_eq!(*this_id, crdt);
                let state = self.get_state_for_register(info);

                state.each_value(|rv| {
                    // Recursively copy value and conflicting values.
                    match rv {
                        RegisterValue::Primitive(_) => {}
                        RegisterValue::OwnedCRDT(CRDTKind::Map, child_map) => {
                            // I could use recursion here but this avoids stack-smashing attacks.
                            maps_to_copy.push(*child_map);
                        }
                        RegisterValue::OwnedCRDT(CRDTKind::Register, _) => { todo!() }
                        RegisterValue::OwnedCRDT(CRDTKind::Collection, _) => { todo!() }
                        RegisterValue::OwnedCRDT(CRDTKind::Text, text_crdt) => {
                            // Eventually (rich) text items might contain more embedded CRDTs. But for
                            // now this is fine.
                            let rope = self.checkout_text(*text_crdt);
                            result.texts.insert(*text_crdt, rope);
                        }
                    }
                });

                this_map.insert(key.clone(), state);
            }
            result.maps.insert(crdt, this_map);
        }

        result
    }
}

impl Default for Branch {
    fn default() -> Self {
        Self::new()
    }
}

impl Branch {
    pub fn new() -> Self {
        Self {
            frontier: Default::default(),
            maps: BTreeMap::from([(ROOT_CRDT_ID, Default::default())]),
            texts: Default::default(),
        }
    }

    fn recursive_delete_reg_state(&mut self, state: RegisterState) {
        fn delete_value(b: &mut Branch, val: RegisterValue) {
            if let RegisterValue::OwnedCRDT(kind, key) = val {
                b.recursive_delete(kind, key);
            }
        }

        delete_value(self, state.value);
        for rv in state.conflicts_with {
            delete_value(self, rv);
        }
    }

    fn recursive_delete(&mut self, kind: CRDTKind, crdt: LVKey) {
        // TODO: Make this not recursive to avoid stack smashing.
        match kind {
            CRDTKind::Map => {
                let Some(map) = self.maps.remove(&crdt) else { return; };
                for (_, state) in map {
                    self.recursive_delete_reg_state(state);
                }
            }
            CRDTKind::Text => {
                self.texts.remove(&crdt); // Easy peasy!
            }
            _ => { todo!() }
        }
    }

    /// Returns the list of version ranges which were merged, in reverse order (!!!)
    pub fn merge_changes_to_tip(&mut self, oplog: &OpLog) -> SmallVec<DTRange, 4> {
        // Well, for now nothing can be deleted yet. So that makes things easier.
        let diff_rev = oplog.cg.diff_since_rev(self.frontier.as_ref());

        for range in diff_rev.iter().rev() {
            // for (_, text_crdt) in self.text_index.range(*range) {
            //     text_crdts_to_send.insert(*text_crdt);
            // }

            for (_v, (map_crdt, key)) in oplog.map_index.range(*range) {
                if oplog.deleted_crdts.contains(map_crdt) { continue; } // Container was deleted. Ignore!

                // I could be more clever here, but the easier answer is to just fully replace this
                // object key with the new (current) value.
                let obj = self.maps.entry(*map_crdt).or_default();
                let info = oplog.map_keys.get(&(*map_crdt, key.clone())).unwrap();
                let state = oplog.get_state_for_register(info);

                // I could iterate through the state looking for new CRDT items to insert, but I
                // don't think I need to since they'll also show up in the map_index set.
                let old_state = obj.insert(key.clone(), state);

                let Some(old_state) = old_state else { continue; };
                old_state.each_value(|v| {
                    if let RegisterValue::OwnedCRDT(kind, key) = v {
                        // A register was superceded which used to store a CRDT value. Recursively
                        // delete the old value.
                        self.recursive_delete(*kind, *key);
                    }
                })
            }

            for (_v, text_crdt) in oplog.text_index.range(*range) {
                if oplog.deleted_crdts.contains(text_crdt) { continue; }

                let textinfo = oplog.texts.get(text_crdt).unwrap();
                let text_content = self.texts.entry(*text_crdt).or_default();

                textinfo.merge_into(text_content, &oplog.cg, self.frontier.as_ref(), oplog.cg.version.as_ref());
            }
        }

        self.frontier = oplog.cg.version.clone();
        diff_rev
    }

    pub fn crdt_at_path(&self, path: &[&str]) -> (CRDTKind, LVKey) {
        let mut kind = CRDTKind::Map;
        let mut key = ROOT_CRDT_ID;

        for p in path {
            match kind {
                CRDTKind::Map => {
                    let obj = self.maps.get(&key).unwrap();
                    let state = obj.get(*p).unwrap();

                    match state.value {
                        RegisterValue::Primitive(_) => {
                            panic!("Found primitive, not CRDT");
                        }
                        RegisterValue::OwnedCRDT(new_kind, new_key) => {
                            kind = new_kind;
                            key = new_key;
                        }
                    }
                }
                _ => {
                    panic!("Invalid path in document");
                }
            }
        }

        (kind, key)
    }

    pub fn text_at_path(&self, path: &[&str]) -> LVKey {
        let (kind, key) = self.crdt_at_path(path);
        if kind != CRDTKind::Text {
            panic!("Unexpected CRDT kind {:?}", kind);
        } else { key }
    }

    pub fn register_in_map(&self, path: &[&str], key: &str) -> Option<&RegisterValue> {
        let (kind, crdt) = self.crdt_at_path(path);
        if kind != CRDTKind::Map {
            panic!("Expected a map, found a {:?}", kind);
        }

        Some(&self.maps.get(&crdt)?.get(key)?.value)
    }

    // TODO: Probably better to return a Result here.
    pub fn str_in_map(&self, path: &[&str], key: &str) -> Option<&str> {
        if let RegisterValue::Primitive(Primitive::Str(s)) = self.register_in_map(path, key)? {
            Some(s.as_str())
        } else {
            None
        }
    }

    fn dbg_check(&self, _deep: bool) {
        // Every CRDT (except for the root) should be referenced in exactly 1 place.
        let mut owned_map_crdts = BTreeSet::from([ROOT_CRDT_ID]);
        let mut root_map_crdts = BTreeSet::new();

        let mut owned_text_crdts = BTreeSet::new();
        let root_text_crdts: BTreeSet<_> = self.texts.keys()
            .copied()
            .collect();

        for (map_crdt, state) in &self.maps {
            root_map_crdts.insert(*map_crdt);

            for reg_state in state.values() {
                reg_state.each_value(|v| {
                    if let RegisterValue::OwnedCRDT(kind, key) = v {
                        match kind {
                            CRDTKind::Map => &mut owned_map_crdts,
                            CRDTKind::Text => &mut owned_text_crdts,
                            _ => { unimplemented!() }
                        }.insert(*key);
                    }
                });
            }
        }

        assert_eq!(owned_map_crdts, root_map_crdts);
        assert_eq!(owned_text_crdts, root_text_crdts);
    }
}

#[cfg(test)]
mod tests {
    use crate::{CRDTKind, CreateValue, Branch, OpLog, Primitive, ROOT_CRDT_ID};
    use crate::list::operation::TextOperation;

    fn check_oplog_checkouts_match(oplog: &OpLog) -> Branch {
        // There's two ways we can get a checkout for an oplog: Either call checkout_tip() or
        // make a new branch and call merge_changes_to_tip().

        // Do both and make sure the results match.

        let branch1 = oplog.checkout_tip();
        branch1.dbg_check(true);
        // dbg!(&branch);

        let mut branch2 = Branch::new();
        branch2.merge_changes_to_tip(&oplog);
        branch2.dbg_check(true);

        assert_eq!(branch1, branch2);
        branch1
    }

    #[test]
    fn simple_branch_checkout() {
        let mut oplog = OpLog::new();
        let branch = oplog.checkout_tip();
        branch.dbg_check(true);
        assert_eq!(branch, Branch::new());

        let seph = oplog.cg.get_or_create_agent_id_from_str("seph");
        let text = oplog.local_map_set(seph, ROOT_CRDT_ID, "content", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(seph, text, TextOperation::new_insert(0, "Oh hai!"));
        oplog.local_text_op(seph, text, TextOperation::new_delete(0..3));

        let kaarina = oplog.cg.get_or_create_agent_id_from_str("kaarina");
        let title = oplog.local_map_set(kaarina, ROOT_CRDT_ID, "title", CreateValue::NewCRDT(CRDTKind::Text));
        oplog.local_text_op(kaarina, title, TextOperation::new_insert(0, "Please read this cool info"));

        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "conflict", CreateValue::NewCRDT(CRDTKind::Map));
        let parents = oplog.cg.version.clone();
        let a = oplog.cg.assign_local_op_with_parents(parents.as_ref(), seph, 1).start;
        let b = oplog.cg.assign_local_op_with_parents(parents.as_ref(), kaarina, 1).start;
        oplog.remote_map_set(child_obj, a, "yo", CreateValue::Primitive(Primitive::I64(123)));
        oplog.remote_map_set(child_obj, b, "yo", CreateValue::Primitive(Primitive::I64(321)));

        // let b = oplog.checkout_tip();
        // dbg!(b);
        // dbg!(b.crdt_at_path(&["title"]));
        // dbg!(b.register_in_map(&["conflict"], "yo"));

        check_oplog_checkouts_match(&oplog);

        // dbg!(oplog.checkout_tip().simple_val());
    }

    #[test]
    fn checkout_simple_items() {
        let mut oplog = OpLog::new();

        let seph = oplog.cg.get_or_create_agent_id_from_str("seph");
        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "child", CreateValue::NewCRDT(CRDTKind::Map));
        oplog.local_map_set(seph, child_obj, "a", CreateValue::Primitive(Primitive::I64(222)));

        let result = oplog.checkout_register_at_path_nc(&["child"], "a");
        assert_eq!(result, Some(Primitive::I64(222)));
    }

    #[test]
    fn overwrite_crdt_works() {
        let mut oplog = OpLog::new();
        let seph = oplog.cg.get_or_create_agent_id_from_str("seph");

        let mut branch_incremental = Branch::new();
        let child_obj = oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::NewCRDT(CRDTKind::Map));
        branch_incremental.merge_changes_to_tip(&oplog);
        let text_item = oplog.local_map_set(seph, child_obj, "text_item", CreateValue::NewCRDT(CRDTKind::Text));
        branch_incremental.merge_changes_to_tip(&oplog);
        oplog.local_text_op(seph, text_item, TextOperation::new_insert(0, "yooo"));
        branch_incremental.merge_changes_to_tip(&oplog);
        oplog.local_map_set(seph, child_obj, "smol_embedded", CreateValue::NewCRDT(CRDTKind::Map));
        branch_incremental.merge_changes_to_tip(&oplog);

        // Now overwrite the parent item.
        oplog.local_map_set(seph, ROOT_CRDT_ID, "overwritten", CreateValue::Primitive(Primitive::I64(123)));
        branch_incremental.merge_changes_to_tip(&oplog);

        let branch_expected = check_oplog_checkouts_match(&oplog);

        assert_eq!(branch_expected, branch_incremental);
    }
}
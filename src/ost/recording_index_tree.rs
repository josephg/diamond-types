//! This file is for debugging. It provides an implementation of index_tree which can record
//! and play back operations made to an index tree.

use std::cell::RefCell;
use std::hint::black_box;
use std::ops::Deref;
use rle::RleDRun;
use crate::{DTRange, LV};
use crate::ost::{IndexContent, IndexTree};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde::de::DeserializeOwned;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TreeCommand<V: Copy> {
    GetEntry(LV),
    SetRange(DTRange, V),
    Clear,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordingTree<V: Copy> {
    inner: IndexTree<V>,
    pub actions: RefCell<Vec<TreeCommand<V>>>,
}

impl<V: IndexContent + Default> Default for RecordingTree<V> {
    fn default() -> Self {
        Self {
            inner: IndexTree::default(),
            actions: RefCell::new(vec![]),
        }
    }
}

impl<V: IndexContent + Default> RecordingTree<V> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_entry(&self, lv: LV) -> RleDRun<V> {
        self.actions.borrow_mut().push(TreeCommand::GetEntry(lv));
        self.inner.get_entry(lv)
    }

    pub fn clear(&mut self) {
        self.actions.get_mut().push(TreeCommand::Clear);
        self.inner.clear();
    }

    pub fn set_range(&mut self, range: DTRange, data: V) {
        self.actions.get_mut().push(TreeCommand::SetRange(range, data));
        self.inner.set_range(range, data)
    }

    pub fn dbg_check(&self) {
        self.inner.dbg_check()
    }

    #[cfg(feature = "gen_test_data")]
    pub fn actions_to_json(&self) -> Vec<u8> where V: Serialize {
        // serde_json::to_vec_pretty(self.actions.borrow().deref()).unwrap()
        serde_json::to_vec(self.actions.borrow().deref()).unwrap()
    }

    #[cfg(feature = "gen_test_data")]
    pub fn stats(&self) {
        let set_acts = self.actions.borrow().iter()
            .filter(|a| if let TreeCommand::SetRange(_, _) = a { true } else { false })
            .count();
        dbg!(set_acts);

        let get_acts = self.actions.borrow().iter()
            .filter(|a| if let TreeCommand::GetEntry(_) = a { true } else { false })
            .count();
        dbg!(get_acts);
    }
}

#[derive(Debug, Clone)]
pub struct IndexTreeReplay<V: IndexContent>(Vec<TreeCommand<V>>);

#[cfg(feature = "serde")]
impl<V: IndexContent + Default + DeserializeOwned> IndexTreeReplay<V> {
    pub fn from_json(json: &[u8]) -> Self {
        Self(serde_json::from_slice(json).unwrap())
    }

    pub fn replay(&self) {
        let mut tree = IndexTree::new();

        for action in self.0.iter() {
            match action {
                TreeCommand::GetEntry(lv) => { black_box(tree.get_entry(*lv)); },
                TreeCommand::SetRange(range, val) => tree.set_range(*range, *val),
                TreeCommand::Clear => tree.clear(),
            }
        }

        black_box(tree);
    }
}


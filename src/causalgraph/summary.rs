use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;
use crate::{CausalGraph, DTRange};
use rle::MergeableIterator;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VSEntry {
    pub name: SmartString,
    pub versions: SmallVec<[DTRange; 2]>,
}

pub struct VersionSummary(Vec<VSEntry>);

impl CausalGraph {
    fn summarize(&self) -> VersionSummary {
        VersionSummary(self.client_data.iter().map(|c| {
            VSEntry {
                name: c.name.clone(),
                versions: c.item_times
                    .iter()
                    .map(|e| e.range())
                    .merge_spans()
                    .collect()
            }
        }).collect())
    }
}
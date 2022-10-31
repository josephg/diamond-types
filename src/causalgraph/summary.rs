use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;
use crate::{CausalGraph, DTRange};
use rle::MergeableIterator;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VSEntry {
    pub name: SmartString,
    pub versions: SmallVec<[DTRange; 2]>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VersionSummary(Vec<VSEntry>);

impl CausalGraph {
    fn summarize(&self) -> VersionSummary {
        VersionSummary(self.client_data.iter().filter_map(|c| {
            if c.item_times.is_empty() { None }
            else {
                Some(VSEntry {
                    name: c.name.clone(),
                    versions: c.item_times
                        .iter()
                        .map(|e| e.range())
                        .merge_spans()
                        .collect()
                })
            }
        }).collect())
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use crate::CausalGraph;
    use crate::causalgraph::summary::{VersionSummary, VSEntry};
    use crate::remotespan::CRDTSpan;

    #[test]
    fn summary_smoke() {
        let mut cg = CausalGraph::new();
        assert_eq!(cg.summarize(), VersionSummary(vec![]));

        cg.get_or_create_agent_id("seph");
        cg.get_or_create_agent_id("mike");
        cg.merge_and_assign(&[], CRDTSpan {
            agent: 0,
            seq_range: (0..5).into()
        });

        // dbg!(cg.summarize());
        assert_eq!(cg.summarize(), VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                versions: smallvec![(0..5).into()]
            }
        ]));

        cg.merge_and_assign(&[], CRDTSpan {
            agent: 1,
            seq_range: (0..5).into()
        });
        cg.merge_and_assign(&[4], CRDTSpan {
            agent: 0,
            seq_range: (5..10).into()
        });

        assert_eq!(cg.summarize(), VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                versions: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                versions: smallvec![(0..5).into()]
            }
        ]));

        // And with a gap...
        cg.merge_and_assign(&[4], CRDTSpan {
            agent: 1,
            seq_range: (15..20).into()
        });

        assert_eq!(cg.summarize(), VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                versions: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                versions: smallvec![(0..5).into(), (15..20).into()]
            }
        ]));
    }
}
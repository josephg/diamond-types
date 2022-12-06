use smallvec::{SmallVec, smallvec};
use smartstring::alias::String as SmartString;
use crate::{CausalGraph, DTRange, Frontier, LV};
use rle::{HasLength, MergeableIterator, SplitableSpanHelpers};

#[cfg(feature = "serde")]
use serde::{Serialize, Deserialize};
use crate::causalgraph::agent_assignment::AgentAssignment;
use crate::rle::RleSpanHelpers;

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VSEntry {
    pub name: SmartString,
    pub versions: SmallVec<[DTRange; 2]>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VersionSummary(Vec<VSEntry>);

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct VersionSummaryFlat(Vec<(SmartString, usize)>);

// Serialize as {name1: [[start, end], [start, end], ..], name2: ...}.
#[cfg(feature = "serde")]
mod serde_encoding {
    use std::fmt::Formatter;
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde::de::{MapAccess, Visitor};
    use smallvec::SmallVec;
    use crate::causalgraph::summary::{VersionSummary, VersionSummaryFlat, VSEntry};
    use crate::DTRange;
    use smartstring::alias::String as SmartString;

    impl Serialize for VersionSummary {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for e in &self.0 {
                map.serialize_entry(&e.name, &e.versions)?;
            }
            map.end()
        }
    }

    struct VSVisitor;

    impl<'de> Visitor<'de> for VSVisitor {
        type Value = VersionSummary;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("A version summary map")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error> where A: MapAccess<'de> {
            let mut vs = VersionSummary(Vec::with_capacity(map.size_hint().unwrap_or(0)));

            while let Some((k, v)) = map.next_entry::<SmartString, SmallVec<[DTRange; 2]>>()? {
                vs.0.push(VSEntry {
                    name: k,
                    versions: v,
                })
            }
            Ok(vs)
        }
    }

    impl<'de> Deserialize<'de> for VersionSummary {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
            deserializer.deserialize_map(VSVisitor)
        }
    }

    impl Serialize for VersionSummaryFlat {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for e in &self.0 {
                map.serialize_entry(&e.0, &e.1)?;
            }
            map.end()
        }
    }

    struct VSVisitorFlat;

    impl<'de> Visitor<'de> for VSVisitorFlat {
        type Value = VersionSummaryFlat;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("A flat version summary")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error> where A: MapAccess<'de> {
            let mut vs = VersionSummaryFlat(Vec::with_capacity(map.size_hint().unwrap_or(0)));

            while let Some((k, v)) = map.next_entry::<SmartString, usize>()? {
                vs.0.push((k, v))
            }
            Ok(vs)
        }
    }

    impl<'de> Deserialize<'de> for VersionSummaryFlat {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
            deserializer.deserialize_map(VSVisitorFlat)
        }
    }
}


impl AgentAssignment {
    pub fn summarize_versions(&self) -> VersionSummary {
        VersionSummary(self.client_data.iter().filter_map(|c| {
            if c.item_times.is_empty() { None } else {
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

    pub fn summarize_versions_flat(&self) -> VersionSummaryFlat {
        VersionSummaryFlat(self.client_data.iter().filter_map(|c| {
            if c.item_times.is_empty() { None } else { Some((c.name.clone(), c.get_next_seq())) }
        }).collect())
    }

    pub fn intersect_with_flat_summary_full<V>(&self, summary: &VersionSummaryFlat, mut visitor: V)
        where V: FnMut(&str, DTRange, Option<LV>)
    {
        for (name, end_seq) in summary.0.iter() {
            let agent_id = self.get_agent_id(name);
            let mut next_seq = 0;

            if let Some(agent_id) = agent_id {
                let entries = &self.client_data[agent_id as usize].item_times;

                for e in entries.iter() {
                    let entry_start = e.0;

                    assert_eq!(entry_start, next_seq, "Entries for client not packed!");
                    let entry_end_seq = e.end();
                    next_seq = entry_end_seq;

                    if entry_start >= *end_seq { break; }

                    let mut range = e.range();
                    if entry_end_seq > *end_seq {
                        range.truncate_h(*end_seq - entry_start);
                    }

                    visitor(name, range, Some(e.1.start));
                }
            }

            if next_seq < *end_seq {
                visitor(name, (next_seq..*end_seq).into(), None);
            }
        }
    }
}

impl CausalGraph {
    pub fn intersect_with_flat_summary(&self, summary: &VersionSummaryFlat, frontier: &[LV]) -> (Frontier, Option<VersionSummaryFlat>) {
        let mut remainder: Option<VersionSummaryFlat> = None;
        // We'll just accumulate all the versions we see and check for dominators.
        // It would probably still be correct to just take the last version from each agent.
        let mut versions: SmallVec<[LV; 4]> = frontier.into();

        self.agent_assignment.intersect_with_flat_summary_full(summary, |name, seq, v| {
            if let Some(v) = v {
                let v_last = v + seq.len() - 1;
                versions.push(v_last);
            } else {
                let remainder = remainder.get_or_insert_with(Default::default);
                remainder.0.push((name.into(), seq.end));
            }
        });

        (
            self.graph.find_dominators_unsorted(&versions),
            remainder
        )
    }

    // pub fn intersect_with_summary_full<V>(&self, summary: &VersionSummary, visitor: V)
    // where V: FnMut(&str, usize, usize, Option<Time>)
    // {
    //     for VSEntry { name, versions: seqs } in &summary.0 {
    //         let agent_id = self.get_agent_id(name);
    //
    //         for seq_range in seqs {
    //             if let Some(agent_id) = agent_id {
    //                 let cg = &self.client_data[agent_id as usize].item_times;
    //
    //                 let mut idx = cg.find_index(seq_range.start).unwrap_or_else(|e| e);
    //
    //                 loop {
    //                     todo!()
    //                 }
    //             }
    //         }
    //     }
    // }

    // pub fn intersect_with_summary(&self, summary: &VersionSummary,
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;
    use crate::CausalGraph;
    use crate::causalgraph::summary::{VersionSummary, VersionSummaryFlat, VSEntry};
    use crate::causalgraph::agent_span::AgentSpan;

    #[test]
    fn summary_smoke() {
        let mut cg = CausalGraph::new();
        assert_eq!(cg.agent_assignment.summarize_versions(), VersionSummary(vec![]));
        assert_eq!(cg.agent_assignment.summarize_versions_flat(), VersionSummaryFlat(vec![]));

        cg.get_or_create_agent_id("seph");
        cg.get_or_create_agent_id("mike");

        assert_eq!(cg.agent_assignment.summarize_versions(), VersionSummary(vec![]));
        assert_eq!(cg.agent_assignment.summarize_versions_flat(), VersionSummaryFlat(vec![]));

        cg.merge_and_assign(&[], AgentSpan {
            agent: 0,
            seq_range: (0..5).into()
        });

        // dbg!(cg.summarize());
        assert_eq!(cg.agent_assignment.summarize_versions(), VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                versions: smallvec![(0..5).into()]
            }
        ]));

        cg.merge_and_assign(&[], AgentSpan {
            agent: 1,
            seq_range: (0..5).into()
        });
        cg.merge_and_assign(&[4], AgentSpan {
            agent: 0,
            seq_range: (5..10).into()
        });

        assert_eq!(cg.agent_assignment.summarize_versions(), VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                versions: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                versions: smallvec![(0..5).into()]
            }
        ]));

        assert_eq!(cg.agent_assignment.summarize_versions_flat(), VersionSummaryFlat(vec![
            ("seph".into(), 10),
            ("mike".into(), 5)
        ]));

        // cg.intersect_with_flat_summary_full(&VersionSummaryFlat(vec![
        //     ("seph".into(), 20),
        //     ("mike".into(), 100),
        // ]), |name, seq, v| {
        //     dbg!(name, seq, v);
        // });
        dbg!(cg.intersect_with_flat_summary(&VersionSummaryFlat(vec![
            ("seph".into(), 10),
            ("mike".into(), 5),
        ]), &[9]));

        // And with a gap...
        cg.merge_and_assign(&[4, 9], AgentSpan {
            agent: 1,
            seq_range: (15..20).into()
        });

        assert_eq!(cg.agent_assignment.summarize_versions(), VersionSummary(vec![
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

    #[test]
    #[cfg(all(feature = "serde", feature = "serde_json"))]
    fn test_serialize() {
        let mut cg = CausalGraph::new();

        cg.get_or_create_agent_id("seph");
        cg.get_or_create_agent_id("mike");
        cg.merge_and_assign(&[], AgentSpan {
            agent: 0,
            seq_range: (0..5).into()
        });

        cg.merge_and_assign(&[], AgentSpan {
            agent: 1,
            seq_range: (0..5).into()
        });
        cg.merge_and_assign(&[4], AgentSpan {
            agent: 0,
            seq_range: (5..10).into()
        });
        cg.merge_and_assign(&[4], AgentSpan {
            agent: 1,
            seq_range: (15..20).into()
        });

        let summary = cg.agent_assignment.summarize_versions();
        let s = serde_json::to_string(&summary).unwrap();

        let summary2: VersionSummary = serde_json::from_str(&s).unwrap();
        // dbg!(summary2);
        assert_eq!(summary, summary2);
        // summary
    }
}
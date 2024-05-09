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
    pub seq_ranges: SmallVec<DTRange, 2>,
}

/// A full version summary names the ranges of known sequence numbers for each agent. This is useful
/// when synchronizing changes.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct VersionSummary(Vec<VSEntry>);

/// A flat version summary just names the **next** sequence number from each user agent. This is
/// useful when the agent IDs are guaranteed to be sequential - that is, for graphs with the
/// property that (agent, seq0) < (agent, seq1) iff seq0 < seq1.
///
/// IF the same user agent can submit changes on multiple branches, this property does not hold.
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
                map.serialize_entry(&e.name, &e.seq_ranges)?;
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

            while let Some((k, v)) = map.next_entry::<SmartString, SmallVec<DTRange, 2>>()? {
                vs.0.push(VSEntry {
                    name: k,
                    seq_ranges: v,
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
            if c.lv_for_seq.is_empty() { None } else {
                Some(VSEntry {
                    name: c.name.clone(),
                    seq_ranges: c.lv_for_seq
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
            if c.lv_for_seq.is_empty() { None }
            else { Some((c.name.clone(), c.get_next_seq())) }
        }).collect())
    }

    pub fn intersect_with_flat_summary_full<V>(&self, summary: &VersionSummaryFlat, mut visitor: V)
        where V: FnMut(&str, DTRange, Option<LV>)
    {
        for (name, known_next_seq) in summary.0.iter() {
            let agent_id = self.get_agent_id(name);
            let mut next_seq = 0;

            if let Some(agent_id) = agent_id {
                let entries = &self.client_data[agent_id as usize].lv_for_seq;

                for e in entries.iter() {
                    let entry_start = e.0;

                    assert_eq!(entry_start, next_seq, "Entries for client not packed!");
                    let entry_end_seq = e.end();
                    next_seq = entry_end_seq;

                    if entry_start >= *known_next_seq { break; }

                    let mut seq_range = e.range();
                    if entry_end_seq > *known_next_seq {
                        seq_range.truncate_h(*known_next_seq - entry_start);
                    }

                    visitor(name, seq_range, Some(e.1.start));
                }
            }

            if next_seq < *known_next_seq {
                visitor(name, (next_seq..*known_next_seq).into(), None);
            }
        }
    }

    pub fn intersect_with_summary_full<'a, V>(&self, summary: &'a VersionSummary, mut visitor: V)
        where V: FnMut(&'a str, DTRange, Option<LV>)
    {
        for VSEntry { name, seq_ranges } in summary.0.iter() {
            if let Some(agent_id) = self.get_agent_id(name) {
                let client_data = &self.client_data[agent_id as usize];

                for seq_range in seq_ranges {
                    // entries.iter_range skips missing entries, so we need to manually yield those.
                    let mut expect_next_seq = seq_range.start;
                    for entry in client_data.lv_for_seq.iter_range(*seq_range) {
                        let seq_range = entry.range();

                        if seq_range.start > expect_next_seq {
                            visitor(name, (expect_next_seq..seq_range.start).into(), None);
                        }

                        expect_next_seq = seq_range.end;

                        visitor(name, seq_range, Some(entry.1.start));
                    }

                    if expect_next_seq < seq_range.end {
                        visitor(name, (expect_next_seq..seq_range.end).into(), None);
                    }
                }
            } else {
                // We're missing all operations for this user agent. Yield back the data from vs.
                for seq_range in seq_ranges {
                    visitor(name, *seq_range, None);
                }
            }
        }
    }
}

impl CausalGraph {
    pub fn intersect_with_flat_summary(&self, summary: &VersionSummaryFlat, frontier: &[LV]) -> (Frontier, Option<VersionSummaryFlat>) {
        let mut remainder: Option<VersionSummaryFlat> = None;
        // We'll just accumulate all the versions we see and check for dominators.
        // It would probably still be correct to just take the last version from each agent.
        let mut versions: SmallVec<LV, 4> = frontier.into();

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
            self.graph.find_dominators(&versions),
            remainder
        )
    }

    pub fn intersect_with_summary(&self, summary: &VersionSummary, frontier: &[LV]) -> (Frontier, Option<VersionSummary>) {
        let mut remainder: Option<VersionSummary> = None;

        // We'll just accumulate all the versions we see and check for dominators.
        let mut versions: SmallVec<LV, 4> = frontier.into();

        self.agent_assignment.intersect_with_summary_full(summary, |name, seq_range, v| {
            if let Some(v) = v {
                let v_last = v + seq_range.len() - 1;
                versions.push(v_last);
            } else {
                let remainder = remainder.get_or_insert_with(Default::default);
                match remainder.0.last_mut() {
                    Some(entry) if entry.name == name => {
                        entry.seq_ranges.push(seq_range);
                    }
                    _ => {
                        remainder.0.push(VSEntry {
                            name: name.into(),
                            seq_ranges: smallvec![seq_range],
                        })
                    }
                }
            }
        });

        (
            self.graph.find_dominators(&versions),
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
                seq_ranges: smallvec![(0..5).into()]
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
                seq_ranges: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                seq_ranges: smallvec![(0..5).into()]
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
                seq_ranges: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                seq_ranges: smallvec![(0..5).into(), (15..20).into()]
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

    #[test]
    fn intersect_summary() {
        let mut cg = CausalGraph::new();
        cg.get_or_create_agent_id("seph");

        let vs = VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                seq_ranges: smallvec![(0..10).into()]
            },
            VSEntry {
                name: "mike".into(),
                seq_ranges: smallvec![(0..5).into()]
            }
        ]);

        let mut intersect = vec![];
        cg.agent_assignment.intersect_with_summary_full(&vs, |name, seq_range, v_base| {
            intersect.push((name, seq_range, v_base));
        });
        assert_eq!(&intersect, &[
            ("seph", (0..10).into(), None),
            ("mike", (0..5).into(), None),
        ]);

        let (frontier, remainder) = cg.intersect_with_summary(&vs, &[]);
        assert!(frontier.is_empty());
        assert_eq!(remainder.as_ref(), Some(&vs));

        cg.get_or_create_agent_id("mike");
        cg.merge_and_assign(&[], AgentSpan {
            agent: 0,
            seq_range: (1..5).into(),
        });
        cg.merge_and_assign(&[], AgentSpan {
            agent: 0,
            seq_range: (8..9).into(),
        });

        let mut intersect = vec![];
        cg.agent_assignment.intersect_with_summary_full(&vs, |name, seq_range, v_base| {
            intersect.push((name, seq_range, v_base));
        });
        assert_eq!(&intersect, &[
            ("seph", (0..1).into(), None),
            ("seph", (1..5).into(), Some(0)),
            ("seph", (5..8).into(), None),
            ("seph", (8..9).into(), Some(4)),
            ("seph", (9..10).into(), None),
            ("mike", (0..5).into(), None),
        ]);

        let (frontier, remainder) = cg.intersect_with_summary(&vs, &[]);
        assert_eq!(frontier.as_ref(), &[3, 4]);
        assert_eq!(remainder, Some(VersionSummary(vec![
            VSEntry {
                name: "seph".into(),
                seq_ranges: smallvec![(0..1).into(), (5..8).into(), (9..10).into()],
            },
            VSEntry {
                name: "mike".into(),
                seq_ranges: smallvec![(0..5).into()],
            },
        ])));

        let kaarina = cg.get_or_create_agent_id("kaarina");
        let v = cg.merge_and_assign(&[3, 4], AgentSpan {
            agent: kaarina,
            seq_range: (0..10).into(),
        }).last();
        let (frontier, _) = cg.intersect_with_summary(&vs, &[v]);
        assert_eq!(frontier.as_ref(), &[v]);
    }
}
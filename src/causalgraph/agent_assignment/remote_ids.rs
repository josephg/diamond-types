/// This file contains utilities to convert remote IDs to local version and back.


use smartstring::alias::String as SmartString;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;
use rle::{HasLength, MergableSpan, SplitableSpanHelpers};
use crate::dtrange::DTRange;
use crate::{Frontier, LV};
use crate::causalgraph::agent_assignment::{AgentAssignment, ClientID};
use crate::causalgraph::agent_span::{AgentVersion, AgentSpan};

// /// Remote IDs are IDs you can pass to a remote peer.
// #[derive(Clone, Debug, Eq, PartialEq)]
// #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// pub struct RemoteVersionOwned(pub ClientID, pub usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RemoteVersion(pub ClientID, pub usize);

// impl<'a> From<&'a RemoteVersionOwned> for RemoteVersion<'a> {
//     fn from(rv: &'a RemoteVersionOwned) -> Self {
//         RemoteVersion(rv.0.as_str(), rv.1)
//     }
// }
// impl<'a> From<&RemoteVersion<'a>> for RemoteVersionOwned {
//     fn from(rv: &RemoteVersion) -> Self {
//         RemoteVersionOwned(rv.0.into(), rv.1)
//     }
// }
// impl<'a> From<RemoteVersion<'a>> for RemoteVersionOwned {
//     fn from(rv: RemoteVersion) -> Self {
//         RemoteVersionOwned(rv.0.into(), rv.1)
//     }
// }
//
// impl<'a> RemoteVersion<'a> {
//     pub fn to_owned(&self) -> RemoteVersionOwned {
//         self.into()
//     }
// }

// impl<S> From<(S, usize)> for RemoteVersionOwned where S: Into<SmartString> {
//     fn from(r: (S, usize)) -> Self {
//         Self(r.0.into(), r.1)
//     }
// }
impl<'a, S> From<(S, usize)> for RemoteVersion where S: Into<Uuid> {
    fn from(r: (S, usize)) -> Self {
        Self(r.0.into(), r.1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RemoteVersionSpan(pub Uuid, pub DTRange);

impl HasLength for RemoteVersionSpan {
    fn len(&self) -> usize {
        self.1.len()
    }
}

impl SplitableSpanHelpers for RemoteVersionSpan {
    fn truncate_h(&mut self, at: usize) -> Self {
        Self(self.0, self.1.truncate_h(at))
    }
}

impl MergableSpan for RemoteVersionSpan {
    fn can_append(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1.can_append(&other.1)
    }

    fn append(&mut self, other: Self) {
        self.1.append(other.1)
    }
}

pub type RemoteFrontier = SmallVec<RemoteVersion, 2>;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum VersionConversionError {
    UnknownAgent,
    SeqInFuture,
}

impl AgentAssignment {
    pub fn try_remote_to_local_version(&self, rv: RemoteVersion) -> Result<LV, VersionConversionError> {
        let agent = self.get_agent_id(rv.0)
            .ok_or(VersionConversionError::UnknownAgent)?;

        self.client_data[agent as usize]
            .try_seq_to_lv(rv.1)
            .ok_or(VersionConversionError::SeqInFuture)
    }

    /// This panics if the ID isn't known to the document.
    pub fn remote_to_local_version(&self, RemoteVersion(name, seq): RemoteVersion) -> LV {
        let agent = self.get_agent_id(name).unwrap();
        self.client_data[agent as usize].seq_to_lv(seq)
    }

    pub(crate) fn agent_version_to_remote(&self, (agent, seq): AgentVersion) -> RemoteVersion {
        RemoteVersion(
            self.get_agent_name(agent),
            seq
        )
    }

    pub(crate) fn agent_span_to_remote(&self, loc: AgentSpan) -> RemoteVersionSpan {
        RemoteVersionSpan(
            self.get_agent_name(loc.agent),
            loc.seq_range
        )
    }

    pub(crate) fn remote_to_agent_version_unknown(&mut self, RemoteVersion(name, seq): RemoteVersion) -> AgentVersion {
        let agent = self.get_or_create_agent_id(name);
        (agent, seq)
    }
    pub(crate) fn remote_to_agent_version_known(&self, RemoteVersion(name, seq): RemoteVersion) -> AgentVersion {
        let agent = self.get_agent_id(name).unwrap();
        (agent, seq)
    }

    pub fn local_to_remote_version(&self, v: LV) -> RemoteVersion {
        let agent_v = self.local_to_agent_version(v);
        self.agent_version_to_remote(agent_v)
    }

    /// **NOTE:** This method will return a version span with length min(lv, agent_v). The
    /// resulting length will NOT be guaranteed to be the same as the input.
    pub fn local_to_remote_version_span(&self, v: DTRange) -> RemoteVersionSpan {
        let agent_span = self.local_span_to_agent_span(v);
        self.agent_span_to_remote(agent_span)
    }

    pub fn try_remote_to_local_frontier<'a, B: 'a, I>(&self, ids_iter: I) -> Result<Frontier, VersionConversionError>
        where RemoteVersion: From<B>, I: Iterator<Item=B> + 'a
    {
        let frontier: Frontier = ids_iter
            .map(|rv| self.try_remote_to_local_version(rv.into()))
            .collect::<Result<Frontier, VersionConversionError>>()?;

        Ok(frontier)
    }

    // pub fn try_remote_to_local_frontier<'a, I: Iterator<Item=RemoteVersion<'a>> + 'a>(&self, ids_iter: I) -> Result<Frontier, VersionConversionError> {
    // }

    // This method should work for &RemoteVersionOwned and RemoteVersion and whatever else.
    pub fn remote_to_local_frontier<'a, B: 'a, I>(&self, ids_iter: I) -> Frontier
        where RemoteVersion: From<B>, I: Iterator<Item=B> + 'a
    {
        let frontier: Frontier = ids_iter
            .map(|rv| self.remote_to_local_version(rv.into()))
            .collect();

        frontier
    }

    pub fn local_to_remote_frontier(&'_ self, local_frontier: &[LV]) -> RemoteFrontier {
        // Could return an impl Iterator here instead.
        local_frontier
            .iter()
            .map(|lv| self.local_to_remote_version(*lv))
            .collect()
    }

    // pub fn local_to_remote_frontier_owned(&'_ self, local_frontier: &[LV]) -> RemoteFrontierOwned {
    //     // Could return an impl Iterator here instead.
    //     local_frontier
    //         .iter()
    //         .map(|lv| self.local_to_remote_version(*lv).into())
    //         .collect()
    // }

    pub fn iter_remote_mappings(&self) -> impl Iterator<Item = RemoteVersionSpan> + '_ {
        self.client_with_lv
            .iter()
            .map(|item| self.agent_span_to_remote(item.1))
    }

    pub fn iter_remote_mappings_range(&self, range: DTRange) -> impl Iterator<Item = RemoteVersionSpan> + '_ {
        self.client_with_lv
            .iter_range(range)
            .map(|item| self.agent_span_to_remote(item.1))
    }
}

#[cfg(test)]
mod test {
    use crate::causalgraph::agent_assignment::remote_ids::{RemoteVersion};
    use crate::CausalGraph;
    use crate::causalgraph::agent_assignment::client_id_from_str;

    #[test]
    fn id_smoke_test() {
        let mut cg = CausalGraph::new();

        let seph_uuid = client_id_from_str("seph").unwrap();
        let mike_uuid = client_id_from_str("mike").unwrap();

        cg.get_or_create_agent_id(seph_uuid);
        cg.get_or_create_agent_id(mike_uuid);
        cg.assign_local_op_with_parents(&[], 0, 2);
        cg.assign_local_op_with_parents(&[], 1, 4);

        assert_eq!(0, cg.agent_assignment.remote_to_local_version(RemoteVersion(seph_uuid, 0)));
        assert_eq!(1, cg.agent_assignment.remote_to_local_version(RemoteVersion(seph_uuid, 1)));
        assert_eq!(2, cg.agent_assignment.remote_to_local_version(RemoteVersion(mike_uuid, 0)));

        for lv in 0..cg.len() {
            let rv = cg.agent_assignment.local_to_remote_version(lv);
            let expect_lv = cg.agent_assignment.remote_to_local_version(rv);
            assert_eq!(lv, expect_lv);
        }

        // assert_eq!(oplog.get_vector_clock().as_slice(), &[
        //     RemoteId {
        //         agent: "seph".into(),
        //         seq: 2,
        //     },
        //     RemoteId {
        //         agent: "mike".into(),
        //         seq: 4,
        //     },
        // ]);
    }

    #[test]
    fn remote_versions_can_be_empty() {
        let cg = CausalGraph::new();
        assert!(cg.agent_assignment.remote_to_local_frontier(std::iter::empty::<RemoteVersion>()).is_root());
    }
}
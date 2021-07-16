use smartstring::alias::{String as SmartString};
use smallvec::SmallVec;
// use crate::LocalOp;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteId {
    pub agent: SmartString,
    pub seq: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteOp {
    Ins {
        origin_left: RemoteId,
        origin_right: RemoteId,
        ins_content: SmartString, // ?? Or just length?
    },

    Del {
        id: RemoteId,
        len: u32,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteTxn {
    pub id: RemoteId,
    pub parents: SmallVec<[RemoteId; 2]>, // usually 1 entry
    pub ops: SmallVec<[RemoteOp; 2]> // usually 1-2 entries.
}

// #[derive(Clone, Debug, Eq, PartialEq)]
// pub struct BraidTxn {
//     pub id: RemoteId,
//     pub parents: SmallVec<[RemoteId; 2]>, // usually 1 entry
//     pub ops: SmallVec<[LocalOp; 2]> // usually 1-2 entries.
// }

// thread_local! {
// const REMOTE_ROOT: RemoteId = RemoteId {
//     agent: "ROOT".into(),
//     seq: u32::MAX
// };
// }
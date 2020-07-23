// Should this be called mod.rs?
use inlinable_string::InlinableString;

pub type ClientName = InlinableString;
pub type ClientID = u16;
pub type ClientSeq = u32;

pub const CLIENT_INVALID: ClientID = ClientID::MAX;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CRDTLocation {
    pub client: ClientID,
    pub seq: ClientSeq,
}
pub mod alloc;
pub mod list;
mod rle;
mod localtime;
mod unicount;
mod remotespan;

pub type AgentId = u32;
pub const ROOT_AGENT: AgentId = AgentId::MAX;
pub const ROOT_TIME: usize = usize::MAX;
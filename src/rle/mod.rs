
pub type RLEKey = u32;

mod simple_rle;
mod mutable_rle;

pub use simple_rle::Rle;
pub use mutable_rle::MutRle;

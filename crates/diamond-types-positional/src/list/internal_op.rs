
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct Operation {
//     pub pos: usize,
//     pub len: usize,
//
//     /// rev marks the operation order as reversed. For now this is only supported on deletes, for
//     /// backspacing.
//     /// TODO: Consider swapping this to fwd
//     pub reversed: bool,
//
//     // TODO: Remove content_known by making content an Option(...)
//     pub content_known: bool,
//     pub tag: InsDelTag,
//     // pub content_bytes_offset: usize,
// }
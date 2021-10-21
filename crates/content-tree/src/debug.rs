use std::fmt::*;
use crate::*;

struct DebugContent<'a, E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize>(&'a ContentTreeRaw<E, I, IE, LE>);

impl<'a, E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> Debug for DebugContent<'a, E, I, IE, LE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter())
            .finish()
    }
}


impl<E: ContentTraits, I: TreeMetrics<E>, const IE: usize, const LE: usize> Debug for ContentTreeRaw<E, I, IE, LE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentTree")
            .field("count", &self.count)
            .field("(content)", &DebugContent(self))
            .finish()
    }
}
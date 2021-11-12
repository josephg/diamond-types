use rle::SplitableSpan;
use crate::list::{ListCRDT, OpSet, Time};
use crate::list::operation::PositionalComponent;
use crate::localtime::TimeSpan;
use crate::rle::{KVPair, RleVec};

pub(crate) struct OpIter<'a> {
    list: &'a RleVec<KVPair<PositionalComponent>>,
    idx: usize,
    range: TimeSpan,
}

impl<'a> Iterator for OpIter<'a> {
    type Item = KVPair<PositionalComponent>;

    fn next(&mut self) -> Option<Self::Item> {
        // I bet there's a more efficient way to write this function.
        if self.idx >= self.list.0.len() { return None; }

        let KVPair(mut time, mut c) = self.list[self.idx].clone();
        if time >= self.range.end { return None; }

        if time + c.len > self.range.end {
            c.truncate(self.range.end - time);
        }

        if time < self.range.start {
            c.truncate_keeping_right(self.range.start - time);
            time = self.range.start;
        }

        self.idx += 1;
        Some(KVPair(time, c))
    }
}

impl<'a> OpIter<'a> {
    fn new(list: &'a RleVec<KVPair<PositionalComponent>>, range: TimeSpan) -> Self {
        OpIter {
            list: &list,
            idx: list.find_index(range.start).unwrap(),
            range
        }
    }
}

impl OpSet {
    pub(crate) fn iter_ops(&self, range: TimeSpan) -> OpIter {
        OpIter::new(&self.operations, range)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::list::operation::{InsDelTag, PositionalComponent};
    use crate::rle::{KVPair, RleVec};
    use InsDelTag::*;

    #[test]
    fn iter_smoke() {
        let mut ops: RleVec<KVPair<PositionalComponent>> = RleVec::new();

        ops.push(KVPair(0, PositionalComponent {
            pos: 100,
            len: 10,
            rev: false, content_known: false, tag: Ins
        }));
        ops.push(KVPair(10, PositionalComponent {
            pos: 200,
            len: 20,
            rev: false, content_known: false, tag: Del
        }));

        assert_eq!(OpIter::new(&ops, (0..30).into()).collect::<Vec<_>>(), ops.0.as_slice());

        assert_eq!(OpIter::new(&ops, (1..5).into()).collect::<Vec<_>>(), &[KVPair(1, PositionalComponent {
            pos: 101,
            len: 4,
            rev: false, content_known: false, tag: Ins,
        })]);

        assert_eq!(OpIter::new(&ops, (6..16).into()).collect::<Vec<_>>(), &[
            KVPair(6, PositionalComponent {
                pos: 106,
                len: 4,
                rev: false, content_known: false, tag: Ins,
            }),
            KVPair(10, PositionalComponent {
                pos: 200,
                len: 6,
                rev: false, content_known: false, tag: Del,
            }),
        ]);
    }
}
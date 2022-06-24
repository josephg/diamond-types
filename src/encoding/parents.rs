use crate::encoding::RlePackWriteCursor;
use crate::history::MinimalHistoryEntry;

#[derive(Debug, Default)]
struct ParentsCursor;

impl RlePackWriteCursor for ParentsCursor {
    type Item = MinimalHistoryEntry;

    fn write_and_advance(&mut self, item: &Self::Item, dest: &mut Vec<u8>) {
        todo!()
    }
}
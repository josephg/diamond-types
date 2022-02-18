use content_tree::{ContentLength, Cursor, DEFAULT_IE, DEFAULT_LE, FindContent, Pair, TreeMetrics};
use crate::list::merge::yjsspan::YjsSpan;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MarkerMetrics;

/// This is a helper trait to describe the indexes for YjsSpan items we're merging.
///
/// For each item we keep track of two values:
///
/// - The "current content length" - which is 1 for each item in the INSERTED state, and 0 for
///   anything else. (This is field 0 in each internal pair)
/// - The "upstream length". This is the length / position in the document we're merging into. This
///   is 0 if the item has ever been deleted, or 1 otherwise. (Because anything we've merged must
///   have been inserted at some point. (This is field 1).
///
/// The current length is tagged as "content length" to make cursor utility methods easier to use.
impl TreeMetrics<YjsSpan> for MarkerMetrics {
    type Update = Pair<isize>;
    type Value = Pair<usize>;

    fn increment_marker(marker: &mut Self::Update, entry: &YjsSpan) {
        marker.0 += entry.content_len() as isize;
        marker.1 += entry.upstream_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &YjsSpan) {
        marker.0 -= entry.content_len() as isize;
        marker.1 -= entry.upstream_len() as isize;
    }

    fn decrement_marker_by_val(marker: &mut Self::Update, val: &Self::Value) {
        marker.0 -= val.0 as isize;
        marker.1 -= val.1 as isize;
    }

    fn update_offset_by_marker(offset: &mut Self::Value, by: &Self::Update) {
        offset.0 = offset.0.wrapping_add(by.0 as usize);
        offset.1 = offset.1.wrapping_add(by.1 as usize);
    }

    fn increment_offset(offset: &mut Self::Value, by: &YjsSpan) {
        offset.0 += by.content_len();
        offset.1 += by.upstream_len();
    }
}

impl FindContent<YjsSpan> for MarkerMetrics {
    fn index_to_content(offset: Self::Value) -> usize {
        offset.0
    }
}

impl MarkerMetrics {
    pub(super) fn upstream_len(offset: <Self as TreeMetrics<YjsSpan>>::Value) -> usize {
        offset.1
    }
}

/// Get the upstream position of a cursor into a MarkerMetrics object. I'm not sure if this is the
/// best place for this method, but it'll do.
pub(super) fn upstream_cursor_pos(cursor: &Cursor<YjsSpan, MarkerMetrics, DEFAULT_IE, DEFAULT_LE>) -> usize {
    cursor.count_pos_raw(MarkerMetrics::upstream_len,
                         YjsSpan::upstream_len,
                         YjsSpan::upstream_len_at)
}

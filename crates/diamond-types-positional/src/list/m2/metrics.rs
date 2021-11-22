use content_tree::{ContentLength, FindContent, FindOffset, Pair, TreeMetrics};
use crate::list::m2::yjsspan2::YjsSpan2;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MarkerMetrics;

/// Item 0 is the current content length. This is returned to queries by content length,
/// Item 1 is the upstream length. Ie, the length once everything has been applied. This is tagged
/// as offset length, though the logic is sometimes a bit twisted.
impl TreeMetrics<YjsSpan2> for MarkerMetrics {
    type Update = Pair<isize>;
    type Value = Pair<usize>;

    fn increment_marker(marker: &mut Self::Update, entry: &YjsSpan2) {
        marker.0 += entry.content_len() as isize;
        marker.1 += entry.upstream_len() as isize;
    }

    fn decrement_marker(marker: &mut Self::Update, entry: &YjsSpan2) {
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

    fn increment_offset(offset: &mut Self::Value, by: &YjsSpan2) {
        offset.0 += by.content_len();
        offset.1 += by.upstream_len();
    }
}

impl FindContent<YjsSpan2> for MarkerMetrics {
    fn index_to_content(offset: Self::Value) -> usize {
        offset.0
    }
}

// This is cheating a little to reuse a bunch of methods in ContentTree.
impl FindOffset<YjsSpan2> for MarkerMetrics {
    fn index_to_offset(offset: Self::Value) -> usize {
        offset.1
    }
}
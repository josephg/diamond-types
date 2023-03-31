use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::{Index, IndexMut};
use smallvec::{Array, SmallVec, smallvec};
use crate::causalgraph::graph::Graph;
use crate::dtrange::DTRange;
use crate::LV;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use crate::causalgraph::graph::tools::DiffFlag;

/// A `LocalFrontier` is a set of local Time values which point at the set of changes with no
/// children at this point in time. When there's a single writer this will always just be the last
/// local version we've seen.
///
/// The start of time is named with an empty list.
///
/// A frontier must always remain sorted (in numerical order). Note: This is not checked when
/// deserializing via serde!
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(transparent))]
pub struct Frontier(pub SmallVec<[LV; 2]>);

pub type FrontierRef<'a> = &'a [LV];

impl AsRef<[LV]> for Frontier {
    fn as_ref(&self) -> &[LV] {
        self.0.as_slice()
    }
}

impl<'a> From<FrontierRef<'a>> for Frontier {
    fn from(f: FrontierRef<'a>) -> Self {
        // This is a bit dangerous - but we still verify that the data is sorted in debug mode...
        Frontier::from_sorted(f)
    }
}

impl From<SmallVec<[LV; 2]>> for Frontier {
    fn from(f: SmallVec<[LV; 2]>) -> Self {
        debug_assert_frontier_sorted(f.as_slice());
        Frontier(f)
    }
}

impl From<LV> for Frontier {
    fn from(v: LV) -> Self {
        Frontier::new_1(v)
    }
}

impl Default for Frontier {
    fn default() -> Self {
        Self::root()
    }
}

impl Index<usize> for Frontier {
    type Output = LV;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl IndexMut<usize> for Frontier {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

// Helper method. Not sure where to put this.
pub(crate) fn is_sorted_iter<const EXPECT_UNIQ: bool, V: Ord + Eq + Debug, I: Iterator<Item = V>>(mut iter: I) -> bool {
    let Some(mut last) = iter.next() else { return true; };

    for i in iter {
        if EXPECT_UNIQ {
            debug_assert_ne!(i, last);
        }
        if i <= last { return false; }
        last = i;
    }

    true
}
pub(crate) fn is_sorted_iter_uniq<V: Ord + Eq + Debug, I: Iterator<Item = V>>(iter: I) -> bool {
    is_sorted_iter::<true, V, I>(iter)
}

pub(crate) fn is_sorted_slice<const EXPECT_UNIQ: bool, V: Ord + Eq + Debug + Copy>(slice: &[V]) -> bool {
    if slice.len() >= 2 {
        let mut last = slice[0];
        for t in &slice[1..] {
            if EXPECT_UNIQ {
                debug_assert!(*t != last);
            }
            if last >= *t { return false; }
            last = *t;
        }
    }
    true
}

pub(crate) fn frontier_is_sorted(f: FrontierRef) -> bool {
    // is_sorted_iter(f.iter().copied())
    is_sorted_slice::<true, _>(f)
}

pub(crate) fn debug_assert_frontier_sorted(frontier: FrontierRef) {
    debug_assert!(frontier_is_sorted(frontier));
}

pub(crate) fn sort_frontier<T: Array<Item=LV>>(v: &mut SmallVec<T>) {
    if !frontier_is_sorted(v.as_slice()) {
        v.sort_unstable();
    }
}

impl IntoIterator for Frontier {
    type Item = LV;
    type IntoIter = <SmallVec<[LV; 2]> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<LV> for Frontier {
    fn from_iter<T: IntoIterator<Item=LV>>(iter: T) -> Self {
        Frontier::from_unsorted_iter(iter.into_iter())
    }
}

impl Frontier {
    pub fn root() -> Self {
        Self(smallvec![])
    }

    pub fn new_1(v: LV) -> Self {
        Self(smallvec![v])
    }

    pub fn from_unsorted(data: &[LV]) -> Self {
        let mut arr: SmallVec<[LV; 2]> = data.into();
        sort_frontier(&mut arr);
        Self(arr)
    }

    pub fn from_unsorted_iter<I: Iterator<Item=LV>>(iter: I) -> Self {
        let mut arr: SmallVec<[LV; 2]> = iter.collect();
        sort_frontier(&mut arr);
        Self(arr)
    }

    pub fn from_sorted(data: &[LV]) -> Self {
        debug_assert_frontier_sorted(data);
        Self(data.into())
    }

    /// Frontiers should always be sorted smallest to largest.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<usize> {
        self.0.iter()
    }

    pub fn try_get_single_entry(&self) -> Option<LV> {
        if self.len() == 1 { Some(self.0[0]) }
        else { None }
    }

    pub fn try_get_single_entry_mut(&mut self) -> Option<&mut LV> {
        if self.len() == 1 { Some(&mut self.0[0]) }
        else { None }
    }

    pub fn replace(&mut self, with: FrontierRef) {
        // TODO: Is this faster than *self = with.into(); ?
        self.0.resize(with.len(), 0);
        self.0.copy_from_slice(with);
    }

    pub fn debug_check_sorted(&self) {
        debug_assert_frontier_sorted(self.0.borrow());
    }


    /// Advance a frontier by the set of time spans in range
    pub fn advance(&mut self, graph: &Graph, mut range: DTRange) {
        // This is a little crass. Might be nicer to use a &T iterator in RLEVec.
        let txn_idx = graph.entries.find_index(range.start).unwrap();

        for txn in &graph.entries[txn_idx..] {
            debug_assert!(txn.contains(range.start));

            let end = txn.span.end.min(range.end);
            txn.with_parents(range.start, |parents| {
                self.advance_by_known_run(parents, (range.start..end).into());
            });

            if end >= range.end { break; }
            range.start = end;
        }
    }

    /// Just like advance_by_known_run, the range MUST be in a single transaction in the graph.
    pub fn advance_sparse_known_run(&mut self, graph: &Graph, parents: &[LV], range: DTRange) {
        // Could copy the other cases from advance_by_known_run... eh.
        if self.as_ref() == parents {
            // Fastest path. We're just extending the span.
            self.replace_with_1(range.last());
        } else {
            // We'll probably still replace the version with range.last(), but there's some edge
            // cases for find_dominators to figure out.
            self.0 = graph.find_dominators_2(self.as_ref(), &[range.last()]).0;
        }
    }

    pub fn advance_sparse(&mut self, graph: &Graph, range: DTRange) {
        let txn_idx = graph.entries.find_index(range.start).unwrap();
        let first_txn = &graph.entries[txn_idx];
        if first_txn.span.end >= range.end {
            // Fast path.
            first_txn.with_parents(range.start, |parents| {
                self.advance_sparse_known_run(graph, parents, range);
            })
        } else {
            // This is a lot more complicated than I'd like, but I think its the fastest approach
            // here. We'll make a frontier from from the transactions within the range, then merge
            // that with the current frontier.
            let mut f2 = Frontier::root();
            f2.advance(graph, range); // This is a bit cheeky, but the result should be correct.
            // And merge that together. This will usually just return f2.
            self.0 = graph.find_dominators_2(self.as_ref(), f2.as_ref()).0;
        }
    }

    /// Advance branch frontier by a transaction.
    ///
    /// This is ONLY VALID if the range is entirely within a txn.
    pub fn advance_by_known_run(&mut self, parents: &[LV], span: DTRange) {
        // TODO: Check the branch contains everything in txn_parents, but not txn_id:
        // Check the operation fits. The operation should not be in the branch, but
        // all the operation's parents should be.
        // From braid-kernel:
        // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
        // for (const parent of op.parents) {
        //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
        // }

        if parents.len() == 1 && self.0.len() == 1 && parents[0] == self.0[0] {
            // Short circuit the common case where time is just advancing linearly.
            self.0[0] = span.last();
        } else if self.0.as_slice() == parents {
            self.replace_with_1(span.last());
        } else {
            assert!(!self.0.contains(&span.start)); // Remove this when branch_contains_version works.
            debug_assert_frontier_sorted(self.0.as_slice());

            self.0.retain(|o| !parents.contains(o)); // Usually removes all elements.

            // In order to maintain the order of items in the branch, we want to insert the new item
            // in the appropriate place. This will almost always do self.0.push(), but when changes
            // are concurrent that won't be correct. (Do it and run the tests if you don't believe
            // me).
            // TODO: Check if its faster to try and append it to the end first.
            self.insert_nonoverlapping(span.last());
        }
    }

    pub fn merge_union(&mut self, other: &[LV], graph: &Graph) {
        if !other.is_empty()
            && other != self.as_ref()
            && (other.len() != 1 || !graph.frontier_contains_version(self.as_ref(), other[0]))
        {
            *self = graph.version_union(self.as_ref(), other);
        }
    }

    pub fn retreat(&mut self, graph: &Graph, mut range: DTRange) {
        if range.is_empty() { return; }

        self.debug_check_sorted();

        let mut txn_idx = graph.entries.find_index(range.last()).unwrap();
        loop {
            let last_order = range.last();
            let txn = &graph.entries[txn_idx];
            // debug_assert_eq!(txn_idx, history.0.find_index(range.last()).unwrap());
            debug_assert_eq!(txn, graph.entries.find(last_order).unwrap());
            // let mut idx = frontier.iter().position(|&e| e == last_order).unwrap();

            if self.len() == 1 {
                // Fast case. Just replace frontier's contents with parents.
                if range.start > txn.span.start {
                    self[0] = range.start - 1;
                    break;
                } else {
                    // self.0 = txn.parents.as_ref().into();
                    *self = txn.parents.clone()
                }
            } else {
                // Remove the old item from frontier and only reinsert parents when they aren't included
                // in the transitive history from this point.
                self.0.retain(|t| *t != last_order);

                txn.with_parents(range.start, |parents| {
                    for parent in parents {
                        // TODO: This is pretty inefficient. We're calling frontier_contains_time in a
                        // loop and each call to frontier_contains_time does a call to history.find() in
                        // turn for each item in branch.
                        debug_assert!(!self.is_root());
                        // TODO: At least check shadow directly.
                        if !graph.frontier_contains_version(self.as_ref(), *parent) {
                            self.insert_nonoverlapping(*parent);
                        }
                    }
                });
            }

            if range.start >= txn.span.start {
                break;
            }

            // Otherwise keep scanning down through the txns.
            range.end = txn.span.start;
            txn_idx -= 1;
        }
        if cfg!(debug_assertions) { self.check(graph); }
        self.debug_check_sorted();
    }

    fn insert_nonoverlapping(&mut self, new_item: LV) {
        // In order to maintain the order of items in the branch, we want to insert the new item in the
        // appropriate place.

        // Binary search might actually be slower here than a linear scan.
        let new_idx = self.0.binary_search(&new_item).unwrap_err();
        self.0.insert(new_idx, new_item);

        // match self.0.last() {
        //     Some(v) if *v < new_item => { self.0.push(new_item); }
        //     None => { self.0.push(new_item); }
        //     _ => {
        //         let new_idx = self.0.binary_search(&new_item).unwrap_err();
        //         self.0.insert(new_idx, new_item);
        //     }
        // }

        self.debug_check_sorted();
    }

    pub fn insert(&mut self, new_item: LV) {
        // And we're returning in the Ok() case here because it means the item is already in the
        // frontier.
        let Err(new_idx) = self.0.binary_search(&new_item) else { return; };
        self.0.insert(new_idx, new_item);
        self.debug_check_sorted();
    }

    pub(crate) fn check(&self, parents: &Graph) {
        assert!(frontier_is_sorted(&self.0));
        if self.len() >= 2 {
            let dominators = parents.find_dominators(&self.0);
            assert_eq!(&dominators, self);
            // let mut self = self.iter().copied().collect::<Vec<_>>();
            // let mut self = self.0.to_vec();
            // for i in 0..self.len() {
            //     let removed = self.remove(i);
            //     assert!(!history.version_contains_time(&self, removed));
            //     self.insert(i, removed);
            // }
        }
    }

    pub fn replace_with_1(&mut self, new_val: LV) {
        // I could truncate / etc, but this is faster in benchmarks.
        // replace(&mut self.0, smallvec::smallvec![new_val]);
        self.0 = smallvec::smallvec![new_val];
    }
}

pub fn local_frontier_eq<A: AsRef<[LV]> + ?Sized, B: AsRef<[LV]> + ?Sized>(a: &A, b: &B) -> bool {
    // Almost all branches only have one element in them.
    debug_assert_frontier_sorted(a.as_ref());
    debug_assert_frontier_sorted(b.as_ref());
    a.as_ref() == b.as_ref()
}

#[allow(unused)]
pub fn local_frontier_is_root(branch: &[LV]) -> bool {
    branch.is_empty()
}


// This walks both frontiers and finds how the frontier has changed. There's probably a better way
// to implement this.
struct FrontierDiff<'a> {
    a: &'a [LV],
    b: &'a [LV],
}

pub(crate) fn diff_frontier_entries<'a>(a: &'a [LV], b: &'a [LV]) -> impl Iterator<Item = (DiffFlag, LV)> + 'a {
    FrontierDiff { a, b }
}


fn slice_take_first(slice: &mut &[LV]) -> Option<LV> {
    if let [first, tail @ ..] = slice {
        *slice = tail;
        Some(*first)
    } else { None }
}

impl<'a> Iterator for FrontierDiff<'a> {
    type Item = (DiffFlag, LV);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.split_first(), self.b.split_first()) {
            (None, None) => None,
            (Some((a, rest)), None) => {
                self.a = rest;
                Some((DiffFlag::OnlyA, *a))
            },
            (None, Some((b, rest))) => {
                self.b = rest;
                Some((DiffFlag::OnlyB, *b))
            },
            (Some((a, a_rest)), Some((b, b_rest))) => {
                match a.cmp(b) {
                    Ordering::Equal => {
                        // Take from both.
                        self.a = a_rest;
                        self.b = b_rest;
                        Some((DiffFlag::Shared, *a))
                    }
                    Ordering::Less => {
                        // Take from a.
                        self.a = a_rest;
                        Some((DiffFlag::OnlyA, *a))
                    }
                    Ordering::Greater => {
                        // Take from b.
                        self.b = b_rest;
                        Some((DiffFlag::OnlyB, *a))
                    }
                }
            }
        }
    }
}

/// This method clones a version or parents vector. Its slightly faster and smaller than just
/// calling v.clone() directly.
#[inline]
pub fn clone_smallvec<T, const LEN: usize>(v: &SmallVec<[T; LEN]>) -> SmallVec<[T; LEN]> where T: Clone + Copy {
    // This is now smaller again as of rust 1.60. Looks like the problem was fixed.
    v.clone()

    // if v.spilled() { // Unlikely. If only there was a stable rust intrinsic for this..
    //     v.clone()
    // } else {
    //     unsafe {
    //         // We only need to copy v.len() items, because LEN is small (2, usually) its actually
    //         // faster & less code to just copy the bytes in all cases rather than branch.
    //         // let mut arr: MaybeUninit<[T; LEN]> = MaybeUninit::uninit();
    //         // std::ptr::copy_nonoverlapping(v.as_ptr(), arr.as_mut_ptr().cast(), LEN);
    //         // SmallVec::from_buf_and_len_unchecked(arr, v.len())
    //
    //         let mut result: MaybeUninit<SmallVec<[T; LEN]>> = MaybeUninit::uninit();
    //         std::ptr::copy_nonoverlapping(v, result.as_mut_ptr(), 1);
    //         result.assume_init()
    //     }
    // }
}

#[cfg(test)]
mod test {
    use smallvec::smallvec;
    use crate::causalgraph::graph::GraphEntrySimple;

    use crate::Frontier;
    use super::*;

    #[test]
    fn frontier_movement_smoke_tests() {
        let mut branch: Frontier = Frontier::root();
        branch.advance_by_known_run(&[], (0..10).into());
        assert_eq!(branch.as_ref(), &[9]);

        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..10).into(), parents: Frontier::root() }
        ]);
        graph.dbg_check(true);

        branch.retreat(&graph, (5..10).into());
        assert_eq!(branch.as_ref(), &[4]);

        branch.retreat(&graph, (0..5).into());
        assert!(branch.is_root());
    }

    #[test]
    fn frontier_stays_sorted() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..2).into(), parents: Frontier::root() },
            GraphEntrySimple { span: (2..6).into(), parents: Frontier::new_1(0) },
            GraphEntrySimple { span: (6..50).into(), parents: Frontier::new_1(0) },
        ]);
        graph.dbg_check(true);

        let mut branch: Frontier = Frontier::from_sorted(&[1, 10]);
        branch.advance(&graph, (2..4).into());
        assert_eq!(branch.as_ref(), &[1, 3, 10]);

        branch.advance(&graph, (11..12).into());
        assert_eq!(branch.as_ref(), &[1, 3, 11]);

        branch.retreat(&graph, (2..4).into());
        assert_eq!(branch.as_ref(), &[1, 11]);

        branch.retreat(&graph, (11..12).into());
        assert_eq!(branch.as_ref(), &[1, 10]);
    }

    #[test]
    fn advance_sparse() {
        let graph = Graph::from_simple_items(&[
            GraphEntrySimple { span: (0..10).into(), parents: Frontier::root() },
            GraphEntrySimple { span: (10..20).into(), parents: Frontier::new_1(5) },
            // GraphEntrySimple { span: (6..50).into(), parents: Frontier::new_1(0) },
        ]);
        graph.dbg_check(true);

        // This isn't thorough, but should be good enough.
        let mut f = Frontier::root();
        f.advance_sparse(&graph, (0..5).into());
        // Should only include subgraph items
        assert_eq!(f.as_ref(), &[4]);

        f.advance_sparse(&graph, (7..8).into());
        assert_eq!(f.as_ref(), &[7]);

        f.advance_sparse(&graph, (9..15).into());
        assert_eq!(f.as_ref(), &[9, 14]);
    }

    #[test]
    fn advance_empty_by_known_run() {
        // Regression.
        // let graph = Graph::from_entries(&[
        //     GraphEntrySimple { span: (0..10).into(), parents: Frontier::root(), },
        //  ];

        let mut f = Frontier::root();
        f.insert_nonoverlapping(4);
        assert_eq!(f.as_ref(), &[4]);
    }
}

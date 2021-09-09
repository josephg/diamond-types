// An entry is expected to contain multiple items.
pub trait SplitableSpan: Clone {
    /// The number of child items in the entry. This is indexed with the size used in truncate.
    fn len(&self) -> usize;

    /// Split the entry, returning the part of the entry which was jettisoned. After truncating at
    /// `pos`, self.len() == `pos` and the returned value contains the rest of the items.
    ///
    /// ```ignore
    /// let initial_len = entry.len();
    /// let rest = entry.truncate(truncate_at);
    /// assert!(initial_len == truncate_at + rest.len());
    /// ```
    ///
    /// `at` parameter must obey *0 < at < entry.len()*
    fn truncate(&mut self, at: usize) -> Self;

    // This is strictly unnecessary given truncate(), but it makes some code cleaner.
    // fn truncate_keeping_right(&mut self, at: usize) -> Self;
    fn truncate_keeping_right(&mut self, at: usize) -> Self {
        let mut other = self.clone();
        *self = other.truncate(at);
        other
    }

    /// See if the other item can be appended to self. `can_append` will always be called
    /// immediately before `append`.
    fn can_append(&self, other: &Self) -> bool;
    fn append(&mut self, other: Self);
    fn prepend(&mut self, other: Self);
    // fn prepend(&mut self, mut other: Self) {
    //     other.append(*self);
    //     *self = other;
    // }
}

/// Simple example where entries are runs of positive or negative items. This is used for testing
/// and for the encoder.
impl SplitableSpan for i32 {
    // type Item = bool; // Negative runs = false, positive = true.

    fn len(&self) -> usize {
        self.abs() as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at = at as i32;
        // dbg!(at, *self);
        debug_assert!(at > 0 && at < self.abs());
        debug_assert_ne!(*self, 0);

        let abs = self.abs();
        let sign = self.signum();
        *self = at * sign;

        (abs - at) * sign
    }

    // fn can_append(&self, other: &Self) -> bool {
    //     self.signum() == other.signum()
    // }
    fn can_append(&self, other: &Self) -> bool {
        (*self >= 0) == (*other >= 0)
    }

    fn append(&mut self, other: Self) {
        debug_assert!(self.can_append(&other));
        *self += other;
    }

    fn prepend(&mut self, other: Self) {
        self.append(other);
    }
}

// /// A splitablespan in reverse. This is useful for lists made in descending order.
// #[derive(Copy, Clone, Debug, PartialEq, Eq)]
// pub struct ReverseSpan<S: SplitableSpan + Clone>(pub S);
//
// impl<S: SplitableSpan + Clone> SplitableSpan for ReverseSpan<S> {
//     fn len(&self) -> usize { self.0.len() }
//
//     fn truncate(&mut self, at: usize) -> Self {
//         panic!("Cannot truncate ReverseSpan");
//     }
//
//     fn can_append(&self, other: &Self) -> bool { other.0.can_append(&self.0) }
//     fn append(&mut self, other: Self) { self.0.prepend(other.0); }
//     fn prepend(&mut self, other: Self) { self.0.append(other.0); }
// }

/// Simple test helper to verify an implementation of SplitableSpan is valid and meets expected
/// constraints.
#[cfg(test)]
pub fn test_splitable_methods_valid<E: SplitableSpan + std::fmt::Debug + Clone + Eq>(entry: E) {
    assert!(entry.len() >= 2, "Call this with a larger entry");
    for i in 1..entry.len() {
        // Split here and make sure we get the expected results.
        let mut start = entry.clone();
        let end = start.truncate(i);

        assert_eq!(start.len(), i);
        assert_eq!(end.len(), entry.len() - i);

        assert!(start.can_append(&end));

        let mut merge_append = start.clone();
        merge_append.append(end.clone());
        assert_eq!(merge_append, entry);

        let mut merge_prepend = end.clone();
        merge_prepend.prepend(start.clone());
        assert_eq!(merge_prepend, entry);

        // Split using truncate_keeping_right. We should get the same behaviour.
        let mut end2 = entry.clone();
        let start2 = end2.truncate_keeping_right(i);
        assert_eq!(end2, end);
        assert_eq!(start2, start);
    }
}

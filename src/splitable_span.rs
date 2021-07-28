
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

    // This is pretty gross. Hopefully the optimizer can sort this out.
    // #[inline]
    // fn truncate_keeping_right(&mut self, at: usize) -> Self {
    //     let b = self.truncate(at);
    //     let remainder = self;
    //     *self = b;
    //     remainder
    // }

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
        return self.abs() as usize;
    }

    fn truncate(&mut self, at: usize) -> Self {
        let at = at as i32;
        // dbg!(at, *self);
        debug_assert!(at > 0 && at < self.abs());
        debug_assert_ne!(*self, 0);

        let abs = self.abs();
        let sign = self.signum();
        *self = at * sign;
        return (abs - at) * sign;
    }

    fn can_append(&self, other: &Self) -> bool {
        self.signum() == other.signum()
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
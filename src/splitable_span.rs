
// An entry is expected to contain multiple items.
pub trait SplitableSpan {
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

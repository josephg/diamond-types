//! A shelf is a value tagged with a version. You can think of them as a tuple in the form `(VALUE, VERSION)`.
//!
//! The rules for merging two shelves `(A, A#)` and `(B, B#)` are as follows:
//!
//! 1. If `A#` is greater than `B#`, return `(A, A#)`.
//! 2. If `B#` is greater than `A#`, return `(B, B#)`.
//! 3. If `A` and `B` are both objects, recursively merge all keys, return `(X, B#)`
//! 4. Otherwise, pick `(B, B#)`.

use std::hash::{Hash, Hasher};
use std::{cmp::Ordering, collections::BTreeMap};

use fxhash::hash32;
use serde::{Deserialize, Serialize};

pub type ItemMap<T> = BTreeMap<String, Shelf<T>>;

/// An item in a shelf.
///
/// If the item is an `Item::Map`, then it will be recursively merged when two shelves have the same version.
///
/// # Examples
///
/// ```
/// use shelf::{Item, ItemMap};
///
/// let a: Item<usize> = Item::Value(42);
/// let b: Item<usize> = 42.into();
/// assert_eq!(a, b);
///
/// let mut map: ItemMap<usize> = ItemMap::new();
/// map.insert("a".into(), 42.into());
/// map.insert("b".into(), 43.into());
/// let item: Item<usize> = map.into();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Item<T>
where
    T: Hash,
{
    Value(T),
    Map(ItemMap<T>),
}

impl<T> PartialEq for Item<T>
where
    T: Hash,
{
    fn eq(&self, other: &Self) -> bool {
        // Relies on hashing, rather than PartialEq, so that PartialEq does not need to be implemented for T.
        hash32(self) == hash32(other)
    }
}

impl<T> Hash for Item<T>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        // WARNING: Changing order of fields will change order of hashes used to merge shelves.
        //          Therefore, it must be considered a breaking change.
        core::mem::discriminant(self).hash(state);
        match self {
            Item::Map(map) => map.hash(state),
            Item::Value(v) => v.hash(state),
        };
    }
}

impl<T> From<T> for Item<T>
where
    T: Hash,
{
    /// Creates a new shelf with a version of 0
    fn from(value: T) -> Self {
        Self::Value(value)
    }
}

impl<T> From<ItemMap<T>> for Item<T>
where
    T: Hash,
{
    /// Creates a new shelf with a version of 0
    fn from(value: ItemMap<T>) -> Self {
        Item::Map(value)
    }
}

/// A shelf is a value tagged with a version. They follow a deterministic set of rules when merged.
///
/// Shelves contain a single `Item::Value`. If this is an `Item::Map`, its values will get recursively merged.
/// The rules for merging two shelves `(A, A#)` and `(B, B#)` are as follows:
///
/// 1. If `A#` is greater than `B#`, return `(A, A#)`.
/// 2. If `B#` is greater than `A#`, return `(B, B#)`.
/// 3. If `A` and `B` are both objects, recursively merge all keys, return `(X, B#)`
/// 4. Otherwise, pick `(B, B#)`.
///
/// # Examples
/// ```
/// use shelf::Shelf;
///
/// // Create a shelf with a version of 0
/// let a: Shelf<usize> = 42.into();
/// // Create a shelf with a version of 1
/// let b: Shelf<usize> = Shelf::new(43.into(), 1);
/// // Merging consumes both shelves to avoid allocation
/// let merged = a.merge(b);
/// assert_eq!(merged, Shelf::new(43.into(), 1));
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shelf<T>
where
    T: Hash,
{
    value: Item<T>,
    version: usize,
}

impl<T> Shelf<T>
where
    T: Hash,
{
    /// Creates a new shelf with a specified version
    pub fn new(value: Item<T>, version: usize) -> Self {
        Self { value, version }
    }

    /// Sets a new value within the shelf, increasing the version by one.
    pub fn set(&mut self, value: Item<T>) {
        self.value = value;
        self.version += 1;
    }

    /// Recursively merges this shelf with another.
    ///
    /// Both the shelves are consumed in this operation, to avoid allocation.
    /// The rules for merging two shelves – `(A, A#)` and `(B, B#)` are as follows:
    ///
    /// 1. If `A#` is greater than `B#`, return `(A, A#)`.
    /// 2. If `B#` is greater than `A#`, return `(B, B#)`.
    /// 3. If `A` and `B` are both objects, recursively merge all keys, return `(X, B#)`
    /// 4. Otherwise, pick `(B, B#)`.
    pub fn merge(mut self, other: Shelf<T>) -> Shelf<T> {
        match self.version.cmp(&other.version) {
            // If A# > B#, pick A
            Ordering::Greater => self,
            // If A# < B#, pick B
            Ordering::Less => other,
            // If A# == B#, try to recursively merge
            Ordering::Equal => {
                if let Item::Map(map_a) = &mut self.value {
                    if let Item::Map(map_b) = other.value {
                        // Both A and B are maps, so recursively merge
                        for (key, value_b) in map_b.into_iter() {
                            if let Some(value_a) = map_a.remove(&key) {
                                let new_value = value_a.merge(value_b);
                                map_a.insert(key, new_value);
                            } else {
                                map_a.insert(key, value_b);
                            }
                        }
                        return self;
                    }
                }

                // Fallback case: hash the shelves to generate a predictable result, regardless of the order in which they are passed to the function.
                // The hash algorithm must be consistent across architectures and platforms, as the tie between shelves must always be broken with the same result.
                // That is why the DefaultHasher supplied by Rust is not used – the algorithm is not defined and can vary across versions.
                let self_hash = hash32(&self.value);
                let other_hash = hash32(&other.value);

                if self_hash > other_hash {
                    self
                } else {
                    other
                }
            }
        }
    }
}

impl<T> Hash for Shelf<T>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        // WARNING: Changing order of fields will change order of hashes used to merge shelves.
        //          Therefore, it must be considered a breaking change.
        self.value.hash(state);
        self.version.hash(state);
    }
}

impl<T> PartialEq for Shelf<T>
where
    T: Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.version == other.version
    }
}

impl<T> From<T> for Shelf<T>
where
    T: Hash,
{
    /// Creates a new shelf with a version of 0
    fn from(value: T) -> Self {
        Self::new(Item::from(value), 0)
    }
}

impl<T> From<ItemMap<T>> for Shelf<T>
where
    T: Hash,
{
    /// Creates a new shelf with a version of 0
    fn from(value: ItemMap<T>) -> Self {
        Self::new(Item::from(value), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge() {
        let a = Shelf::new(42.into(), 0);
        let b = Shelf::new(43.into(), 1);
        // When A# > B#, merging returns B
        assert_eq!(a.clone().merge(b.clone()), b);
        // When B# > A#, merging returns B
        assert_eq!(b.clone().merge(a), b);
    }

    #[test]
    fn merge_conflict() {
        let a = Shelf::new(42.into(), 0);
        let b = Shelf::new(43.into(), 0);
        // When A# == B#, merging returns a deterministic result
        assert_eq!(a.clone().merge(b.clone()), b);
        // When B# == A#, merging returns a deterministic result
        assert_eq!(b.clone().merge(a.clone()), b);
        // B wins because the hash of it's item is greater
        assert!(hash32(&b.value) > hash32(&a.value));
    }

    #[test]
    fn merge_recursive() {
        // Tests:
        //  - When one version is later
        //  - When versions and data are the same
        //  - When keys are missing
        //  - When versions are the same, and both are objects

        // From the README of https://github.com/dglittle/shelf
        // let a = r"[{a: [42, 0], b: [42, 0], c: [42, 0]}, 0]";
        // let b = r"[{a: [42, 0],             c: [43, 1]}, 0]";
        // let expected = r"[{a: [42, 0], b: [42, 0], c: [43, 1]}, 0]";

        let mut a_map: BTreeMap<String, Shelf<usize>> = BTreeMap::new();
        a_map.insert("a".into(), 42.into());
        a_map.insert("b".into(), 42.into());
        a_map.insert("c".into(), 42.into());
        let a_shelf: Shelf<usize> = a_map.into();

        let mut b_map: BTreeMap<String, Shelf<usize>> = BTreeMap::new();
        b_map.insert("a".into(), 42.into());
        b_map.insert("c".into(), Shelf::new(43.into(), 1));
        let b_shelf: Shelf<usize> = b_map.into();

        let mut expected_map: BTreeMap<String, Shelf<usize>> = BTreeMap::new();
        expected_map.insert("a".into(), 42.into());
        expected_map.insert("b".into(), 42.into());
        expected_map.insert("c".into(), Shelf::new(43.into(), 1));
        let expected_shelf: Shelf<usize> = expected_map.into();

        let actual_shelf = a_shelf.merge(b_shelf);
        assert_eq!(actual_shelf, expected_shelf);
    }

    #[test]
    #[allow(clippy::eq_op)]
    fn equality() {
        let a: Shelf<usize> = 42.into();
        let b: Shelf<usize> = Shelf::new(43.into(), 1);
        assert_ne!(a, b);
        assert_ne!(b, a);
        assert_eq!(a, a);
        assert_eq!(b, b);
    }
}

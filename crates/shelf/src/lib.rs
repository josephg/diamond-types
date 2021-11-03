// use std::collections::BTreeMap;
//
// #[derive(Clone, Debug, PartialEq)]
// enum DynShelfItem {
//     Null,
//     Bool(bool),
//     Int(i64),
//     Float(f64),
//     Str(String),
//     Map(BTreeMap<String, DynShelfItem>),
//     Tuple(Vec<DynShelfItem>),
// }
//
// #[derive(Clone, Debug, PartialEq, Eq)]
// enum DynShelfVersion {
//     Scalar(u64),
//     Map(BTreeMap<String, DynShelfVersion>),
//     Tuple(Vec<DynShelfVersion>),
// }
//
// #[derive(Clone, Debug, PartialEq)]
// struct DynShelf {
//     data: DynShelfItem,
//     versions: DynShelfVersion,
//     next_seq: usize,
// }
//
// #[derive(Clone, Debug, PartialEq, Eq)]
// enum PathItem {
//     Str(String), // For maps
//     Index(usize), // For lists (tuples)
// }
//
// impl DynShelf {
//     fn new_map() -> Self {
//         DynShelf {
//             data: DynShelfItem::Map(Default::default()),
//             versions: DynShelfVersion::Map(Default::default()),
//             next_seq: 1,
//         }
//     }
//
//     fn insert(&mut self, path: &[PathItem], new_value: DynShelfItem) {
//         let mut d = &mut self.data;
//         let mut v = &mut self.versions;
//         for p in path {
//             match (p, d, v) {
//                 (PathItem::Str(path), DynShelfItem::Map(val), DynShelfVersion::Map(version)) => {
//                     if let Some(v) = val.get_mut() {
//                         d = v;
//                     } else {
//                         val.
//                     }
//                 },
//
//                 (_, _, _) => panic!("Invalid key or data")
//             }
//         }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     #[test]
//     fn it_works() {
//         let result = 2 + 2;
//         assert_eq!(result, 4);
//     }
// }

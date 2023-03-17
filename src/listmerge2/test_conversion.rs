use std::collections::HashMap;
use smallvec::{SmallVec, smallvec};
use rle::{MergableSpan, RleRun};
use crate::{DTRange, Frontier, LV};
use crate::causalgraph::graph::Graph;
use crate::causalgraph::graph::tools::DiffFlag;
use crate::listmerge2::Index;
use crate::rle::{KVPair, RleVec};

#[derive(Debug, Clone)]
enum GraphEntry1 {
    Merge {
        parents: SmallVec<[usize; 2]>, // Could have 0 or 1 items.
        // child: usize,
        // state: MergeState,
    },
    Split {
        parent: usize,
        // children: SmallVec<[usize; 2]>,
        num_children: usize,
        // state: SplitState,
    },
    Ops {
        parent: usize,
        span: DTRange,
    },
}

use GraphEntry1::*;

impl Graph {
    fn to_entry_list(&self) -> Vec<GraphEntry1> {
        let mut result = vec![];

        // Map of (base version, result index) tuples
        let mut version_map = HashMap::<Vec<usize>, usize>::new();
        version_map.insert(vec![], usize::MAX); // ROOT entry.

        for e in self.entries.iter() {
            let mut split_points: SmallVec<[usize; 4]> = smallvec![];
            // let mut split_points: SmallVec<[usize; 4]> = smallvec![e.span.last()];

            // let mut children = e.child_indexes.clone();
            for &child_idx in &e.child_indexes {
                let child = &self.entries.0[child_idx];
                for &p in child.parents.as_ref() {
                    if e.span.contains(p) {
                        split_points.push(p);
                    }
                }
            }

            split_points.sort_unstable();


            let mut start = e.span.start;
            let mut iter = split_points.into_iter();

            let mut last_split_point = None;
            let mut num_children = 0;



            let mut add_to_result = |result: &mut Vec<GraphEntry1>, start: LV, last: LV, parents: &[LV], num_children: usize| {
                let end = last + 1;
                // println!("{start} .. {last} / end: {end} count {num_children} parents {:?}", parents);

                let parent_idx = version_map.get(parents).copied().unwrap_or_else(|| {
                    if parents.len() >= 2 {
                        let idx = result.len();
                        result.push(Merge {
                            parents: parents.iter().copied().map(|p| {
                                *version_map.get(&vec![p]).unwrap()
                            }).collect()
                        });
                        idx
                    } else {
                        panic!("Missing version in map {:?}", parents);
                    }
                });

                assert_ne!(start, end);
                let ops_idx = result.len();
                result.push(Ops {
                    parent: parent_idx,
                    span: (start..end).into()
                });

                if num_children <= 1 {
                    version_map.insert(vec![last], ops_idx);
                } else {
                    let split_idx = result.len();
                    result.push(Split {
                        parent: ops_idx,
                        num_children,
                    });
                    version_map.insert(vec![last], split_idx);
                }
            };

            loop {
                let next_split_point = iter.next();
                // dbg!(last_split_point, next_split_point);
                match (last_split_point, next_split_point) {
                    (None, Some(_)) => {
                        last_split_point = next_split_point;
                        num_children = 1;
                    },

                    (Some(p1), Some(p2)) if p1 == p2 => {
                        num_children += 1;
                    },
                    (Some(p1), Some(p2)) => {
                        e.with_parents(start, |parents| {
                            add_to_result(&mut result, start, p1, parents, num_children);
                        });
                        start = p1 + 1;
                        last_split_point = Some(p2);
                        num_children = 1;
                    },

                    (Some(p), None) => {
                        e.with_parents(start, |parents| {
                            add_to_result(&mut result, start, p, parents, num_children);
                        });
                        start = p + 1;
                        last_split_point = None;
                    },

                    (None, None) => {
                        // Emit everything else.
                        if start != e.span.end() {
                            e.with_parents(start, |parents| {
                                add_to_result(&mut result, start, e.span.last(), parents, 0);
                            });
                        }
                        break;
                    },
                }
            }
        }

        result
    }
}


#[derive(Debug, Clone)]
enum GraphEntry2 {
    Merge {
        parents: SmallVec<[usize; 2]>, // Could have 0 or 1 items.
        span: DTRange,
    },
    Split {
        parent: usize,
        // children: SmallVec<[usize; 2]>,
        num_children: usize,
    },
}

impl From<&GraphEntry1> for GraphEntry2 {
    fn from(value: &GraphEntry1) -> Self {
        match value {
            Merge { parents } => {
                GraphEntry2::Merge {
                    parents: parents.clone(),
                    span: (0..0).into(),
                }
            }
            Split { parent, num_children } => {
                GraphEntry2::Split { parent: *parent, num_children: *num_children }
            }
            Ops { parent, span } => {
                GraphEntry2::Merge {
                    parents: smallvec![*parent],
                    span: *span,
                }
            }
        }
    }
}

fn ge1_to_ge2(input: &Vec<GraphEntry1>) -> Vec<GraphEntry2> {
    let mut result = vec![];

    let mut iter = input.iter();
    let mut last: Option<&GraphEntry1> = None;
    loop {
        let next = iter.next();
        match (last, next) {
            (None, None) => { break; },
            (None, Some(e)) => { last = Some(e); }

            (Some(Merge { parents }), Some(Ops { parent: _, span })) => {
                result.push(GraphEntry2::Merge {
                    parents: parents.clone(),
                    span: *span,
                });
                last = None;
            }

            (Some(e1), _) => {
                result.push(e1.into());
                last = next;
            }
        }
    }

    result
}

#[derive(Debug, Clone)]
enum GraphEntry3 {
    Merge {
        parents: SmallVec<[usize; 2]>, // Could have 0 or 1 items.
    },
    Split {
        parent: usize,
        span: DTRange,
        num_children: usize,
    },
}

impl From<&GraphEntry1> for GraphEntry3 {
    fn from(value: &GraphEntry1) -> Self {
        match value {
            Merge { parents } => {
                GraphEntry3::Merge { parents: parents.clone() }
            }
            Split { parent, num_children } => {
                GraphEntry3::Split { parent: *parent, span: Default::default(), num_children: *num_children }
            }
            Ops { parent, span } => {
                GraphEntry3::Split {
                    parent: *parent,
                    span: *span,
                    num_children: 1,
                }
            }
        }
    }
}

fn ge1_to_ge3(input: &Vec<GraphEntry1>) -> Vec<GraphEntry3> {
    let mut result = vec![];

    let mut iter = input.iter();
    let mut last: Option<&GraphEntry1> = None;
    loop {
        let next = iter.next();
        match (last, next) {
            (None, None) => { break; },
            (None, Some(e)) => { last = Some(e); }

            (Some(Ops { parent, span }), Some(Split { parent: _, num_children })) => {
                result.push(GraphEntry3::Split {
                    parent: *parent,
                    span: *span,
                    num_children: *num_children,
                });
                last = None;
            }
            (Some(e1), _) => {
                result.push(e1.into());
                last = next;
            }
        }
    }

    result
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;
    use crate::causalgraph::graph::tools::test::fancy_graph;
    use crate::list::ListOpLog;
    use crate::listmerge2::test_conversion::{ge1_to_ge2, ge1_to_ge3, GraphEntry1, GraphEntry2, GraphEntry3};

    #[test]
    fn foo() {
        let cg = fancy_graph();
        let result = cg.to_entry_list();
        dbg!(result);
    }

    #[test]
    fn node_cc() {
        let mut bytes = vec![];
        // File::open("benchmark_data/git-makefile.dt").unwrap().read_to_end(&mut bytes).unwrap();
        File::open("benchmark_data/node_nodecc.dt").unwrap().read_to_end(&mut bytes).unwrap();
        let o = ListOpLog::load_from(&bytes).unwrap();
        let cg = o.cg;

        let result = cg.graph.to_entry_list();
        // dbg!(result);

        let size_1 = std::mem::size_of::<GraphEntry1>();
        println!("1. num: {}, size of each {}, total size {}", result.len(), size_1, result.len() * size_1);

        let ge2 = ge1_to_ge2(&result);
        let size_2 = std::mem::size_of::<GraphEntry2>();
        println!("2. num: {}, size of each {}, total size {}", ge2.len(), size_2, ge2.len() * size_2);

        let ge3 = ge1_to_ge3(&result);
        let size_3 = std::mem::size_of::<GraphEntry3>();
        println!("3. num: {}, size of each {}, total size {}", ge3.len(), size_3, ge3.len() * size_3);

    }

}


// git_makefile:
// 1. num: 2612, size of each 32, total size 83584
// 2. num: 1846, size of each 48, total size 88608
// 3. num: 1981, size of each 40, total size 79240

// node_nodecc:
// 1. num: 183, size of each 32, total size 5856
// 2. num: 137, size of each 48, total size 6576
// 3. num: 147, size of each 40, total size 5880

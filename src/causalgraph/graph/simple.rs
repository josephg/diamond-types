use std::collections::BinaryHeap;
use crate::causalgraph::graph::{Graph, GraphEntrySimple};
use crate::{DTRange, Frontier, LV};
use crate::rle::RleKeyedAndSplitable;

impl Graph {
    /// This method returns the graph, but split up so parents always refer to the last entry of an
    /// item. This is useful for debugging, exporting the causal graph and for printing the causal
    /// graph using DOT.
    fn make_simple_graph(&self, frontier: &[LV]) -> Vec<GraphEntrySimple> {
        let mut result = vec![];

        let mut queue = frontier.iter().copied().collect::<BinaryHeap<LV>>();

        while let Some(v) = queue.pop() {
            // println!("Popped {v}");

            let e = self.entries.find_packed(v);
            // We could use the entry's end here, but if the frontier is partial it'll end up wrong.
            let mut span_remaining: DTRange = (e.span.start..v+1).into();
            // let mut last = v;

            while let Some(&peek_v) = queue.peek() {
                // println!("- Peeked {peek_v}");
                if peek_v < span_remaining.start { break; }

                queue.pop();
                if peek_v == span_remaining.end { continue; } // Ignore duplicates.

                // Emit peek_v+1..=v.
                let emit_here = span_remaining.truncate_from(peek_v + 1);
                result.push(GraphEntrySimple {
                    span: emit_here,
                    parents: Frontier::new_1(peek_v),
                });
            }

            result.push(GraphEntrySimple {
                span: span_remaining,
                parents: e.parents.clone(),
            });

            // Add parents.
            queue.extend(e.parents.iter().copied());
            // dbg!(&queue);
        }

        result.reverse();
        result
    }
}

#[cfg(test)]
mod test {
    use crate::causalgraph::graph::GraphEntrySimple;
    use crate::causalgraph::graph::tools::test::fancy_graph;
    use crate::LV;

    fn check_simple_graph(g: &[GraphEntrySimple]) {
        let mut last = 0;
        for e in g {
            assert!(e.span.start >= last);
            last = e.span.end;

            for &p in e.parents.iter() {
                assert!(p < e.span.start);
            }

            // And the big one: All items which reference this item through their parents must
            // reference the last entry of our span.
            for ee in g {
                for &p in ee.parents.iter() {
                    assert!(p < e.span.start || p >= e.span.last(), "Parent points inside this entry");
                }
            }
        }
    }

    #[test]
    fn fancy_graph_as_simple() {
        let g = fancy_graph();

        let check = |f: &[LV]| {
            let simple_graph = g.make_simple_graph(f);
            check_simple_graph(&simple_graph);
        };

        check(&[]);
        check(&[0]);
        check(&[3]);
        check(&[6]);
        check(&[0, 3]);
        check(&[10]);
        check(&[5, 10]);

        // for e in r {
        //     println!("{:?}", e);
        // }
    }
}

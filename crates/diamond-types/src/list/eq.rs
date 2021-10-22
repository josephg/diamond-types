/// This file implements equality checking for ListCRDT objects. This implementation is reasonably
/// inefficient. Its mostly just to aid in unit testing & support for fuzzing.

use crate::list::{ListCRDT, Time, ROOT_AGENT, Branch};
use crate::rle::RleVec;
use crate::list::span::YjsSpan;
use rle::{HasLength, SplitableSpan};
// use std::fs::File;
// use std::io::Write;
use crate::order::TimeSpan;
use diamond_core::*;
// use smallvec::smallvec;

// TODO: Not gonna lie... I kinda hate these typedefs. Remove them?
type AgentMap = Vec<Option<AgentId>>;
type AgentMapRef<'a> = &'a [Option<AgentId>];

fn agent_map_from(a: &ListCRDT, b: &ListCRDT) -> AgentMap {
    a.client_data.iter()
        .map(|client| b.get_agent_id(client.name.as_str()))
        .collect()
}

fn map_agent(map: AgentMapRef, agent: AgentId) -> AgentId {
    if agent == ROOT_AGENT { ROOT_AGENT }
    else { map[agent as usize].unwrap() }
}

fn map_crdt_location(map: AgentMapRef, loc: CRDTId) -> CRDTId {
    CRDTId {
        agent: map_agent(map, loc.agent),
        seq: loc.seq
    }
}

fn set_eq(a: &[Time], b: &[Time]) -> bool {
    if a.len() != b.len() { return false; }
    for aa in a.iter() {
        if !b.contains(aa) { return false; }
    }
    true
}

// const DEBUG_EQ: bool = true;
const DEBUG_EQ: bool = false;

impl PartialEq for ListCRDT {
    fn eq(&self, other: &Self) -> bool {
        // There's a few ways list CRDT objects can end up using different bytes to represent the
        // same data. The main two are:
        // - Different peers can use different IDs to describe the same agent IDs
        // - If different peers see concurrent operations in different orders, the ops will have
        //   different order numbers assigned

        let agent_a_to_b = agent_map_from(self, other);
        // let agent_b_to_a = agent_map_from(other, self);

        // We need to check equality of a bunch of things.

        // 1. Frontiers should match. The frontier property is a set, so order is not guaranteed.
        if self.frontier.len() != other.frontier.len() { return false; }

        let a_to_b_order = |order: Time| {
            let a_loc = self.get_crdt_location(order);
            let b_loc = map_crdt_location(&agent_a_to_b, a_loc);
            other.crdt_to_localtime(b_loc)
        };

        let a_to_b_span = |order: Time, max: u32| {
            let a_span = self.get_crdt_span(order, max);
            let b_loc = map_crdt_location(&agent_a_to_b, a_span.loc);
            other.crdt_span_to_localtime(b_loc, a_span.len)
        };

        for order in self.frontier.iter() {
            // O(n^2). Could do better by sorting each, but n is very small so its nbd.
            let other_order = a_to_b_order(*order);
            if !other.frontier.contains(&other_order) {
                if DEBUG_EQ { eprintln!("Frontier does not match"); }
                return false;
            }
        }

        // 2. Compare the range trees. This is the money subject, right here.

        // This is inefficient. Use a double iterator or something if this is a hot path and not
        // just for testing.
        let mut a_items: RleVec<YjsSpan> = RleVec::new();
        let mut b_items: RleVec<YjsSpan> = RleVec::new();

        for mut entry in self.range_tree.raw_iter() {
            // dbg!(entry);
            // Map the entry to a. The entry could be a mix from multiple user agents. Split it
            // up if so.
            loop {
                let TimeSpan {
                    start: order, len
                } = a_to_b_span(entry.time, entry.len() as u32);

                a_items.push(YjsSpan {
                    time: order,
                    origin_left: a_to_b_order(entry.origin_left),
                    origin_right: a_to_b_order(entry.origin_right),
                    len: len as i32 * entry.len.signum()
                });

                if len == entry.len() as u32 {
                    break;
                } else {
                    // Trim from entry and iterate
                    entry.truncate_keeping_right(len as usize);
                }
            }
        }
        for entry in other.range_tree.raw_iter() {
            b_items.push(entry);
        }
        // dbg!(&a_items, &b_items);
        if a_items != b_items {
            if DEBUG_EQ {
                println!("Items do not match:");
                self.debug_print_segments();
                println!("\n ----- \n");
                other.debug_print_segments();
                // println!("a {:#?}", &a_items);
                // println!("b {:#?}", &b_items);

                // For debugging.
                // let mut a = File::create("a").unwrap();
                // a.write_fmt(format_args!("{:#?}", &a_items)).unwrap();
                // let mut b = File::create("b").unwrap();
                // b.write_fmt(format_args!("{:#?}", &b_items)).unwrap();
                // println!("Item lists written to 'a' and 'b'");

                // dbg!(&self);
                // dbg!(a_to_b_order(84));
                // dbg!(a_to_b_order(85));
                // dbg!(self.client_with_order.find(84));
                // dbg!(self.client_with_order.find(85));
                // dbg!(a_to_b_span(84, 2));
            }
            return false;
        }

        // 3. Compare the delete lists
        // let mut mapped = Rle::new();
        // for del in self.deletes.iter() {
        //     // mapped.append(KVPair())
        // }

        // 4. Compare txn parents.
        // Almost all txns simply have their immediate ancestor as a parent. This might bite me
        // later but I'm just going to compare the first txn in each span.
        // This is dodgy because txn coalescing might be different in each peer. We should probably
        // do this both ways around.
        for txn in self.txns.iter() {
            let other_order = a_to_b_order(txn.time);
            let expect_parents = txn.parents.iter()
                .map(|p| a_to_b_order(*p)).collect::<Branch>();

            let (other_txn, offset) = other.txns.find_with_offset(other_order).unwrap();
            if let Some(actual_parent) = other_txn.parent_at_offset(offset as usize) {
                if expect_parents.len() != 1 || expect_parents[0] != actual_parent { return false; }
            } else if !set_eq(&expect_parents, &other_txn.parents) { return false; }
        }

        // 5. The contained text content should also match. If we've gotten this far and it doesn't
        // match, its probably due to an error - it might be worth panicking instead of returning
        // false.
        if let (Some(a), Some(b)) = (&self.text_content, &other.text_content) {
            if a != b { return false; }
        }

        true
    }
}

impl Eq for ListCRDT {}


#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;

    #[test]
    fn eq_empty() {
        let a = ListCRDT::new();
        let b = ListCRDT::new();
        assert_eq!(a, b);
    }

    #[test]
    fn simple_eq() {
        let mut a = ListCRDT::new();
        a.get_or_create_agent_id("seph");
        a.local_insert(0, 0, "hi".into());
        assert_eq!(a, a); // reflexive

        let mut b = ListCRDT::new();
        b.get_or_create_agent_id("seph");
        b.local_insert(0, 0, "hi".into());
        assert_eq!(a, b);
    }

    #[test]
    fn concurrent_simple() {
        let mut a = ListCRDT::new();
        a.get_or_create_agent_id("mike");
        a.local_insert(0, 0, "aaa".into());

        let mut b = ListCRDT::new();
        b.get_or_create_agent_id("seph");
        b.local_insert(0, 0, "bb".into());

        a.replicate_into(&mut b);
        b.replicate_into(&mut a);
        assert_eq!(a, b);

        a.local_delete(0, 2, 2);
        a.replicate_into(&mut b);
        assert_eq!(a, b);

        // dbg!(&a.content_tree, &b.content_tree);

        // dbg!(&a.frontier, &b.frontier);

    }
}
use std::ops::Range;

use content_tree::SplitableSpan;

use crate::list::{encoding, ListCRDT, Order, ROOT_ORDER};
use crate::list::encoding::{Chunk, Parents, Run, SpanWriter};
use crate::list::ot::positional::InsDelTag;
use crate::rangeextra::OrderRange;
use crate::rle::{KVPair, RleSpanHelpers, RleVec};
use crate::list::encoding::varint::{num_encode_i64_with_extra_bit, mix_bit_u64, encode_u64, encode_u32};
use smallvec::smallvec;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct EditRun {
    diff: i32, // From previous item
    len: u32,
    is_delete: bool,
    backspace_mode: bool,
}

impl SplitableSpan for EditRun {
    fn len(&self) -> usize { self.len as usize }

    fn truncate(&mut self, _at: usize) -> Self { unimplemented!() } // Shouldn't get called.

    fn can_append(&self, other: &Self) -> bool {
        self.is_delete == other.is_delete && (other.diff == 0 || merge_bksp(self, other).is_some())
    }
    fn append(&mut self, other: Self) {
        if let Some(r) = merge_bksp(self, &other) {
            *self = r;
        } else { self.len += other.len; }
    }
}

fn merge_bksp(r1: &EditRun, r2: &EditRun) -> Option<EditRun> {
    if !r1.is_delete || !r2.is_delete { return None; }
    if !(r2.len == 1 && r2.diff == -1) { return None; }

    if r1.backspace_mode {
        return Some(EditRun {
            diff: r1.diff,
            len: r1.len + 1,
            is_delete: true,
            backspace_mode: true
        });
    } else if r1.len == 1 {
        return Some(EditRun {
            diff: r1.diff + 1, // Weird but we'll go after the deleted character.
            len: r1.len + 1,
            is_delete: true,
            backspace_mode: true
        });
    } else { return None; }
}

fn write_editrun(into: &mut Vec<u8>, val: EditRun) {
    let mut dest = [0u8; 20];
    let mut pos = 0;

    let mut n = num_encode_i64_with_extra_bit(val.diff as i64, val.len != 1);
    if val.is_delete {
        n = mix_bit_u64(n, val.backspace_mode);
    }
    n = mix_bit_u64(n, val.is_delete);
    pos += encode_u64(n, &mut dest[..]);
    if val.len != 1 {
        pos += encode_u32(val.len, &mut dest[pos..]);
    }

    into.extend_from_slice(&dest[..pos]);
}

impl ListCRDT {
    /// This encoding method encodes all the data as a series of original braid style positional
    /// patches. The resulting encoded form contains each patch as it was originally typed, tracking
    /// *when* the patch happened (via its parents), *where* (in a simple integer position) and
    /// *what* the change was (insert, delete).
    ///
    /// This form ends up being much more compact than the other approaches, storing Martin's
    /// editing trace in 28kb of overhead (compared to about 100kb for the fast encoding format).
    pub fn encode_patches(&self, verbose: bool) -> Vec<u8> {
        let mut result = self.encode_common();
        // result now has:
        // - Common header
        // - Document content
        // - List of agents

        // Write the frontier
        // push_chunk_header(into, Chunk::Frontier, data.len());
        // for v in self.frontier.iter() {
        //     push_u32(&mut result, *v);
        // }

        let mut w = SpanWriter::new(write_editrun);

        // For optimization, the stream of patches emitted here does not necessarily match the order
        // of patches we're storing locally. For example, if two users are concurrently editing two
        // different branches, emitting everything in our native, local order would result in O(n^2)
        // writing time (to go back and forth between branches), loading time (the same) and end up
        // bigger on disk (both because the insert positions wouldn't line up nicely, and because
        // the txn parents wouldn't be linear).
        //
        // The O(n^2) problems of this don't show up if two users are editing different sections of
        // a document at once, though interleaved edits will still compress slightly worse than if
        // the edits were fully linearized.

        // Anyway, the upshoot of all of that is that the output file may have different orders than
        // we store in memory. So we need to map:
        // - Frontiers
        // - Parents
        // - Agent assignments
        // Maps from current order numbers -> output order numbers
        //
        // Note I'm using a vec for this and inserting. It would be theoretically better to use
        // another b-tree here for this, but I don't think the extra code size & overhead is worth
        // it for nearly any normal use cases. (The reordering should be stable - once a document
        // has been reordered and saved, next time its loaded there will be no further reordering.)
        let mut inner_to_outer_map: RleVec<KVPair<Range<Order>>> = RleVec::new();
        let mut outer_to_inner_map: RleVec<KVPair<Range<Order>>> = RleVec::new();

        let mut next_output_order = 0;
        let mut last_edit_pos: u32 = 0;

        for (range, patch) in self.iter_original_patches() {
            // dbg!(&range);
            w.push(EditRun {
                diff: i32::wrapping_sub(patch.pos as i32,last_edit_pos as i32),
                len: patch.len,
                is_delete: patch.tag == InsDelTag::Del,
                backspace_mode: false, // Filled in by the appending code (above).
            });
            last_edit_pos = patch.pos;
            if patch.tag == InsDelTag::Ins { last_edit_pos += patch.len; }

            if range.start != next_output_order {
                // To cut down on allocations and copying, these maps are both lazy. They only
                // contain entries where the output orders don't match the current document orders.
                outer_to_inner_map.push(KVPair(next_output_order, range.clone()));
                inner_to_outer_map.insert(KVPair(range.start, range.transpose(next_output_order)));
            }

            next_output_order += range.order_len();
        }
        let patch_data = w.flush_into_inner();
        // dbg!(&outer_to_inner_map);
        // dbg!(&inner_to_outer_map);

        let local_to_remote_order = |order: Order| -> Order {
            if order == ROOT_ORDER {
                ROOT_ORDER
            } else if let Some((val, offset)) = inner_to_outer_map.find_with_offset(order) {
                val.1.start + offset
            } else { order }
        };

        // *** Frontier ***

        let mut frontier_data = vec!();
        for v in self.frontier.iter() {
            // dbg!(v, local_to_remote_order(*v));
            encoding::push_u32(&mut frontier_data, local_to_remote_order(*v));
        }
        encoding::push_chunk(&mut result, Chunk::Frontier, &frontier_data);


        // So I could map this during the loop above, with patches. That would avoid the allocation
        // for outer_to_inner_map, but the agent list here is usually in massive runs. I haven't
        // benchmarked it but (I assume) doing it here is still much more performant for the average
        // case where there's no map, and the data is pretty much copied over.
        // let mut agent_data = Vec::new();
        let mut agent_writer = SpanWriter::new(encoding::push_run_u32);
        let mut parent_writer = SpanWriter::new(encoding::write_parents);

        outer_to_inner_map.for_each_sparse(next_output_order, |item| {
            let range = match item {
                Ok(KVPair(_, mapped_range)) => mapped_range.clone(),
                Err(range) => range,
            };

            // *** Parents ***

            // Parents need to be mapped twice. We need to iterate in the items in output order
            // (achieved by the outer loop), and then for each item visited, map the parent orders
            // to output orders.
            let mut order = range.start;
            let mut idx = self.txns.find_index(order).unwrap();
            while order < range.end {
                let txn = &self.txns[idx];
                // The outer_to_inner_map should always line up along txn boundaries.
                debug_assert_eq!(range.start, txn.order);
                assert!(range.end >= txn.end());

                // dbg!((order, &txn.parents));
                let mut parents = Parents {
                    order: txn.order .. txn.order + txn.len,
                    parents: smallvec![]
                };
                for &p in &txn.parents {
                    parents.parents.push(local_to_remote_order(p));
                }
                parent_writer.push(parents);

                order += txn.len;
                idx += 1;
            }

            // *** Mapped Agent Assignments ***
            let mut order = range.start;
            let mut idx = self.client_with_order.find_index(order).unwrap();
            while order < range.end {
                let e = &self.client_with_order[idx];
                let next_order = e.end().min(range.end);
                agent_writer.push(Run {
                    val: e.1.loc.agent as _,
                    len: (next_order - order) as _
                });
                order = next_order;
                // This is sort of weird. We only really need to bump idx if we consume the
                // whole range. If we don't, we'll exit the loop presently anyway. This could be
                // cleaned up using loop {} and an explicit break, but its not a big deal.
                idx += 1;
            }
        });
        let agent_assignment_data = agent_writer.flush_into_inner();
        let parents_data = parent_writer.flush_into_inner();

        if verbose {
            dbg!(agent_assignment_data.len());
            dbg!(parents_data.len());
            dbg!(patch_data.len());
            // dbg!(&inner_to_outer_map);
        }
        encoding::push_chunk(&mut result, Chunk::AgentAssignment, &agent_assignment_data);
        encoding::push_chunk(&mut result, Chunk::Parents, &parents_data);
        encoding::push_chunk(&mut result, Chunk::Patches, &patch_data);

        result
    }
}

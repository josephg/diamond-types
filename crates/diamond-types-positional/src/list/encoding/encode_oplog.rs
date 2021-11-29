use rle::HasLength;
use crate::list::encoding::*;
use crate::list::operation::InsDelTag;
use crate::list::operation::InsDelTag::{Del, Ins};
use crate::list::OpLog;
use crate::rle::KVPair;

const MAGIC_BYTES_SMALL: [u8; 8] = *b"DIAMONDp";


// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
// pub struct EditRun {
//     cursor_diff: isize, // Cursor movement from previous item
//     len: usize,
//     tag: InsDelTag,
//     reversed: bool,
//     has_content: bool,
// }
//
// impl HasLength for EditRun {
//     fn len(&self) -> usize { self.len }
// }
//
// impl MergableSpan for EditRun {
//     fn can_append(&self, other: &Self) -> bool {
//         self.tag == other.tag && self.has_content == other.has_content
//     }
//
//     fn append(&mut self, other: Self) {
//         todo!()
//     }
// }

impl OpLog {
    pub fn encode(&self, verbose: bool) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&MAGIC_BYTES_SMALL);

        // TODO: The fileinfo chunk should specify DT version, encoding version and information
        // about the data types we're encoding.
        push_chunk(&mut result, Chunk::FileInfo, &[]);

        // TODO: Do this without the unnecessary allocation.
        let mut agent_names = Vec::new();
        for client_data in self.client_data.iter() {
            push_str(&mut agent_names, client_data.name.as_str());
        }
        push_chunk(&mut result, Chunk::AgentNames, &agent_names);


        // *** Frontier ***

        // This is sort of redundant - as the orders from the operation set can be replayed to
        // figure out the frontier.

        let mut frontier_data = vec!();
        for v in self.frontier.iter() {
            // dbg!(v, local_to_remote_order(*v));
            // push_u32(&mut frontier_data, local_to_remote_order(*v));
            push_usize(&mut frontier_data, *v);
        }
        push_chunk(&mut result, Chunk::Frontier, &frontier_data);


        // *** Inserted (text) content and operations ***

        // There's two ways I could iterate through the operations:
        //
        // 1. In local operation order. Each operation at that operation's local time. This is much
        //    simpler and faster - since we're essentially just copying oplog into the file.
        // 2. In optimized order. This would use txn_trace to reorder the operations in the
        //    operation log to maximize runs (and thus minimize file size). At some point I'd like
        //    to do this optimization - but I'm not sure where. (Maybe we should optimize in-place?)

        // Note I'm going to push the text of all insert operations separately from the operation
        // data itself.
        //
        // Note for now this includes text that was later deleted. It is also in time-order not
        // document-order.
        //
        // Another way of storing this content would be to interleave it with the operations
        // themselves. That would work fine but:
        //
        // - The interleaved approach would be a bit more complex when dealing with other (non-text)
        //   data types.
        // - Interleaved would result in a slightly smaller file size (tens of bytes smaller)
        // - Interleaved would be easier to consume, because we wouldn't need to match up inserts
        //   with the text
        // - Interleaved it would compress much less well with snappy / lz4.

        // I'm going to gather it all before writing because we don't actually store the number of
        // bytes!
        let mut inserted_text = String::new();
        let mut deleted_text = String::new();
        // let mut w = SpanWriter::new(|into, op| {
        //
        // });

        let mut op_data = Vec::new();

        // The cursor position of the previous edit. We're differential, baby.
        let mut last_cursor_pos: usize = 0;

        // This is a bit gross at the moment because its cloning the SmartString.
        // TODO(perf): Clean this up.
        let mut len_total = 0u64;
        let mut diff_zig_total = 0u64;
        let mut num_ops = 0;

        for (_i, KVPair(_, op)) in self.operations.iter_merged().enumerate() {
            // For now I'm ignoring known content in delete operations.
            let use_content = op.tag == Ins;
            if use_content { assert!(op.content_known); }

            if use_content {
                inserted_text.push_str(&op.content);
            }

            if op.tag == Del && op.content_known {
                deleted_text.push_str(&op.content);
            }

            // Note I'm relying on the operation log itself to be iter_merged, which simplifies
            // things here greatly.

            // This is a bit of a tradeoff. Sometimes when items get split, they retain their
            // reversed tag. This can actually make encoding size smaller - because we can avoid
            // storing a local diff of -1 / 1. But then we have to store op.reversed even for length
            // 1 items. And there are a lot of those - so eh.
            let reversed = op.reversed && op.len > 1;
            // let reversed = op.reversed;
            // if op.len == 1 { assert!(!op.reversed); }

            // let op_start = op.pos;
            let op_start = if op.tag == Del && reversed {
                op.pos + op.len
            } else {
                op.pos
            };

            // let op_end = op.pos;
            let op_end = if op.tag == Ins && !reversed {
                op.pos + op.len
            } else {
                op.pos
            };

            let cursor_movement = isize::wrapping_sub(op_start as isize, last_cursor_pos as isize) as i64;
            last_cursor_pos = op_end;

            // println!("pos {} diff {} del {} rev {} len {}", op.pos, cursor_movement, op.tag == Del, reversed, op.len);
            // println!("diff {} del {:?} rev {} len {}", cursor_movement, op.tag, reversed, op.len);

            // if op.len != 1 { len_total += op.len as u64; }
            // diff_zig_total += num_encode_zigzag_i64(cursor_movement);
            diff_zig_total += cursor_movement.abs() as u64;
            num_ops += 1;

            // So generally about 40% of changes have length of 1, and about 40% of changes (even
            // after RLE) happen without the cursor moving.
            let mut dest = [0u8; 20];
            let mut pos = 0;

            // TODO: Make usize variants of all of this and use that rather than u64 / i64.
            let mut n = if op.len != 1 {
                let mut n = op.len as u64;
                // When len == 1, the item is never considered reversed.
                if op.tag == Del { n = mix_bit_u64(n, reversed) };
                n
            } else if cursor_movement != 0 {
                num_encode_zigzag_i64(cursor_movement)
            } else {
                0
            };

            n = mix_bit_u64(n, op.tag == Del);
            n = mix_bit_u64(n, cursor_movement != 0);
            n = mix_bit_u64(n, op.len != 1);
            pos += encode_u64(n, &mut dest[pos..]);

            if op.len != 1 && cursor_movement != 0 {
                let mut n2 = num_encode_zigzag_i64(cursor_movement);
                pos += encode_u64(n2, &mut dest[pos..]);
            }

            op_data.extend_from_slice(&dest[..pos]);
        }

        if verbose {
            // dbg!(len_total, diff_zig_total, num_ops);
            println!("op_data.len() {}", &op_data.len());
            println!("inserted text length {}", inserted_text.len());
            println!("deleted text length {}", deleted_text.len());
        }

        push_chunk(&mut result, Chunk::Content, &inserted_text.as_bytes());
        push_chunk(&mut result, Chunk::Patches, &op_data);

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::list::ListCRDT;

    #[test]
    fn encoding_smoke_test() {
        let mut doc = ListCRDT::new();
        doc.get_or_create_agent_id("seph");
        doc.local_insert(0, 0, "hi there");

        let data = doc.ops.encode(true);
        dbg!(data.len(), data);
    }
}
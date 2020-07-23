/**
 * This CRDT is loosely based of the list CRDT in automerge.
 * 
 * Each client has an ID. (Usually a string or something). This is mapped
 * locally into a number based on the order in which the local data store has
 * witnessed operations. Numerical ID 0 is reserved.
 * 
 * Each character (/ item in the list) has a name based on the tuple (client ID,
 * seq). The sequence number is the index of the character inserted by that
 * client. (So if a client inserts 100 characters in its first change, those
 * characters are numbered 0-99, and the first character inserted by the next
 * operation is seq 100).
 *
 * ID 0 is reserved for root location in the document. (So inserting the first
 * character in a document will use parent 0/0).
 * 
 * Each operations specifies a list of:
 * 
 * - Position (client id / character id)
 * - Inserted characters (or '') or number of deleted characters
 *
 * Inserts use the position as the parent (predecessor).
 * Deletes use the position as the base of a range of deleted characters. Eg, if
 * the parent is id (10, 15) and the operation deletes 10 characters, it will
 * delete (10, 15), (10, 16), ... (10, 24). The characters do not need to be
 * sequential in the document.
 * 
 * An operation cannot refer to itself when specifying insert locations.
 * 
 * The order of op components inside the operation mostly doesn't matter - but
 * it does matter for inserted characters, since the first character the
 * operation inserts is (id, base_seq), then (id, base_seq+1), and so on in the
 * order inserted characters appear in the operation.
 */
#[allow(dead_code)]

mod btree;
mod common;

use common::*;

use inlinable_string::{InlinableString, StringExt};
use ropey::Rope;
use smallvec::SmallVec;

pub enum OpAction {
    Insert(InlinableString),
    // Deleted characters in sequence. In a CRDT these characters must be
    // contiguous from a single client.
    Delete(u32)
}

// pub struct OTOpComponent {
//     location: i32,
//     action: OpAction,
// }


pub struct CRDTLocation {
    client: ClientID,
    seq: ClientSeq,
}

/**
 * A crdt operation is a set of small operation components at locations.
 * 
 * Each of those components specifies:
 * 
 * - A location - which is the element immediately to the left of the cursor
 * - Optional inserted text at that location
 * - Optional deleted sequence at that location. This sequence must be
 *   contiguous inserts from the client.
 */
pub struct CRDTOpComponent {
    location: CRDTLocation,
    action: OpAction,
}
// Most operations only have 1 component.
pub struct CRDTOp(SmallVec<[CRDTOpComponent; 1]>);

// pub fn apply_ot_mut(rope: &mut Rope, op: &OTOp) {
//     let loc = op.location as usize;
//     if op.action.delete > 0 {
//         rope.remove(loc..loc + op.action.delete as usize);
//     }
//     if !op.action.insert.is_empty() {
//         rope.insert(loc, op.action.insert.as_ref());
//     }
// }

struct ClientData {
    // Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    // We need to be able to map each location to an item in the associated BST.
    // Note for inserts which insert a lot of contiguous characters, this will
    // contain a lot of repeated pointers. I'm trading off memory for simplicity
    // here - which might or might not be the right approach.
    ops: Vec<*const u32>
}

struct CRDTState {
    client_data: Vec<ClientData>,


    // ops_from_client: Vec<Vec<
}

// impl CRDTOp {
// pub fn crdt_to_ot(crdt_op: &CRDTOp) -> OTOp {
//     unimplemented!();
// }
// pub fn ot_to_crdt(ot_op: &OTOp) -> CRDTOp {
//     unimplemented!();
// }
// }


#[cfg(test)]
mod tests {
    use ropey::Rope;
    use super::*;
    use inlinable_string::InlinableString;

    #[test]
    fn it_works() {
        let mut r = Rope::new();
        // apply_ot_mut(&mut r, &OTOp {
        //     location: 0,
        //     action: OpAction {
        //         delete: None,
        //         insert: Some(InlinableString::from("asdf")),
        //     }
        // });
        // apply_ot_mut(&mut r, &OTOp {
        //     location: 0,
        //     action: OpAction {
        //         delete: Some(InlinableString::from("as")),
        //         insert: Some(InlinableString::from("xy")),
        //     }
        // });
        // r.insert(0, "hi");
        println!("text: '{}'", r);
    }

    #[test]
    fn foo() {
        use btree::*;
        use common::*;

        let mut tree = MarkerTree::new();

        let notify = |loc, _ptr| {
            dbg!(loc);
        };

        tree.insert(0, 2, CRDTLocation {
            client: 0,
            seq: 10
        }, notify);
        tree.insert(2, 3, CRDTLocation {
            client: 0,
            seq: 12
        }, notify);

        // type 2
        tree.insert(5, 1, CRDTLocation {
            client: 1,
            seq: 100
        }, notify);

        tree.insert(5, 10, CRDTLocation {
            client: 2,
            seq: 100
        }, notify);

        // type 3
        tree.insert(1, 5, CRDTLocation { client: 3, seq: 1000 }, notify);
        dbg!(tree);
    }
}

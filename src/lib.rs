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

use btree::*;
use common::*;
use std::pin::Pin;
use std::ptr;

use inlinable_string::InlinableString;
// use smallvec::SmallVec;

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
// pub struct CRDTOpComponent {
//     location: CRDTLocation,
//     action: OpAction,
// }
// Most operations only have 1 component.
// pub struct CRDTOp(SmallVec<[CRDTOpComponent; 1]>);

// pub fn apply_ot_mut(rope: &mut Rope, op: &OTOp) {
//     let loc = op.location as usize;
//     if op.action.delete > 0 {
//         rope.remove(loc..loc + op.action.delete as usize);
//     }
//     if !op.action.insert.is_empty() {
//         rope.insert(loc, op.action.insert.as_ref());
//     }
// }

#[derive(Debug)]
struct ClientData {
    // Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    // We need to be able to map each location to an item in the associated BST.
    // Note for inserts which insert a lot of contiguous characters, this will
    // contain a lot of repeated pointers. I'm trading off memory for simplicity
    // here - which might or might not be the right approach.
    ops: Vec<ptr::NonNull<NodeLeaf>>
}

#[derive(Debug)]
pub struct CRDTState {
    client_data: Vec<ClientData>,

    marker_tree: Pin<Box<MarkerTree>>,

    // ops_from_client: Vec<Vec<
}


impl CRDTState {
    pub fn new() -> Self {
        CRDTState {
            client_data: Vec::new(),
            marker_tree: MarkerTree::new()
        }
    }

    fn get_or_create_clientid(&mut self, name: &ClientName) -> ClientID {
        if let Some(id) = self.get_clientid(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: name.clone(),
                ops: Vec::new()
            });
            (self.client_data.len() - 1) as ClientID
        }
    }

    fn get_clientid(&self, name: &ClientName) -> Option<ClientID> {
        self.client_data.iter()
        .position(|client_data| &client_data.name == name)
        .map(|id| id as ClientID)
    }

    pub fn insert(&mut self, client_id: ClientID, pos: u32, text: InlinableString) -> CRDTLocation {
        // First lookup and insert into the marker tree
        let ops = &mut self.client_data[client_id as usize].ops;
        let loc_base = CRDTLocation {
            client: client_id,
            seq: ops.len() as ClientSeq
        };
        let inserted_length = text.chars().count();
        // ops.reserve(inserted_length);
        ops.resize(ops.len() + inserted_length, ptr::NonNull::dangling());

        let client_data = &mut self.client_data;

        let cursor = self.marker_tree.cursor_at_pos(pos, true);
        let insert_location = if pos == 0 {
            // This saves an awful lot of code needing to be executed.
            CRDT_DOC_ROOT
        } else { cursor.tell() };

        self.marker_tree.insert(cursor, inserted_length as ClientSeq, loc_base, |loc, len, leaf| {
            eprintln!("insert callback {:?} len {}", loc, len);
            let ops = &mut client_data[loc.client as usize].ops;
            for op in &mut ops[loc.seq as usize..(loc.seq+len) as usize] {
                *op = leaf;
            }
        });

        insert_location
    }

    pub fn insert_name(&mut self, client_name: &ClientName, pos: u32, text: InlinableString) -> CRDTLocation {
        let id = self.get_or_create_clientid(client_name);
        self.insert(id, pos, text)
    }

    pub fn lookup_position(&self, loc: CRDTLocation) -> u32 {
        if loc == CRDT_DOC_ROOT { return 0; }

        let ops = &self.client_data[loc.client as usize].ops;
        unsafe { MarkerTree::lookup_position(loc, ops[loc.seq as usize]) }
    }

    pub fn lookup_position_name(&self, client_name: &ClientName, seq: ClientSeq) -> u32 {
        let id = self.get_clientid(client_name).expect("Invalid client name");
        self.lookup_position(CRDTLocation {
            client: id,
            seq,
        })
    }
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
    // use ropey::Rope;
    use super::*;
    // use inlinable_string::InlinableString;

    #[test]
    fn it_works() {
        // let mut r = Rope::new();
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
        // println!("text: '{}'", r);
    }

    #[test]
    fn foo() {
        let mut state = CRDTState::new();
        let fred = InlinableString::from("fred");
        let george = InlinableString::from("george");

        eprintln!("{:#?}", state.insert_name(&fred, 0, InlinableString::from("hi there")));
        eprintln!("state {:#?}", state);
        
        eprintln!("root position is {}", state.lookup_position(CRDT_DOC_ROOT));
        eprintln!("position 1 is {}", state.lookup_position_name(&fred, 1));
        eprintln!("fred position 7 is {}", state.lookup_position_name(&fred, 7));
        eprintln!("{:#?}", state.insert_name(&george, 3, InlinableString::from("over "))); // hi over there
        eprintln!("fred position 7 is {}", state.lookup_position_name(&fred, 7));
        eprintln!("state {:#?}", state);
    }

    // #[test]
    // fn foo() {
    //     use btree::*;
    //     use common::*;

    //     let mut tree = MarkerTree::new();

    //     let notify = |loc, _ptr| {
    //         dbg!(loc);
    //     };

    //     tree.insert(0, 2, CRDTLocation { client: 0, seq: 10 }, notify);
    //     tree.insert(2, 3, CRDTLocation { client: 0, seq: 12 }, notify);

    //     // type 2
    //     tree.insert(5, 1, CRDTLocation { client: 1, seq: 100 }, notify);

    //     tree.insert(5, 10, CRDTLocation { client: 2, seq: 100 }, notify);

    //     // type 3
    //     tree.insert(1, 5, CRDTLocation { client: 3, seq: 1000 }, notify);
    //     dbg!(tree);
    // }
}

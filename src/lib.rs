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

// mod btree;
mod common;
mod marker_tree;

use marker_tree::*;
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

// #[derive(Debug)]
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

    pub fn get_or_create_clientid(&mut self, name: &str) -> ClientID {
        if let Some(id) = self.get_clientid(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: InlinableString::from(name),
                ops: Vec::new()
            });
            (self.client_data.len() - 1) as ClientID
        }
    }

    fn get_clientid(&self, name: &str) -> Option<ClientID> {
        self.client_data.iter()
        .position(|client_data| &client_data.name == name)
        .map(|id| id as ClientID)
    }

    pub fn insert(&mut self, client_id: ClientID, pos: u32, inserted_length: usize) -> CRDTLocation {
        // First lookup and insert into the marker tree
        let ops = &mut self.client_data[client_id as usize].ops;
        let loc_base = CRDTLocation {
            client: client_id,
            seq: ops.len() as ClientSeq
        };
        // let inserted_length = text.chars().count();
        // ops.reserve(inserted_length);
        let dangling_ptr = ptr::NonNull::dangling();
        ops.resize(ops.len() + inserted_length, dangling_ptr);

        let client_data = &mut self.client_data;

        let cursor = self.marker_tree.cursor_at_pos(pos, true);
        let insert_location = if pos == 0 {
            // This saves an awful lot of code needing to be executed.
            CRDT_DOC_ROOT
        } else { cursor.tell() };

        self.marker_tree.insert(cursor, inserted_length as ClientSeq, loc_base, |loc, len, leaf| {
            // eprintln!("insert callback {:?} len {}", loc, len);
            let ops = &mut client_data[loc.client as usize].ops;
            for op in &mut ops[loc.seq as usize..(loc.seq+len) as usize] {
                *op = leaf;
            }
        });

        if cfg!(debug_assertions) {
            // Check all the pointers have been assigned.
            let ops = &mut self.client_data[client_id as usize].ops;
            for e in &ops[ops.len() - inserted_length..] {
                assert_ne!(*e, dangling_ptr);
            }
        }

        insert_location
    }

    pub fn insert_name(&mut self, client_name: &str, pos: u32, text: InlinableString) -> CRDTLocation {
        let id = self.get_or_create_clientid(client_name);
        self.insert(id, pos, text.chars().count())
    }

    pub fn lookup_crdt_position(&self, loc: CRDTLocation) -> u32 {
        if loc == CRDT_DOC_ROOT { return 0; }

        let ops = &self.client_data[loc.client as usize].ops;
        unsafe { MarkerTree::lookup_position(loc, ops[loc.seq as usize]) }
    }

    pub fn lookup_num_position(&self, pos: usize) -> CRDTLocation {
        // let insert_location = if pos == 0 {
        //     // This saves an awful lot of code needing to be executed.
        //     CRDT_DOC_ROOT
        // } else { cursor.tell() };

        let cursor = self.marker_tree.cursor_at_pos(pos as u32, true);
        cursor.tell()
    }

    pub fn lookup_position_name(&self, client_name: &str, seq: ClientSeq) -> u32 {
        let id = self.get_clientid(client_name).expect("Invalid client name");
        self.lookup_crdt_position(CRDTLocation {
            client: id,
            seq,
        })
    }

    pub fn check(&self) {
        self.marker_tree.check();

        // TODO: Iterate through the tree / through the ops and make sure all
        // the CRDT locations make sense.

        // Maybe also scan the ops to make sure none of them are dangling
        // pointers?
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

    // fn fill_with_junk(state: &mut CRDTState) {
    //     let mut pos = 0;
    //     for _ in 0..10 {
    //         state.insert_name("fred", pos, InlinableString::from("fred"));
    //         state.insert_name("george", pos + 4, InlinableString::from("george"));
    //         pos += 10;
    //         state.check();
    //     }
    // }

    #[test]
    fn first_pos_returns_root() {
        let mut state = CRDTState::new();

        assert_eq!(state.lookup_num_position(0), CRDT_DOC_ROOT);
        state.insert_name("fred", 0, InlinableString::from("hi there"));
        assert_eq!(state.lookup_num_position(0), CRDT_DOC_ROOT);
    }


    #[test]
    fn junk_append() {
        let mut state = CRDTState::new();

        // Fill the document with junk. We need to use 2 different users here so
        // the content doesn't get merged.
        let mut pos = 0;
        for _ in 0..50 {
            state.insert_name("fred", pos, InlinableString::from("fred"));
            state.insert_name("george", pos + 4, InlinableString::from("george"));
            pos += 10;
            state.check();
        }
        
        // eprintln!("state {:#?}", state);
        state.check();
    }

    
    #[test]
    fn junk_prepend() {
    //     use std::io::Write;

        let mut state = CRDTState::new();
        
        // Repeatedly inserting at 0 will prevent all the nodes collapsing, so we don't
        // need to worry about that.
        for _ in 0..65 {
            state.insert_name("fred", 0, InlinableString::from("fred"));
            // state.check();
            // state.marker_tree.print_ptr_tree();
        }
    
        state.check();

        // std::io::stderr().flush().unwrap();
    }
    
}

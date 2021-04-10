// #![feature(core_intrinsics)]
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
mod split_list;
mod txn_simple;
mod splitable_span;
mod alloc;

use marker_tree::*;
use common::*;
use std::pin::Pin;

use inlinable_string::InlinableString;
use std::ptr::NonNull;
use crate::split_list::SplitList;
use std::ops::Index;
use smallvec::SmallVec;

use ropey::Rope;
use crate::splitable_span::SplitableSpan;

pub use alloc::ALLOCATED;

#[derive(Clone, Debug)]
pub enum ExternalOp {
    Insert {
        content: InlinableString,
        predecessor: CRDTLocation,
    },
    // Deleted characters in sequence. In a CRDT these characters must be
    // contiguous from a single client.
    Delete {
        target: CRDTLocation,
        len: ItemCount,
    }
}

#[derive(Clone, Debug)]
pub struct ExternalTxn {
    id: CRDTLocation,
    parents: SmallVec<[CRDTLocation; 2]>,
    ops: SmallVec<[ExternalOp; 1]>,
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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct MarkerEntry {
    len: u32,
    ptr: NonNull<NodeLeaf<Entry>>
}

impl SplitableSpan for MarkerEntry {
    // type Item = NonNull<NodeLeaf>;

    fn len(&self) -> usize {
        self.len as usize
    }

    fn truncate(&mut self, at: usize) -> Self {
        let remainder_len = self.len - at as u32;
        self.len = at as u32;
        return MarkerEntry {
            len: remainder_len,
            ptr: self.ptr
        }
    }

    fn can_append(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }

    fn append(&mut self, other: Self) {
        self.len += other.len;
    }
}

impl Index<usize> for MarkerEntry {
    type Output = NonNull<NodeLeaf<Entry>>;

    fn index(&self, _index: usize) -> &Self::Output {
        &self.ptr
    }
}


#[derive(Debug)]
struct ClientData {
    // Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    // We need to be able to map each location to an item in the associated BST.
    // Note for inserts which insert a lot of contiguous characters, this will
    // contain a lot of repeated pointers. I'm trading off memory for simplicity
    // here - which might or might not be the right approach.
    markers: SplitList<MarkerEntry>,
    // markers: Vec<NonNull<NodeLeaf>>
}

// Toggleable for testing.
const USE_INNER_ROPE: bool = true;

// #[derive(Debug)]
pub struct CRDTState {
    client_data: Vec<ClientData>,

    marker_tree: Pin<Box<MarkerTree<Entry>>>,

    // Probably temporary, eventually.
    text_content: Rope,

    // ops_from_client: Vec<Vec<
}


impl CRDTState {
    pub fn new() -> Self {
        CRDTState {
            client_data: Vec::new(),
            marker_tree: MarkerTree::new(),
            text_content: Rope::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.marker_tree.as_ref().len()
    }

    pub fn get_or_create_client_id(&mut self, name: &str) -> AgentId {
        if let Some(id) = self.get_client_id(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: InlinableString::from(name),
                markers: SplitList::new()
                // markers: Vec::new()
            });
            (self.client_data.len() - 1) as AgentId
        }
    }

    fn get_client_id(&self, name: &str) -> Option<AgentId> {
        self.client_data.iter()
        .position(|client_data| &client_data.name == name)
        .map(|id| id as AgentId)
    }

    fn notify(client_data: &mut Vec<ClientData>, entry: Entry, ptr: NonNull<NodeLeaf<Entry>>) {
        // eprintln!("notify callback {:?} {:?}", entry, ptr);
        let markers = &mut client_data[entry.loc.agent as usize].markers;
        // for op in &mut markers[loc.seq as usize..(loc.seq+len) as usize] {
        //     *op = ptr;
        // }

        markers.replace_range(entry.loc.seq as usize, MarkerEntry { ptr, len: entry.len() as u32 });
    }

    pub fn insert(&mut self, client_id: AgentId, pos: usize, text: &str) -> CRDTLocation {
        let inserted_length = text.chars().count();
        // First lookup and insert into the marker tree
        let markers = &mut self.client_data[client_id as usize].markers;
        let loc_base = CRDTLocation {
            agent: client_id,
            seq: markers.len() as _
        };

        // let dangling_ptr = NonNull::dangling();
        // markers.resize(markers.len() + inserted_length, dangling_ptr);

        let client_data = &mut self.client_data;

        let cursor = self.marker_tree.cursor_at_pos(pos, true);
        // println!("root {:#?}", self.marker_tree);
        let insert_location = if pos == 0 {
            // This saves an awful lot of code needing to be executed.
            CRDT_DOC_ROOT
        } else { cursor.clone().tell_predecessor() };

        let new_entry = Entry {
            loc: loc_base,
            len: inserted_length as i32
        };

        // self.marker_tree.insert(pos, cursor, new_entry, |entry, leaf| {
        self.marker_tree.insert(cursor, new_entry, |entry, leaf| {
            // println!("insert callback {:?}", entry);
            CRDTState::notify(client_data, entry, leaf);
            // let ops = &mut client_data[loc.client as usize].ops;
            // for op in &mut ops[loc.seq as usize..(loc.seq+len) as usize] {
            //     *op = leaf;
            // }
        });

        if USE_INNER_ROPE {
            self.text_content.insert(pos as usize, text);
            assert_eq!(self.text_content.len_chars(), self.marker_tree.len());
        }

        if cfg!(debug_assertions) {
            // Check all the pointers have been assigned.
            // let markers = &mut self.client_data[client_id as usize].ops;
            // for e in &markers[markers.len() - inserted_length..] {
            //     assert_ne!(*e, dangling_ptr);
            // }
        }

        insert_location
    }

    pub fn insert_name(&mut self, client_name: &str, pos: usize, text: &str) -> CRDTLocation {
        let id = self.get_or_create_client_id(client_name);
        self.insert(id, pos, text)
    }

    pub fn delete(&mut self, _client_id: AgentId, pos: usize, len: usize) -> DeleteResult {
        let cursor = self.marker_tree.cursor_at_pos(pos, true);
        // println!("{:#?}", state.marker_tree);
        // println!("{:?}", cursor);
        let client_data = &mut self.client_data;
        // dbg!("delete list", &self.client_data[0].markers);
        let result = MarkerTree::local_delete(&self.marker_tree, cursor, len, |entry, leaf| {
            // eprintln!("notify {:?} / {}", loc, len);
            CRDTState::notify(client_data, entry, leaf);
        });

        if USE_INNER_ROPE {
            self.text_content.remove(pos as usize..pos as usize + len as usize); // vomit.
            assert_eq!(self.text_content.len_chars(), self.marker_tree.len());
        }

        result
    }

    pub fn delete_name(&mut self, client_name: &str, pos: usize, len: usize) -> DeleteResult {
        let id = self.get_or_create_client_id(client_name);
        self.delete(id, pos, len)
    }

    pub fn lookup_crdt_position(&self, loc: CRDTLocation) -> u32 {
        if loc == CRDT_DOC_ROOT { return 0; }

        let markers = &self.client_data[loc.agent as usize].markers;
        unsafe { MarkerTree::lookup_position(loc, markers[loc.seq as usize]) }
    }

    pub fn lookup_num_position(&self, pos: usize) -> CRDTLocation {
        // let insert_location = if pos == 0 {
        //     // This saves an awful lot of code needing to be executed.
        //     CRDT_DOC_ROOT
        // } else { cursor.tell() };

        let cursor = self.marker_tree.cursor_at_pos(pos, true);
        cursor.tell_predecessor()
    }

    pub fn lookup_position_name(&self, client_name: &str, seq: usize) -> u32 {
        let id = self.get_client_id(client_name).expect("Invalid client name");
        self.lookup_crdt_position(CRDTLocation {
            agent: id,
            seq: seq as u32,
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
    use rand::{Rng, SeedableRng};
    use rand::rngs::SmallRng;


    // use ropey::Rope;
    use super::*;
    use crate::alloc::ALLOCATED;
    use std::sync::atomic::Ordering;

    fn random_str(len: usize, rng: &mut SmallRng) -> String {
        let mut str = String::new();
        let alphabet: Vec<char> = "abcdefghijklmnop ".chars().collect();
        for _ in 0..len {
            str.push(alphabet[rng.gen_range(0..alphabet.len())]);
        }
        str
    }

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
        state.insert_name("fred", 0, "hi there");
        assert_eq!(state.lookup_num_position(0), CRDT_DOC_ROOT);
    }


    #[test]
    fn junk_append() {
        let mut state = CRDTState::new();

        // Fill the document with junk. We need to use 2 different users here so
        // the content doesn't get merged.
        let mut pos = 0;
        for _ in 0..50 {
            state.insert_name("fred", pos, "fred");
            state.insert_name("george", pos + 4, "george");
            pos += 10;
            state.check();
        }
        
        // eprintln!("state {:#?}", state);
        state.check();
    }

    
    #[test]
    fn junk_prepend() {
        let mut state = CRDTState::new();

        // Repeatedly inserting at 0 will prevent all the nodes collapsing, so we don't
        // need to worry about that.
        for _ in 0..65 {
            state.insert_name("fred", 0, "fred");
            state.check();

            // state.marker_tree.print_ptr_tree();
            // dbg!(&state.marker_tree);
        }
    
        state.check();
    }
    
    
    #[test]
    fn delete() {
        let mut state = CRDTState::new();
        
        state.insert_name("fred", 0, "a");
        state.insert_name("george", 1, "bC");
        
        state.insert_name("fred", 3, "D");
        state.insert_name("george", 4, "EFgh");

        // println!("tree {:#?}", state.marker_tree);
        // Delete CDEF
        let result = state.delete_name("amanda", 2, 4);
        assert_eq!(result.len(), 3); // TODO: More thorough equality checks here.
        // eprintln!("delete result {:#?}", result);

        println!("tree {:#?}", state.marker_tree);
        state.check();
        assert_eq!(state.len(), 4);

        // state.marker_tree.print_stats();
    }

    #[test]
    fn delete_end() {
        let mut state = CRDTState::new();

        state.insert_name("fred", 0, "abc");
        let _result = state.delete_name("amanda", 1, 2);
        assert_eq!(state.len(), 1);
        state.check();
    }

    #[test]
    fn random_inserts_deletes() {
        let mut doc_len = 0;
        let mut state = CRDTState::new();
        state.get_or_create_client_id("seph"); // Create client id 0.

        // Stable between runs for reproducing bugs.
        let mut rng = SmallRng::seed_from_u64(1234);

        for i in 0..10000 {
            if i % 1000 == 0 {
                println!("i {} doc len {}", i, doc_len);
            }

            let insert_weight = if doc_len < 1000 { 0.55 } else { 0.45 };
            if doc_len == 0 || rng.gen_bool(insert_weight) {
                // Insert something.
                let pos = rng.gen_range(0..=doc_len);
                let len: usize = rng.gen_range(1..10); // Ideally skew toward smaller inserts.
                state.insert(0, pos, random_str(len as usize, &mut rng).as_str());
                doc_len += len;
            } else {
                // Delete something
                let pos = rng.gen_range(0..doc_len);
                // println!("range {}", u32::min(10, doc_len - pos));
                let len = rng.gen_range(1..=usize::min(10, doc_len - pos));
                state.delete(0, pos, len);
                doc_len -= len;
            }

            // Calling check gets slow as the document grows. There's a tradeoff here between
            // iterations and check() calls.
            if i % 100 == 0 { state.check(); }
            // state.check();
            assert_eq!(state.len(), doc_len as usize);
        }
    }

    #[test]
    #[ignore]
    fn automerge_perf() {
        // This test also shows up in the benchmarks. Its included here as well because run as part
        // of the test suite it checks a lot of invariants throughout the run.
        use serde::Deserialize;
        use std::fs::File;
        use std::io::BufReader;

        #[derive(Debug, Clone, Deserialize)]
        struct Edit(usize, usize, String);

        #[derive(Debug, Clone, Deserialize)]
        #[allow(non_snake_case)] // field names match JSON.
        struct TestData {
            edits: Vec<Edit>,
            finalText: String,
        }

        let file = File::open("automerge-trace.json").unwrap();
        let reader = BufReader::new(file);
        let u: TestData = serde_json::from_reader(reader).unwrap();
        println!("final length: {}, edits {}", u.finalText.len(), u.edits.len());

        println!("alloc {}", ALLOCATED.load(Ordering::Acquire));

        let mut state = CRDTState::new();
        let id = state.get_or_create_client_id("jeremy");
        for (_i, Edit(pos, del_len, ins_content)) in u.edits.iter().enumerate() {
            // if i % 1000 == 0 {
            //     println!("i {}", i);
            // }
            // println!("iter {} pos {} del {} ins '{}'", i, pos, del_len, ins_content);
            if *del_len > 0 {
                state.delete(id, *pos as _, *del_len as _);
            } else {
                state.insert(id, *pos as _, ins_content);
            }
        }
        // println!("len {}", state.len());
        assert_eq!(state.len(), u.finalText.len());
        // assert!(state.text_content.eq(&u.finalText));

        // state.client_data[0].markers.print_stats();
        // state.marker_tree.print_stats();
        println!("alloc {}", ALLOCATED.load(Ordering::Acquire));

        println!("final node total {}", state.marker_tree.count_entries());
    }
}

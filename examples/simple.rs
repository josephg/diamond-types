use text_crdt_rust::CRDTState;
use inlinable_string::InlinableString;

fn main() {
    let mut state = CRDTState::new();

    state.insert_name("fred", 0, InlinableString::from("a"));
    state.insert_name("george", 1, InlinableString::from("bC"));

    state.insert_name("fred", 3, InlinableString::from("D"));
    state.insert_name("george", 4, InlinableString::from("EFgh"));

    // println!("tree {:#?}", state.marker_tree);
    // Delete CDEF
    let result = state.delete_name("amanda", 2, 4);
    // eprintln!("delete result {:#?}", result);
    assert_eq!(state.len(), 4);
}
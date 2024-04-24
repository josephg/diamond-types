use crate::encoding::parseerror::ParseError;
use crate::list::{ListCRDT, ListOpLog};
use crate::list::encoding::decode_oplog::{dbg_print_chunks_in, DecodeOptions};
use crate::frontier::local_frontier_eq;
use super::*;

fn simple_doc() -> ListCRDT {
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("seph");
    doc.insert(0, 0, "hi there");
    // TODO: Make another test where we store this stuff...
    doc.delete_without_content(0, 3..7); // 'hi e'
    doc.insert(0, 3, "m");
    doc
}

fn check_encode_decode_matches(oplog: &ListOpLog) {
    let data = oplog.encode(&EncodeOptions {
        user_data: None,
        store_start_branch_content: true,
        experimentally_store_end_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: true,
        compress_content: true,
        verbose: false,
    });

    let oplog2 = ListOpLog::load_from(&data).unwrap();

    // dbg!(oplog, &oplog2);

    assert_eq!(oplog, &oplog2);
}

#[test]
fn encode_decode_smoke_test() {
    let doc = simple_doc();
    let data = doc.oplog.encode(&EncodeOptions::default());

    let result = ListOpLog::load_from(&data).unwrap();
    // dbg!(&result);

    dbg!(&doc.oplog);
    dbg!(&result);
    assert_eq!(&result, &doc.oplog);
    // dbg!(&result);
}

#[test]
fn decode_in_parts() {
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("seph");
    doc.get_or_create_agent_id("mike");
    doc.insert(0, 0, "hi there");

    let data_1 = doc.oplog.encode(&EncodeOptions::default());
    let f1 = doc.oplog.cg.version.clone();

    doc.delete_without_content(1, 3..7); // 'hi e'
    doc.insert(0, 3, "m");
    let f2 = doc.oplog.cg.version.clone();

    let data_2 = doc.oplog.encode_from(&EncodeOptions::default(), f1.as_ref());

    let mut d2 = ListOpLog::new();
    let m1 = d2.decode_and_add(&data_1).unwrap();
    assert_eq!(m1, f1);
    let m2 = d2.decode_and_add(&data_2).unwrap();
    assert_eq!(m2, f2);
    // dbg!(m1, m2);

    assert_eq!(&d2, &doc.oplog);
    // dbg!(&doc.ops, &d2);
}

#[test]
fn merge_parts() {
    let mut oplog = ListOpLog::new();
    oplog.get_or_create_agent_id("seph");
    oplog.add_insert(0, 0, "hi");
    let data_1 = oplog.encode(&EncodeOptions::default());
    oplog.add_insert(0, 2, " there");
    let data_2 = oplog.encode(&EncodeOptions::default());

    let mut log2 = ListOpLog::load_from(&data_1).unwrap();
    println!("\n------\n");
    let final_v = log2.decode_and_add(&data_2).unwrap();
    assert_eq!(&oplog, &log2);
    assert_eq!(final_v, oplog.cg.version);
}

#[test]
fn merge_future_patch_errors() {
    let oplog = simple_doc().oplog;
    let v = oplog.cg.version[0];
    let bytes = oplog.encode_from(&ENCODE_FULL, &[v-1]);

    let err = ListOpLog::load_from(&bytes).unwrap_err();
    assert_eq!(err, ParseError::BaseVersionUnknown);
}

// This test is ignored because it errors (arguably correctly) when reading the base version at
// an unknown point in time. TODO: Rewrite this to make it work.
#[test]
#[ignore]
fn merge_parts_2() {
    let mut oplog_a = ListOpLog::new();
    oplog_a.get_or_create_agent_id("a");
    oplog_a.get_or_create_agent_id("b");

    let t1 = oplog_a.add_insert(0, 0, "aa");
    let data_a = oplog_a.encode(&EncodeOptions::default());

    oplog_a.add_insert_at(1, &[], 0, "bbb");
    let data_b = oplog_a.encode_from(&EncodeOptions::default(), &[t1]);

    // Now we should be able to merge a then b, or b then a and get the same result.
    let mut a_then_b = ListOpLog::new();
    a_then_b.decode_and_add(&data_a).unwrap();
    a_then_b.decode_and_add(&data_b).unwrap();
    assert_eq!(a_then_b, oplog_a);

    println!("\n------\n");

    let mut b_then_a = ListOpLog::new();
    b_then_a.decode_and_add(&data_b).unwrap();
    b_then_a.decode_and_add(&data_a).unwrap();
    assert_eq!(b_then_a, oplog_a);
}

#[test]
fn with_deleted_content() {
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("seph");
    doc.insert(0, 0, "abcd");
    doc.delete(0, 1..3); // delete "bc"

    check_encode_decode_matches(&doc.oplog);
}

#[test]
fn encode_reordered() {
    let mut oplog = ListOpLog::new();
    oplog.get_or_create_agent_id("seph");
    oplog.get_or_create_agent_id("mike");
    let a = oplog.add_insert_at(0, &[], 0, "a");
    oplog.add_insert_at(1, &[], 0, "b");
    oplog.add_insert_at(0, &[a], 1, "c");

    // dbg!(&oplog);
    check_encode_decode_matches(&oplog);
}

#[test]
fn encode_with_agent_shared_between_branches() {
    // Same as above, but only one agent ID.
    let mut oplog = ListOpLog::new();
    oplog.get_or_create_agent_id("seph");
    let a = oplog.add_insert_at(0, &[], 0, "a");
    oplog.add_insert_at(0, &[], 0, "b");
    oplog.add_insert_at(0, &[a], 1, "c");

    // dbg!(&oplog);
    check_encode_decode_matches(&oplog);
}

#[test]
#[ignore]
fn decode_example() {
    let bytes = std::fs::read("../../benchmark_data/node_nodecc.dt").unwrap();
    let oplog = ListOpLog::load_from(&bytes).unwrap();

    // for c in &oplog.client_data {
    //     println!("{} .. {}", c.name, c.get_next_seq());
    // }
    dbg!(oplog.operations.0.len());
    dbg!(oplog.cg.graph.entries.0.len());
}

fn check_unroll_works(dest: &ListOpLog, src: &ListOpLog) {
    // So we're going to decode the oplog with all the different bytes corrupted. The result
    // should always fail if we check the CRC.

    let encoded_proper = src.encode(&EncodeOptions {
        user_data: None,
        store_start_branch_content: true,
        experimentally_store_end_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: true,
        compress_content: true,
        verbose: false
    });

    // dbg!(encoded_proper.len());
    for i in 0..encoded_proper.len() {
        // let i = 55;
        // println!("{i}");
        // We'll corrupt that byte and try to read the document back.
        let mut corrupted = encoded_proper.clone();
        corrupted[i] = !corrupted[i];
        // dbg!(corrupted[i]);

        let mut actual_output = dest.clone();
        // dbg!(&actual_output.cg);

        // In theory, we should always get an error here. But we don't, because the CRC check
        // is optional and the corrupted data can just remove the CRC check entirely!

        let result = actual_output.decode_and_add_opts(&corrupted, DecodeOptions {
            ignore_crc: false,
            verbose: true,
        });

        if let Err(_err) = result {
            // dbg!(&actual_output.cg);
            // dbg!(&dest.cg);

            assert_eq!(&actual_output, dest);
        } else {
            // dbg!(&actual_output);
            // dbg!(src);
            assert_eq!(&actual_output, src);
        }
        // Otherwise the data loaded correctly!

    }
}

#[test]
fn error_unrolling() {
    let doc = simple_doc();

    check_unroll_works(&ListOpLog::new(), &doc.oplog);
}

#[test]
fn save_load_save_load() {
    let oplog1 = simple_doc().oplog;
    let bytes = oplog1.encode(&EncodeOptions {
        user_data: None,
        store_start_branch_content: true,
        // store_inserted_content: true,
        // store_deleted_content: true,
        experimentally_store_end_branch_content: false,
        store_inserted_content: false,
        store_deleted_content: false,
        compress_content: true,
        verbose: false
    });
    dbg_print_chunks_in(&bytes);
    let oplog2 = ListOpLog::load_from(&bytes).unwrap();
    // dbg!(&oplog2);

    let bytes2 = oplog2.encode(&EncodeOptions {
        user_data: None,
        store_start_branch_content: true,
        experimentally_store_end_branch_content: false,
        store_inserted_content: false, // Need to say false here to avoid an assert for this.
        store_deleted_content: true,
        compress_content: true,
        verbose: false
    });
    let oplog3 = ListOpLog::load_from(&bytes2).unwrap();

    // dbg!(oplog3);
    assert_eq!(oplog2, oplog3);
}

#[test]
fn doc_id_preserved() {
    let mut oplog = simple_doc().oplog;
    oplog.doc_id = Some("hi".into());
    let bytes = oplog.encode(&ENCODE_FULL);
    let result = ListOpLog::load_from(&bytes).unwrap();

    // Eq should check correctly.
    assert_eq!(oplog, result);
    // But we'll make sure here because its easy.
    assert_eq!(oplog.doc_id, result.doc_id);
}

#[test]
fn mismatched_doc_id_errors() {
    let mut oplog1 = simple_doc().oplog;
    oplog1.doc_id = Some("aaa".into());

    let mut oplog2 = simple_doc().oplog;
    oplog2.doc_id = Some("bbb".into());

    let bytes = oplog1.encode(&ENCODE_FULL);
    assert_eq!(oplog2.decode_and_add(&bytes).unwrap_err(), ParseError::DocIdMismatch);
    assert_eq!(oplog2.doc_id, Some("bbb".into())); // And the doc ID should be unchanged
}

#[test]
fn doc_id_preserved_when_error_happens() {
    let mut oplog1 = ListOpLog::new();

    let mut oplog2 = simple_doc().oplog;
    oplog2.doc_id = Some("bbb".into());

    let mut bytes = oplog2.encode(&ENCODE_FULL);
    let last_byte = bytes.last_mut().unwrap();
    *last_byte = !*last_byte; // Any change should mess up the checksum and fail.

    // Merging should fail
    oplog1.decode_and_add(&bytes).unwrap_err();
    // And the oplog's doc_id should be unchanged.
    assert_eq!(oplog1.doc_id, None);
}

#[test]
fn merge_returns_root_for_empty_file() {
    let oplog = ListOpLog::new();
    let bytes = oplog.encode(&ENCODE_FULL);

    let mut result = ListOpLog::new();
    let version = result.decode_and_add(&bytes).unwrap();
    assert!(local_frontier_eq(&version, &[]));
}

#[test]
fn merge_returns_version_even_with_overlap() {
    let oplog = simple_doc().oplog;
    let bytes = oplog.encode(&ENCODE_FULL);

    let mut oplog2 = oplog.clone();
    let version = oplog2.decode_and_add(&bytes).unwrap();

    assert!(local_frontier_eq(&version, oplog2.local_frontier_ref()));
}

#[test]
fn merge_patch_returns_correct_version() {
    // This was returning [4, ROOT_VERSION] or some nonsense.
    let mut oplog = simple_doc().oplog;
    let v = oplog.cg.version.clone();
    let mut oplog2 = oplog.clone();

    oplog.add_insert(0, 0, "x");

    let bytes = oplog.encode_from(&ENCODE_FULL, v.as_ref());

    let version = oplog2.decode_and_add(&bytes).unwrap();

    // dbg!(version);
    assert!(local_frontier_eq(&version, oplog2.local_frontier_ref()));
}

#[test]
fn merge_when_parents_unsorted() {
    let data: Vec<u8> = vec![68,77,78,68,84,89,80,83,0,1,224,1,3,221,1,12,52,111,114,55,75,56,78,112,52,109,122,113,12,90,77,80,70,45,69,49,95,116,114,114,74,12,68,80,84,95,104,99,107,75,121,55,102,77,12,82,56,108,87,77,99,112,54,76,68,99,83,12,53,98,78,79,116,82,85,56,120,88,113,83,12,100,85,101,81,83,77,66,54,122,45,72,115,12,50,105,105,80,104,101,116,101,85,107,57,49,12,108,65,71,75,68,90,68,53,108,111,99,75,12,78,113,55,109,65,70,55,104,67,56,52,122,12,116,51,113,52,84,101,121,73,76,85,54,53,12,120,95,120,51,68,95,105,109,81,100,78,115,12,102,120,103,87,90,100,82,111,105,108,73,99,12,115,87,67,73,67,97,78,100,68,65,77,86,12,110,100,56,118,55,74,79,45,114,81,122,45,12,110,85,69,75,69,73,53,81,49,49,45,83,12,120,97,55,121,102,81,88,98,45,120,54,87,12,85,116,82,100,98,71,117,106,57,49,98,49,10,7,12,2,0,0,13,1,4,20,157,2,24,182,1,0,13,174,1,4,120,100,102,120,120,102,100,115,49,120,120,121,122,113,119,101,114,115,100,102,115,100,115,100,97,115,100,115,100,115,100,115,100,97,115,100,97,115,100,113,119,101,119,113,101,119,113,119,107,106,107,106,107,106,107,107,106,107,106,107,108,106,108,107,106,108,107,106,108,107,106,101,101,114,108,106,107,114,101,108,107,116,101,114,116,101,111,114,106,116,111,105,101,106,114,116,111,105,119,106,100,97,98,99,49,49,49,57,49,98,115,110,102,103,104,102,100,103,104,100,102,103,104,100,103,104,100,102,103,104,100,102,103,104,100,107,106,102,108,107,115,100,106,102,108,115,59,107,106,107,108,106,59,107,106,107,106,107,106,59,107,106,108,59,107,106,59,107,108,106,107,106,108,25,2,219,2,21,44,2,3,4,1,6,4,8,1,10,1,12,10,14,1,16,1,18,1,20,4,22,4,24,18,26,99,28,58,30,4,28,1,30,1,32,3,34,2,32,1,34,23,32,39,22,31,81,175,1,21,177,2,239,4,77,169,3,223,6,107,33,79,9,0,26,47,3,0,19,3,18,42,177,1,187,2,43,23,19,211,1,1,1,8,3,10,4,1,8,2,6,8,1,8,22,4,39,96,100,4,142,143,169,235];
    let oplog = ListOpLog::load_from(&data).unwrap();
    // dbg!(&oplog);
    oplog.dbg_check(true);
    oplog.checkout_tip();
}

#[test]
fn regression_1() {
    // I have no idea what bug this caught.
    let doc_data: Vec<u8> = vec![68,77,78,68,84,89,80,83,0,1,28,3,26,12,119,74,74,112,83,108,69,108,72,100,101,53,12,111,74,97,104,71,111,70,103,84,66,114,88,10,7,12,2,0,0,13,1,4,20,34,24,15,0,13,9,4,102,100,115,97,97,115,100,102,25,1,17,21,4,2,4,4,4,22,3,33,35,9,23,4,4,1,4,1,100,4,4,98,110,26];
    let patch_data: Vec<u8> = vec![68,77,78,68,84,89,80,83,0,1,28,3,26,12,119,74,74,112,83,108,69,108,72,100,101,53,12,111,74,97,104,71,111,70,103,84,66,114,88,10,6,12,4,3,0,4,3,20,26,24,10,0,13,4,4,100,115,97,25,1,7,21,3,3,3,2,22,2,27,2,23,3,3,5,0,100,4,65,22,13,47];
    // let doc_data: Vec<u8> = vec![68,77,78,68,84,89,80,83,0,1,187,2,3,184,2,12,52,111,114,55,75,56,78,112,52,109,122,113,12,90,77,80,70,45,69,49,95,116,114,114,74,12,68,80,84,95,104,99,107,75,121,55,102,77,12,82,56,108,87,77,99,112,54,76,68,99,83,12,53,98,78,79,116,82,85,56,120,88,113,83,12,100,85,101,81,83,77,66,54,122,45,72,115,12,50,105,105,80,104,101,116,101,85,107,57,49,12,108,65,71,75,68,90,68,53,108,111,99,75,12,78,113,55,109,65,70,55,104,67,56,52,122,12,116,51,113,52,84,101,121,73,76,85,54,53,12,120,95,120,51,68,95,105,109,81,100,78,115,12,102,120,103,87,90,100,82,111,105,108,73,99,12,115,87,67,73,67,97,78,100,68,65,77,86,12,110,100,56,118,55,74,79,45,114,81,122,45,12,110,85,69,75,69,73,53,81,49,49,45,83,12,120,97,55,121,102,81,88,98,45,120,54,87,12,85,116,82,100,98,71,117,106,57,49,98,49,12,100,120,97,65,122,104,98,50,54,88,114,105,12,86,78,83,81,118,120,89,106,118,88,55,76,12,68,81,110,48,84,67,120,81,85,90,79,78,12,85,109,57,115,105,121,71,84,88,74,81,79,12,108,69,103,121,89,116,52,87,105,53,52,119,12,76,98,121,115,84,66,118,51,122,72,115,117,12,121,87,116,89,108,120,114,48,120,98,106,101,10,7,12,2,0,0,13,1,4,20,239,2,24,203,1,0,13,195,1,4,120,100,102,120,120,102,100,115,49,120,120,121,122,113,119,101,114,115,100,102,115,100,115,100,97,115,100,115,100,115,100,115,100,97,115,100,97,115,100,113,119,101,119,113,101,119,113,119,107,106,107,106,107,106,107,107,106,107,106,107,108,106,108,107,106,108,107,106,108,107,106,101,101,114,108,106,107,114,101,108,107,116,101,114,116,101,111,114,106,116,111,105,101,106,114,116,111,105,119,106,100,97,98,99,49,49,49,57,49,98,115,110,102,103,104,102,100,103,104,100,102,103,104,100,103,104,100,102,103,104,100,102,103,104,100,107,106,102,108,107,115,100,106,102,108,115,59,107,106,107,108,106,59,107,106,107,106,107,106,59,107,106,108,59,107,106,59,107,108,106,107,106,108,97,102,100,115,97,115,100,102,102,97,115,100,102,100,115,97,97,115,100,102,102,25,2,133,3,21,66,2,3,4,1,6,4,8,1,10,1,12,10,14,1,16,1,18,1,20,4,22,4,24,18,26,99,28,58,30,4,28,1,30,1,32,3,34,2,32,1,34,23,32,39,36,66,38,1,40,4,38,3,42,8,44,1,42,4,44,3,46,8,48,4,46,1,22,52,81,175,1,21,177,2,239,4,77,169,3,223,6,107,33,79,9,0,26,47,3,0,19,3,18,187,1,5,187,2,43,175,8,87,0,35,3,27,7,143,1,9,0,35,3,27,7,143,1,9,33,74,23,37,211,1,1,1,8,3,10,4,1,8,2,10,4,23,8,39,96,67,162,1,4,4,8,3,20,9,18,4,4,8,3,20,12,18,4,1,20,100,4,16,215,118,144];
    // let patch_data: Vec<u8> = vec![68,77,78,68,84,89,80,83,0,1,28,3,26,12,121,87,116,89,108,120,114,48,120,98,106,101,12,76,98,121,115,84,66,118,51,122,72,115,117,10,6,12,4,3,0,4,8,20,26,24,10,0,13,4,4,115,100,102,25,1,7,21,3,3,3,2,22,2,27,2,23,3,3,5,0,100,4,233,122,109,54];

    let mut oplog = ListOpLog::load_from(&doc_data).unwrap();
    // dbg!(&oplog);
    println!("\n\n");
    oplog.decode_and_add(&patch_data).unwrap();
    oplog.dbg_check(true);
}

#[test]
fn compat_empty_doc() {
    // This is an empty document from before I made a couple small tweaks. Break compatibility,
    // but do it intentionally.

    // In the older format, I stored StartBranch even when it was ROOT.
    // (From commit 5d1d21cd519a2c631aa1fedc59744f30c0787488)
    let bytes1 = &[0x44,0x4d,0x4e,0x44,0x54,0x59,0x50,0x53,0x00,0x01,0x02,0x03,0x00,0x0a,0x07,0x0c,0x02,0x00,0x00,0x0d,0x01,0x04,0x14,0x06,0x15,0x00,0x16,0x00,0x17,0x00,0x64,0x04,0x6c,0xce,0x6b,0x00];
    // In the newer format, StartBranch is an empty chunk when the document starts at ROOT.
    let bytes2 = &[0x44,0x4d,0x4e,0x44,0x54,0x59,0x50,0x53,0x00,0x01,0x02,0x03,0x00,0x0a,0x00,0x14,0x06,0x15,0x00,0x16,0x00,0x17,0x00,0x64,0x04,0x86,0x77,0x4d,0x6a];

    let expect = ListOpLog::new();
    let a = ListOpLog::load_from(bytes1).unwrap();
    assert_eq!(expect, a);
    let b = ListOpLog::load_from(bytes2).unwrap();
    assert_eq!(expect, b);
}

#[test]
fn compat_simple_doc() {
    // This is copy + pasted here (from simple_doc() above) because this test should stay the
    // same even if I goof with the encoding above.
    let mut doc = ListCRDT::new();
    doc.get_or_create_agent_id("seph");
    doc.insert(0, 0, "hi there");
    doc.delete_without_content(0, 3..7); // 'hi e'
    doc.insert(0, 3, "m");

    dbg!(&doc.oplog.encode(&EncodeOptions {
        user_data: None,
        store_start_branch_content: false,
        experimentally_store_end_branch_content: false,
        store_inserted_content: true,
        store_deleted_content: false,
        compress_content: true,
        verbose: false
    }));

    // From commit 5d1d21cd519a2c631aa1fedc59744f30c0787488
    let bytes1 = &[68,77,78,68,84,89,80,83,0,1,7,3,5,4,115,101,112,104,10,7,12,2,0,0,13,1,4,20,32,24,16,0,13,10,4,104,105,32,116,104,101,114,101,109,25,1,19,21,2,2,13,22,4,65,79,11,0,23,2,13,1,100,4,162,205,138,38];
    assert_eq!(ListOpLog::load_from(bytes1).unwrap(), doc.oplog);

    // From commit xxx
    // With compression disabled, or artificially cranked to compress everything:
    let bytes2_uncompressed = &[68,77,78,68,84,89,80,83,0,1,7,3,5,4,115,101,112,104,10,0,20,32,24,16,0,13,10,4,104,105,32,116,104,101,114,101,109,25,1,19,21,2,2,13,22,4,65,79,11,0,23,2,13,1,100,4,151,117,95,151];
    assert_eq!(ListOpLog::load_from(bytes2_uncompressed).unwrap(), doc.oplog);

    if cfg!(feature = "lz4") {
        let bytes2_compressed_full = &[68, 77, 78, 68, 84, 89, 80, 83, 0, 5, 11, 9, 144, 104, 105, 32, 116, 104, 101, 114, 101, 109, 1, 7, 3, 5, 4, 115, 101, 112, 104, 10, 0, 20, 24, 24, 8, 0, 14, 2, 4, 9, 25, 1, 19, 21, 2, 2, 13, 22, 4, 65, 79, 11, 0, 23, 2, 13, 1, 100, 4, 128, 32, 8, 191];
        assert_eq!(ListOpLog::load_from(bytes2_compressed_full).unwrap(), doc.oplog);
    }
}
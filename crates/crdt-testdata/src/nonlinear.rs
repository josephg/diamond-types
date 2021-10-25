// Nonlinear data has some different fields.

use std::fs::File;
use std::io::BufReader;
use crate::TestPatch;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct NLId {
    pub agent: u32,
    pub seq: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NLPatch {
    pub id: NLId,
    pub parents: Vec<NLId>,
    pub timestamp: String,
    pub patch: TestPatch,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NLDataset {
    #[serde(rename = "startContent")]
    pub start_content: String,
    pub ops: Vec<NLPatch>,
}


pub fn load_nl_testing_data(filename: &str) -> NLDataset {
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);

    // TODO: Add gzip compression.
    serde_json::from_reader(reader).unwrap()
}

// #[test]
// fn foo() {
//     let d = load_nl_testing_data("/home/seph/src/crdt-benchmarks/xml/out/G1-3.json");
//     dbg!(&d);
// }
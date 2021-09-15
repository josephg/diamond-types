// use std::time::SystemTime;
use std::fs::File;
use std::io::{BufReader, Read};
use flate2::bufread::GzDecoder;
use serde::Deserialize;

/// This file contains some simple helpers for loading test data. Its used by benchmarking and
/// testing code.

#[derive(Debug, Clone, Deserialize)]
pub struct TestPatch(pub usize, pub usize, pub String);

#[derive(Debug, Clone, Deserialize)]
pub struct TestTxn {
    // time: String, // ISO String. Unused.
    pub patches: Vec<TestPatch>
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestData {
    #[serde(rename = "startContent")]
    pub start_content: String,
    #[serde(rename = "endContent")]
    pub end_content: String,

    pub txns: Vec<TestTxn>,
}

impl TestData {
    pub fn len(&self) -> usize {
        self.txns.iter()
            .map(|txn| { txn.patches.len() })
            .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        !self.txns.iter().any(|txn| !txn.patches.is_empty())
    }
}

pub fn load_testing_data(filename: &str) -> TestData {
    // let start = SystemTime::now();
    // let mut file = File::open("benchmark_data/automerge-paper.json.gz").unwrap();
    let file = File::open(filename).unwrap();

    let reader = BufReader::new(file);
    // We could pass the GzDecoder straight to serde, but it makes it way slower to parse for
    // some reason.
    let mut reader = GzDecoder::new(reader);
    let mut raw_json = vec!();
    reader.read_to_end(&mut raw_json).unwrap();

    // println!("uncompress time {}", start.elapsed().unwrap().as_millis());

    // let start = SystemTime::now();
    let data: TestData = serde_json::from_reader(raw_json.as_slice()).unwrap();
    // println!("JSON parse time {}", start.elapsed().unwrap().as_millis());

    data
}

#[cfg(test)]
mod tests {
    use crate::load_testing_data;

    #[test]
    fn it_works() {
        let data = load_testing_data("../../benchmark_data/sveltecomponent.json.gz");
        assert!(data.txns.len() > 0);
    }
}

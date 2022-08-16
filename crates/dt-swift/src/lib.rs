use diamond_types::AgentId;
use diamond_types::list::{ListCRDT as InnerListCRDT};
use diamond_types::list::encoding::ENCODE_FULL;
use rand::{distributions::Alphanumeric, Rng};

#[swift_bridge::bridge]
mod ffi {
    extern "Rust" {
        type ListCRDT;

        #[swift_bridge(init)]
        fn new() -> ListCRDT;
        // #[swift_bridge(init)]
        // fn new(agent_name: Option<&str>) -> ListCRDT;

        pub fn replace_wchar(&mut self, wchar_pos: usize, remove: usize, ins: &str);

        pub fn encode(&self) -> Vec<u8>;
        pub fn save(&self, path: &str);

        pub fn to_string(&self) -> String;

        fn decode(bytes: &[u8]) -> ListCRDT;
        fn load_or_new(path: &str) -> ListCRDT;
    }

}


pub struct ListCRDT {
    inner: InnerListCRDT,
    agent_id: AgentId,
}


fn create_agent(crdt: &mut InnerListCRDT) -> AgentId {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    crdt.get_or_create_agent_id(&s)
}
// fn get_agent(crdt: &mut InnerListCRDT, agent_name: Option<&str>) -> AgentId {
//     agent_name.map(|name| {
//         crdt.get_or_create_agent_id(name)
//     }).unwrap_or_else(|| {
//         let s: String = rand::thread_rng()
//             .sample_iter(&Alphanumeric)
//             .take(8)
//             .map(char::from)
//             .collect();
//         crdt.get_or_create_agent_id(&s)
//     })
// }

pub fn decode(bytes: &[u8]) -> ListCRDT {
    let mut inner = InnerListCRDT::load_from(bytes).expect("Failed to decode DT object from bytes");
    let agent_id = create_agent(&mut inner);
    ListCRDT { inner, agent_id }
}
fn load_or_new(path: &str) -> ListCRDT {
    // TODO: Only make a new file if the error is ENOENT
    std::fs::read(path)
        .map(|data| decode(&data))
        .unwrap_or_else(|err| {
            eprintln!("Could not read file: {err}");
            ListCRDT::new()
        })
}

impl ListCRDT {
    pub fn new() -> Self {
        let mut inner = InnerListCRDT::new();
        let agent_id = create_agent(&mut inner);

        Self { inner, agent_id }
    }

    // pub fn ins_unicode(&mut self, pos: usize, content: &str) -> usize {
    //     self.inner.insert(self.agent_id, pos, content)
    //     // let parents: LocalVersion = self.inner.local_version_ref().into();
    //     // self.inner.add_insert_at(self.agent_id.unwrap(), &parents, pos, content)
    // }

    pub fn replace_wchar(&mut self, wchar_pos: usize, remove: usize, ins: &str) {
        if remove > 0 {
            self.inner.delete_at_wchar(self.agent_id, wchar_pos..wchar_pos + remove);
        }
        if !ins.is_empty() {
            self.inner.insert_at_wchar(self.agent_id, wchar_pos, ins);
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.inner.oplog.encode(ENCODE_FULL)
    }

    pub fn save(&self, path: &str) {
        let data = self.encode();
        std::fs::write(path, data).unwrap()
    }

    pub fn to_string(&self) -> String {
        self.inner.branch.content().to_string()
    }
}

// pub struct Branch(DTBranch);
//
// pub struct OpLog {
//     inner: DTOpLog,
//     agent_id: Option<AgentId>,
// }

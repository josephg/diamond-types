
use diamond_types_old::list::ListCRDT;
use diamond_core_old::AgentId;

// Tiny "document" wrapper.
#[derive(Debug)]
struct Doc {
    crdt: ListCRDT,
    agent: AgentId,
}

impl Doc {
    fn new(agent: &str) -> Self {
        let mut crdt = ListCRDT::new();
        let agent = crdt.get_or_create_agent_id(agent);

        Doc { crdt, agent }
    }

    // I'm pretending the content is a usize item to make replays easier.
    fn insert(&mut self, pos: usize, ins_content: &str) {
        self.crdt.local_insert(self.agent, pos, ins_content);
    }
}

fn merge(a: &mut Doc, b: &mut Doc) {
    a.crdt.replicate_into(&mut b.crdt);
    b.crdt.replicate_into(&mut a.crdt);

    // a.crdt.
    assert_eq!(&a.crdt, &b.crdt);
}

#[test]
fn check_algorithm_implements_fugue() {
    let mut a = Doc::new("A");
    let mut b = Doc::new("B");
    let mut c = Doc::new("C");

    a.insert(0, "a");
    b.insert(0, "b");
    merge(&mut a, &mut c);
    c.insert(1, "c");
    merge(&mut a, &mut b);
    b.insert(1, "d");
    merge(&mut b, &mut c);

    // Fugue will generate "abedc", and yjsmod will generate "abdec".
    let _yjsmod_result = "acdb";
    let fugue_result = "adcb";
    let actual_result = c.crdt.to_string();
    assert_eq!(actual_result, fugue_result);
}
use rle::HasLength;
use crate::{AgentId, DTRange, KVPair, RleVec, ROOT_AGENT, LV};
use crate::causalgraph::ClientData;
use crate::rle::RleSpanHelpers;

/// This struct stores the information we need while reading to map from relative agent info and
/// edits to the equivalent local times.
#[derive(Debug, Default)]
pub struct ReadMap {
    /// Map from file's mapped ID -> internal ID, and the last seq we've seen.
    pub agent_map: Vec<(AgentId, usize)>,

    /// Map from the file's relative position -> internal operation position. This usually only
    /// contains 1 entry, which maps the entire file directly across.
    ///
    /// Packed.
    pub txn_map: RleVec<KVPair<DTRange>>,
}

impl ReadMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_time(&self) -> Option<LV> {
        // Another way to implement this would be to default to ROOT_TIME or something and let
        // entries be "simple" if they're first, and they parent off ROOT. But that way lie bugs.
        self.txn_map.last().map(|e| {
            e.1.last()
        })
    }

    pub fn len(&self) -> LV {
        self.txn_map
            .last()
            .map(|e| e.end())
            .unwrap_or(0)
            // .unwrap_or(4000) // For testing.
    }
}

#[derive(Debug, Clone)]
pub struct WriteMap {
    /// Map from oplog's agent ID to the agent id in the file. Paired with the last assigned agent
    /// ID, to support agent IDs bouncing around.
    agent_map: Vec<(Option<AgentId>, usize)>,
    next_mapped_agent: AgentId,

    /// Map from local oplog versions -> file versions. Each entry is KVPair(local start, file range).
    pub txn_map: RleVec<KVPair<DTRange>>,
}

impl WriteMap {
    pub(crate) fn new() -> Self {
        Self {
            agent_map: vec![],
            next_mapped_agent: 0,
            // output: BumpVec::new_in(bump)
            txn_map: Default::default()
        }
    }

    pub(crate) fn with_capacity_from(client_data: &[ClientData]) -> Self {
        Self {
            agent_map: vec![(None, 0); client_data.len()],
            next_mapped_agent: 0,
            // output: BumpVec::new_in(bump)
            txn_map: Default::default()
        }
    }

    pub(crate) fn from_dec(client_data: &[ClientData], read_map: ReadMap) -> Self {
        let mut this = Self::with_capacity_from(client_data);
        this.populate_from_dec(&read_map);
        this
    }

    fn ensure_capacity(&mut self, cap: usize) {
        // There's probably nicer ways to implement this.
        if cap > self.agent_map.len() {
            self.agent_map.resize(cap, (None, 0));
        }
    }

    pub(crate) fn populate_from_dec(&mut self, read_map: &ReadMap) {
        self.next_mapped_agent = read_map.agent_map.len() as AgentId;
        for (mapped_agent, (agent, last)) in read_map.agent_map.iter().enumerate() {
            self.ensure_capacity(*agent as usize + 1);
            self.agent_map[*agent as usize] = (Some(mapped_agent as AgentId), *last);
        }

        // This is a little bit gross. This is a worst-case O(n^2) insertion, but its almost always
        // linear because of how data will actually be read and written.
        for KVPair(file_base, op_range) in read_map.txn_map.iter() {
            let inverted = KVPair(op_range.start, (*file_base..*file_base+op_range.len()).into());
            self.txn_map.insert(inverted);
        }
    }

    pub(crate) fn map_no_root_mut<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        debug_assert_ne!(agent, ROOT_AGENT);

        let agent = agent as usize;
        self.ensure_capacity(agent + 1);

        self.agent_map[agent].0.ok_or_else(|| {
            // We'll quietly map it internally, but still return None because the caller needs to
            // know to write the name itself to the file.
            let mapped = self.next_mapped_agent;

            if persist {
                self.agent_map[agent] = (Some(mapped), 0);
                // println!("Mapped agent {} -> {}", oplog.client_data[agent].name, mapped);
                self.next_mapped_agent += 1;
            }

            client_data[agent].name.as_str()
        })
    }

    /// This is really gross.
    ///
    /// Same as map_no_root_mut except this doesn't take the persist: bool flag and only takes
    /// &self.
    pub(crate) fn map_no_root<'c>(&self, client_data: &'c [ClientData], agent: AgentId) -> Result<AgentId, &'c str> {
        debug_assert_ne!(agent, ROOT_AGENT);

        let agent = agent as usize;
        self.agent_map.get(agent).and_then(|e| e.0).ok_or_else(|| {
            // If its unknown, just return the agent's string name.
            client_data[agent].name.as_str()
        })
    }

    pub(super) fn seq_delta(&mut self, agent: AgentId, span: DTRange, persist: bool) -> isize {
        let agent = agent as usize;
        self.ensure_capacity(agent + 1);

        let item = &mut self.agent_map[agent].1;
        let old_seq = *item;

        if persist {
            *item = span.end;
        }

        isize_diff(span.start, old_seq)
    }

    pub(crate) fn map_maybe_root_mut<'c>(&mut self, client_data: &'c [ClientData], agent: AgentId, persist: bool) -> Result<AgentId, &'c str> {
        if agent == ROOT_AGENT { Ok(0) }
        else { self.map_no_root_mut(client_data, agent, persist).map(|a| a + 1) }
    }

    pub(crate) fn map_maybe_root<'c>(&self, client_data: &'c [ClientData], agent: AgentId) -> Result<AgentId, &'c str> {
        if agent == ROOT_AGENT { Ok(0) }
        else { self.map_no_root(client_data, agent).map(|a| a + 1) }
    }

    // Check if the specified time is known by the txn map.
    pub(crate) fn txn_map_has(&self, time: LV) -> bool {
        self.txn_map.contains_needle(time)

        // This is a little optimization. Does it make any difference?
        // if let Some(last) = self.txn_map.last() {
        //     if last.range().contains(time) {
        //         true
        //     } else {
        //         self.txn_map.find_index(time).is_ok()
        //     }
        // } else { false }
    }
}

pub fn isize_diff(x: usize, y: usize) -> isize {
    // This looks awkward, but the optimizer reduces this to a simple `sub`:
    // https://rust.godbolt.org/z/Ta617dWsK
    let result = (x as i128) - (y as i128);

    debug_assert!(result <= isize::MAX as i128);
    debug_assert!(result >= isize::MIN as i128);

    result as isize
}
